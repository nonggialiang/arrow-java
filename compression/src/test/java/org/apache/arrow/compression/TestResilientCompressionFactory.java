/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.arrow.compression;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.nio.channels.Channels;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.compression.CompressionCodec;
import org.apache.arrow.vector.compression.CompressionUtil;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;
import org.apache.arrow.vector.ipc.message.IpcOption;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.Test;

/**
 * FG patch (D34) tests for {@link ResilientCompressionFactory} — the #1116 recovery path: a zstd
 * stream whose 8-byte uncompressed-length prefixes were zeroed (frame intact) must read back
 * losslessly, while healthy zstd streams and lz4 streams behave exactly as with the delegate
 * factory.
 */
public class TestResilientCompressionFactory {

  private static final int ROWS = 16384;

  /** Writes a zstd stream, then re-writes every compressed buffer's prefix as 0 in the body. */
  private static byte[] corruptAllPrefixes(byte[] stream, RootAllocator unused) {
    // The encapsulated-message framing: continuation(0xFFFFFFFF) + metadataLen + metadata +
    // body(8-aligned). Zero the first 8 bytes of each buffer listed in the RecordBatch flatbuf.
    int pos = 0;
    ByteArrayOutputStream out = new ByteArrayOutputStream(stream.length);
    boolean firstMessage = true;
    while (pos < stream.length) {
      int word = readInt32LE(stream, pos);
      if (word != -1) {
        break; // legacy framing or EOS — not produced here
      }
      int metaLen = readInt32LE(stream, pos + 4);
      if (metaLen == 0) {
        break; // EOS
      }
      java.nio.ByteBuffer metadata = java.nio.ByteBuffer.wrap(stream, pos + 8, metaLen);
      org.apache.arrow.flatbuf.Message msg =
          org.apache.arrow.flatbuf.Message.getRootAsMessage(metadata);
      long bodyLen = msg.bodyLength();
      long paddedBody = (bodyLen + 7) & ~7L;
      int bodyStart = pos + 8 + metaLen;

      byte[] message = java.util.Arrays.copyOfRange(stream, pos, bodyStart + (int) paddedBody);
      if (msg.headerType() == org.apache.arrow.flatbuf.MessageHeader.RecordBatch) {
        org.apache.arrow.flatbuf.RecordBatch rb =
            (org.apache.arrow.flatbuf.RecordBatch)
                msg.header(new org.apache.arrow.flatbuf.RecordBatch());
        // Zero each buffer's prefix in the output copy (skip 0-length buffers: no prefix at all).
        // NB: indices are message-relative (message starts at pos), body starts at 8+metaLen.
        for (int i = 0; i < rb.buffersLength(); i++) {
          org.apache.arrow.flatbuf.Buffer buf = rb.buffers(i);
          if (buf.length() >= 8) {
            int rel = 8 + metaLen + (int) buf.offset();
            for (int b = 0; b < 8; b++) {
              message[rel + b] = 0;
            }
          }
        }
      }
      out.write(message, 0, message.length);
      pos = bodyStart + (int) paddedBody;
    }
    out.write(0xFF);
    out.write(0xFF);
    out.write(0xFF);
    out.write(0xFF);
    out.write(0);
    out.write(0);
    out.write(0);
    out.write(0); // EOS
    return out.toByteArray();
  }

  private static int readInt32LE(byte[] b, int off) {
    return (b[off] & 0xFF)
        | ((b[off + 1] & 0xFF) << 8)
        | ((b[off + 2] & 0xFF) << 16)
        | ((b[off + 3] & 0xFF) << 24);
  }

  private static byte[] writeZstdStream(CompressionCodec.Factory factory) throws Exception {
    Schema schema =
        new Schema(
            java.util.Arrays.asList(
                new Field("id", FieldType.notNullable(new ArrowType.Int(64, true)), null),
                new Field("name", FieldType.nullable(new ArrowType.Utf8()), null)));
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    try (RootAllocator alloc = new RootAllocator(Long.MAX_VALUE);
        VectorSchemaRoot root = VectorSchemaRoot.create(schema, alloc)) {
      BigIntVector id = (BigIntVector) root.getVector("id");
      VarCharVector name = (VarCharVector) root.getVector("name");
      try (ArrowStreamWriter writer =
          new ArrowStreamWriter(
              root,
              null,
              Channels.newChannel(out),
              IpcOption.DEFAULT,
              factory,
              CompressionUtil.CodecType.ZSTD)) {
        writer.start();
        id.allocateNew(ROWS);
        name.allocateNew(ROWS);
        for (int i = 0; i < ROWS; i++) {
          id.setSafe(i, i);
          if (i % 7 == 0) {
            name.setNull(i);
          } else {
            name.setSafe(i, ("row-" + i).getBytes(java.nio.charset.StandardCharsets.UTF_8));
          }
        }
        id.setValueCount(ROWS);
        name.setValueCount(ROWS);
        root.setRowCount(ROWS);
        writer.writeBatch();
      }
    }
    return out.toByteArray();
  }

  /** #1116 shape: all real compressed buffers' prefixes zeroed, the Resilient factory recovers. */
  @Test
  public void corruptedPrefixStreamRecoveredLosslessly() throws Exception {
    byte[] healthy = writeZstdStream(CommonsCompressionFactory.INSTANCE);
    byte[] corrupted = corruptAllPrefixes(healthy, null);

    // 健康流用原版工厂可读（对照组）
    try (RootAllocator alloc = new RootAllocator(Long.MAX_VALUE);
        ArrowStreamReader reader =
            new ArrowStreamReader(
                new ByteArrayInputStream(healthy), alloc, CommonsCompressionFactory.INSTANCE)) {
      assertTrue(reader.loadNextBatch());
    }

    // 损坏流：原版工厂必错（对照，确认损坏有效——首载入空 vector 是静默零值/越界而非必炸）
    boolean plainFactoryWrong = false;
    try (RootAllocator alloc = new RootAllocator(Long.MAX_VALUE);
        ArrowStreamReader reader =
            new ArrowStreamReader(
                new ByteArrayInputStream(corrupted), alloc, CommonsCompressionFactory.INSTANCE)) {
      assertTrue(reader.loadNextBatch());
      BigIntVector id0 = (BigIntVector) reader.getVectorSchemaRoot().getVector("id");
      plainFactoryWrong = id0.get(997) != 997; // 值损坏（零值）或 get 自身抛错都算
    } catch (Exception e) {
      plainFactoryWrong = true;
    }
    assertTrue(plainFactoryWrong, "corrupted stream must misbehave with the plain factory");

    // 损坏流：Resilient 工厂逐值无损读回
    try (RootAllocator alloc = new RootAllocator(Long.MAX_VALUE);
        ArrowStreamReader reader =
            new ArrowStreamReader(
                new ByteArrayInputStream(corrupted), alloc, ResilientCompressionFactory.INSTANCE)) {
      assertTrue(reader.loadNextBatch());
      VectorSchemaRoot root = reader.getVectorSchemaRoot();
      assertEquals(ROWS, root.getRowCount());
      BigIntVector id = (BigIntVector) root.getVector("id");
      VarCharVector name = (VarCharVector) root.getVector("name");
      for (int i = 0; i < ROWS; i += 997) {
        assertEquals(i, id.get(i));
        if (i % 7 == 0) {
          assertTrue(name.get(i) == null);
        } else {
          assertEquals(
              "row-" + i, new String(name.get(i), java.nio.charset.StandardCharsets.UTF_8));
        }
      }
      assertEquals(false, reader.loadNextBatch()); // EOS
    }
  }

  /** Healthy streams read back identically through the Resilient factory (pure passthrough). */
  @Test
  public void healthyStreamUnaffected() throws Exception {
    byte[] healthy = writeZstdStream(CommonsCompressionFactory.INSTANCE);
    try (RootAllocator alloc = new RootAllocator(Long.MAX_VALUE);
        ArrowStreamReader reader =
            new ArrowStreamReader(
                new ByteArrayInputStream(healthy), alloc, ResilientCompressionFactory.INSTANCE)) {
      assertTrue(reader.loadNextBatch());
      VectorSchemaRoot root = reader.getVectorSchemaRoot();
      assertEquals(ROWS, root.getRowCount());
      BigIntVector id = (BigIntVector) root.getVector("id");
      for (int i = 0; i < ROWS; i += 991) {
        assertEquals(i, id.get(i));
      }
    }
  }

  /** A legal zero-row/empty-buffer prefix-0 (8-byte empty shortcut) is passed through. */
  @Test
  public void legalEmptyBuffersPassthrough() throws Exception {
    Schema schema =
        new Schema(
            java.util.Arrays.asList(
                new Field("id", FieldType.notNullable(new ArrowType.Int(64, true)), null)));
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    try (RootAllocator alloc = new RootAllocator(Long.MAX_VALUE);
        VectorSchemaRoot root = VectorSchemaRoot.create(schema, alloc)) {
      BigIntVector id = (BigIntVector) root.getVector("id");
      try (ArrowStreamWriter writer =
          new ArrowStreamWriter(
              root,
              null,
              Channels.newChannel(out),
              IpcOption.DEFAULT,
              CommonsCompressionFactory.INSTANCE,
              CompressionUtil.CodecType.ZSTD)) {
        writer.start();
        // 0 行批：全部 buffer 走空 shortcut（8 字节、前缀 0）
        id.setValueCount(0);
        root.setRowCount(0);
        writer.writeBatch();
        // 非 0 行批
        id.allocateNew(5);
        for (int i = 0; i < 5; i++) {
          id.setSafe(i, i);
        }
        id.setValueCount(5);
        root.setRowCount(5);
        writer.writeBatch();
      }
    }
    try (RootAllocator alloc = new RootAllocator(Long.MAX_VALUE);
        ArrowStreamReader reader =
            new ArrowStreamReader(
                new ByteArrayInputStream(out.toByteArray()),
                alloc,
                ResilientCompressionFactory.INSTANCE)) {
      assertTrue(reader.loadNextBatch());
      assertEquals(0, reader.getVectorSchemaRoot().getRowCount());
      assertTrue(reader.loadNextBatch());
      assertEquals(5, reader.getVectorSchemaRoot().getRowCount());
      assertEquals(false, reader.loadNextBatch());
    }
  }
}
