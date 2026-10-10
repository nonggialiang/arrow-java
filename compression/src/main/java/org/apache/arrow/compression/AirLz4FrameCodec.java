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

import io.airlift.compress.lz4.Lz4Compressor;
import io.airlift.compress.lz4.Lz4Decompressor;
import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.compression.AbstractCompressionCodec;
import org.apache.arrow.vector.compression.CompressionUtil;
import org.apache.commons.compress.compressors.lz4.XXHash32;

/**
 * FG patch (D33): LZ4_FRAME codec backed by <b>aircompressor</b> instead of commons-compress.
 *
 * <p>Rationale: commons-compress {@code FramedLZ4CompressorOutputStream} is a pure-java
 * implementation with pathological write performance (measured 9.4s per MiB vs 0.5s for
 * zstd-jni — a sink writing one 4 MiB result took 38s). aircompressor's LZ4 (block format,
 * pure java, no JNI) is fast and already on the Spark executor classpath; this codec
 * hand-emits the standard LZ4 frame format (v1.6.3) around it: magic + FLG/BD/HC header,
 * independent blocks (max 4 MiB, incompressible blocks stored raw with the high bit set),
 * end mark; no content/block checksums. HC 用 commons-compress 的 XXHash32（读端零成本）。
 * pyarrow / arrow C++ read the frames natively.
 *
 * <p>The Arrow buffer layout ([8B uncompressed length][compressed body]) is handled by
 * {@link AbstractCompressionCodec}.
 */
public class AirLz4FrameCodec extends AbstractCompressionCodec {

  // LZ4 frame magic：字节序列 04 22 4D 18（LE 数值 0x184D2204——注意不是直觉的
  // 0x184D2A50，以 python-lz4 权威字节序为准，实证 2026-10-10）
  private static final int MAGIC = 0x184D2204;
  /** FLG: version 01 | block independence; no block/content checksum, no content size. */
  private static final int FLG = 0x60;
  /** BD: block max size code 7 = 4 MiB. */
  private static final int BD = 0x70;
  private static final int BLOCK_MAX = 4 * 1024 * 1024;

  @Override
  public CompressionUtil.CodecType getCodecType() {
    return CompressionUtil.CodecType.LZ4_FRAME;
  }

  @Override
  protected ArrowBuf doCompress(BufferAllocator allocator, ArrowBuf uncompressedBuffer) {
    int unLen = (int) uncompressedBuffer.writerIndex();
    byte[] in = new byte[unLen];
    uncompressedBuffer.getBytes(0, in, 0, unLen);

    // header: magic(LE) + FLG + BD + HC，HC = (XXH32({FLG,BD}, seed 0) >>> 8) & 0xFF
    XXHash32 xxh = new XXHash32();
    xxh.update(FLG);
    xxh.update(BD);
    byte hc = (byte) ((xxh.getValue() >>> 8) & 0xFF);

    ByteArrayOutputStream frame = new ByteArrayOutputStream(unLen / 2 + 64);
    frame.write(MAGIC & 0xFF);
    frame.write((MAGIC >> 8) & 0xFF);
    frame.write((MAGIC >> 16) & 0xFF);
    frame.write((MAGIC >> 24) & 0xFF);
    frame.write(FLG);
    frame.write(BD);
    frame.write(hc);

    Lz4Compressor lz4 = new Lz4Compressor();
    byte[] scratch = new byte[Math.max(64, lz4.maxCompressedLength(Math.min(unLen, BLOCK_MAX)))];
    for (int off = 0; off < unLen; ) {
      int chunk = Math.min(BLOCK_MAX, unLen - off);
      int compressedLen = lz4.compress(in, off, chunk, scratch, 0, scratch.length);
      if (compressedLen >= chunk) {
        writeBlockSize(frame, chunk | 0x80000000); // 不可压缩 → 原文块（高位标记）
        frame.write(in, off, chunk);
      } else {
        writeBlockSize(frame, compressedLen);
        frame.write(scratch, 0, compressedLen);
      }
      off += chunk;
    }
    frame.write(new byte[] {0, 0, 0, 0}, 0, 4); // end mark（无 content checksum）

    byte[] body = frame.toByteArray();
    ArrowBuf compressedBuffer =
        allocator.buffer(CompressionUtil.SIZE_OF_UNCOMPRESSED_LENGTH + body.length);
    compressedBuffer.setBytes(CompressionUtil.SIZE_OF_UNCOMPRESSED_LENGTH, body, 0, body.length);
    compressedBuffer.writerIndex(CompressionUtil.SIZE_OF_UNCOMPRESSED_LENGTH + body.length);
    return compressedBuffer;
  }

  private static void writeBlockSize(ByteArrayOutputStream frame, int size) {
    frame.write(size & 0xFF);
    frame.write((size >> 8) & 0xFF);
    frame.write((size >> 16) & 0xFF);
    frame.write((size >> 24) & 0xFF);
  }

  @Override
  protected ArrowBuf doDecompress(BufferAllocator allocator, ArrowBuf compressedBuffer) {
    long decompressedLength = readUncompressedLength(compressedBuffer);
    int bodyLen =
        (int) (compressedBuffer.writerIndex() - CompressionUtil.SIZE_OF_UNCOMPRESSED_LENGTH);
    byte[] body = new byte[bodyLen];
    compressedBuffer.getBytes(CompressionUtil.SIZE_OF_UNCOMPRESSED_LENGTH, body, 0, bodyLen);

    ByteBuffer in = ByteBuffer.wrap(body).order(ByteOrder.LITTLE_ENDIAN);
    if (in.getInt() != MAGIC) {
      throw new IllegalArgumentException("Bad LZ4 frame magic");
    }
    int flg = in.get() & 0xFF;
    in.get(); // BD（接受任意块上限）
    in.get(); // HC（进程内可信写方，不校验）
    boolean hasContentChecksum = (flg & 0x04) != 0;

    byte[] out = new byte[(int) decompressedLength];
    int written = 0;
    Lz4Decompressor lz4 = new Lz4Decompressor();
    while (true) {
      int size = in.getInt();
      if (size == 0) {
        break; // end mark
      }
      boolean raw = (size & 0x80000000) != 0;
      int blockLen = size & 0x7FFFFFFF;
      if (raw) {
        System.arraycopy(body, in.position(), out, written, blockLen);
        written += blockLen;
      } else {
        // 块独立：本块原长 = min(剩余总量, 写方块上限 4MiB)
        int chunk = (int) Math.min(decompressedLength - written, (long) BLOCK_MAX);
        lz4.decompress(body, in.position(), blockLen, out, written, chunk);
        written += chunk;
      }
      in.position(in.position() + blockLen);
    }
    if (hasContentChecksum) {
      in.getInt(); // 跳过 content checksum
    }
    if (written != decompressedLength) {
      throw new IllegalArgumentException(
          "LZ4 frame decompressed size mismatch: " + written + " != " + decompressedLength);
    }

    ArrowBuf decompressedBuffer = allocator.buffer(decompressedLength);
    decompressedBuffer.setBytes(0, out, 0, written);
    decompressedBuffer.writerIndex(decompressedLength);
    return decompressedBuffer;
  }
}
