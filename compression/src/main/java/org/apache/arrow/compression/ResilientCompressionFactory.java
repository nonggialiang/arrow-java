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

import com.github.luben.zstd.Zstd;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.compression.CompressionCodec;
import org.apache.arrow.vector.compression.CompressionUtil;

/**
 * FG patch (D34): read-side resilient compression factory — recovers objects corrupted by upstream
 * write bug <a href="https://github.com/apache/arrow-java/issues/1116">apache/arrow-java#1116</a>
 * (12.0.1 ~ 19.0.0 affected, fixed in 20.0.0 via #1119).
 *
 * <p><b>The bug</b>: {@code AbstractCompressionCodec.compress} reads {@code writerIndex()} at
 * multiple points (empty check, size comparison, prefix write); under certain timings the 8-byte
 * uncompressed-length prefix of a zstd buffer is written as {@code 0} while the zstd frame itself
 * is intact. The 19.0.0 reader sees prefix {@code 0} → empty shortcut returns a zero-capacity
 * buffer → {@code loadFieldBuffers} throws IndexOutOfBoundsException → VectorLoader's catch block
 * leaks the already-decompressed {@code ownBuffers} → child allocator close reports "Memory was
 * leaked", masking the original error.
 *
 * <p><b>This factory</b> intercepts only the "#1116 shape" (prefix {@code 0} with non-empty
 * payload): the zstd frame header carries the decompressed content size (the writer's {@code
 * Zstd.compressUnsafe} always writes it), so the buffer is recovered by decompressing with the
 * frame-declared size. Everything else is delegated unchanged to the inner codec. LZ4 frames
 * written by {@link AirLz4FrameCodec} carry no content size (FLG without the content-size flag) —
 * no read-side recovery is possible for LZ4; write-side hardening is the fix there.
 *
 * <p>Contract alignment: {@link CompressionCodec#decompress} implementations must release the
 * compressed input buffer — the fallback path takes over the input (closes it); if the fallback is
 * not applicable, the inner codec handles the input per its original semantics.
 */
public final class ResilientCompressionFactory implements CompressionCodec.Factory {

  /** Shared instance (stateless wrapper). */
  public static final ResilientCompressionFactory INSTANCE =
      new ResilientCompressionFactory(CommonsCompressionFactory.INSTANCE);

  private final CompressionCodec.Factory delegate;

  private ResilientCompressionFactory(CompressionCodec.Factory delegate) {
    this.delegate = delegate;
  }

  @Override
  public CompressionCodec createCodec(CompressionUtil.CodecType codecType) {
    return new ResilientCodec(delegate.createCodec(codecType));
  }

  @Override
  public CompressionCodec createCodec(CompressionUtil.CodecType codecType, int compressionLevel) {
    return new ResilientCodec(delegate.createCodec(codecType, compressionLevel));
  }

  private static final class ResilientCodec implements CompressionCodec {

    private final CompressionCodec inner;

    ResilientCodec(CompressionCodec inner) {
      this.inner = inner;
    }

    @Override
    public ArrowBuf compress(BufferAllocator allocator, ArrowBuf uncompressedBuffer) {
      return inner.compress(
          allocator, uncompressedBuffer); // passthrough (read side never compresses)
    }

    @Override
    public ArrowBuf decompress(BufferAllocator allocator, ArrowBuf compressedBuffer) {
      if (isZeroPrefixedZstd(compressedBuffer)) {
        try {
          return zstdFrameFallback(allocator, compressedBuffer);
        } catch (Exception e) {
          // Not applicable (not a zstd frame / no content size / size mismatch) → fall through
          // to the inner codec's original semantics.
        }
      }
      return inner.decompress(allocator, compressedBuffer);
    }

    @Override
    public CompressionUtil.CodecType getCodecType() {
      return inner.getCodecType();
    }

    /** #1116 shape: prefix 0 with non-empty payload (a legal empty buffer is exactly 8 bytes). */
    private static boolean isZeroPrefixedZstd(ArrowBuf compressed) {
      return compressed.writerIndex() > CompressionUtil.SIZE_OF_UNCOMPRESSED_LENGTH
          && compressed.getLong(0) == 0L;
    }

    /**
     * Recovers via the zstd frame header's declared content size; takes over the input lifetime.
     */
    private static ArrowBuf zstdFrameFallback(BufferAllocator allocator, ArrowBuf compressed) {
      long wi = compressed.writerIndex();
      byte[] body = new byte[(int) (wi - CompressionUtil.SIZE_OF_UNCOMPRESSED_LENGTH)];
      compressed.getBytes(CompressionUtil.SIZE_OF_UNCOMPRESSED_LENGTH, body, 0, body.length);

      long contentSize = Zstd.getFrameContentSize(body, 0);
      if (contentSize <= 0 || contentSize > Integer.MAX_VALUE - 8) {
        throw new IllegalArgumentException("zstd frame content size unavailable: " + contentSize);
      }
      byte[] out = new byte[(int) contentSize];
      // NB: the byte[]/ByteBuffer overload pair — the ByteBuffer one requires a direct buffer
      long n = Zstd.decompress(out, body);
      if (Zstd.isError(n)) {
        throw new IllegalArgumentException("zstd decompress failed: " + Zstd.getErrorName(n));
      }
      if (n != contentSize) {
        throw new IllegalArgumentException(
            "zstd frame content size mismatch: declared=" + contentSize + " actual=" + n);
      }
      ArrowBuf buf = allocator.buffer(contentSize);
      buf.setBytes(0, out, 0, (int) contentSize);
      buf.writerIndex(contentSize);
      compressed.close(); // decompress contract: taking over the input requires releasing it
      return buf;
    }
  }
}
