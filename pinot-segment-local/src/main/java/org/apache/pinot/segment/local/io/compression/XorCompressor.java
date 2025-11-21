/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pinot.segment.local.io.compression;

import java.io.IOException;
import java.nio.ByteBuffer;
import net.jpountz.lz4.LZ4Factory;
import org.apache.pinot.segment.spi.compression.ChunkCompressionType;
import org.apache.pinot.segment.spi.compression.ChunkCompressor;

/**
 * XOR compression for integer and long sequences.
 * Stores the first value verbatim, then XOR of each subsequent value with its predecessor.
 * The XOR stream is further compressed using LZ4.
 */
class XorCompressor implements ChunkCompressor {

  static final XorCompressor INSTANCE = new XorCompressor();
  static final LZ4Factory LZ4_FACTORY = LZ4Factory.fastestInstance();
  private static final byte INT_FLAG = 0;
  private static final byte LONG_FLAG = 1;

  private XorCompressor() {
  }

  @Override
  public int compress(ByteBuffer inUncompressed, ByteBuffer outCompressed)
      throws IOException {
    int outStart = outCompressed.position();
    int remaining = inUncompressed.remaining();
    if (remaining % Integer.BYTES != 0) {
      throw new IOException("Invalid input size: must be multiple of 4 bytes for INT or 8 bytes for LONG");
    }
    if (remaining % Long.BYTES != 0) {
      outCompressed.put(INT_FLAG);
      return compressInts(inUncompressed, outCompressed, outStart);
    }
    outCompressed.put(LONG_FLAG);
    return compressLongs(inUncompressed, outCompressed, outStart);
  }

  private int compressInts(ByteBuffer inUncompressed, ByteBuffer outCompressed, int outStart)
      throws IOException {
    int count = inUncompressed.remaining() / Integer.BYTES;
    if (count == 0) {
      outCompressed.putInt(0);
      outCompressed.flip();
      return 5; // flag + count
    }
    outCompressed.putInt(count);
    int prev = inUncompressed.getInt();
    outCompressed.putInt(prev);
    if (count == 1) {
      outCompressed.flip();
      return outCompressed.limit() - outStart;
    }
    ByteBuffer xorBuf = ByteBuffer.allocate((count - 1) * Integer.BYTES);
    for (int i = 1; i < count; i++) {
      int cur = inUncompressed.getInt();
      int x = cur ^ prev;
      xorBuf.putInt(x);
      prev = cur;
    }
    xorBuf.flip();
    outCompressed.position(outCompressed.position() + Integer.BYTES);
    int compStart = outCompressed.position();
    LZ4_FACTORY.fastCompressor().compress(xorBuf, outCompressed);
    int compSize = outCompressed.position() - compStart;
    outCompressed.putInt(compStart - Integer.BYTES, compSize);
    outCompressed.flip();
    return outCompressed.limit() - outStart;
  }

  private int compressLongs(ByteBuffer inUncompressed, ByteBuffer outCompressed, int outStart)
      throws IOException {
    int count = inUncompressed.remaining() / Long.BYTES;
    if (count == 0) {
      outCompressed.putInt(0);
      outCompressed.flip();
      return 5; // flag + count
    }
    outCompressed.putInt(count);
    long prev = inUncompressed.getLong();
    outCompressed.putLong(prev);
    if (count == 1) {
      outCompressed.flip();
      return outCompressed.limit() - outStart;
    }
    ByteBuffer xorBuf = ByteBuffer.allocate((count - 1) * Long.BYTES);
    for (int i = 1; i < count; i++) {
      long cur = inUncompressed.getLong();
      long x = cur ^ prev;
      xorBuf.putLong(x);
      prev = cur;
    }
    xorBuf.flip();
    outCompressed.position(outCompressed.position() + Integer.BYTES);
    int compStart = outCompressed.position();
    LZ4_FACTORY.fastCompressor().compress(xorBuf, outCompressed);
    int compSize = outCompressed.position() - compStart;
    outCompressed.putInt(compStart - Integer.BYTES, compSize);
    outCompressed.flip();
    return outCompressed.limit() - outStart;
  }

  @Override
  public int maxCompressedSize(int uncompressedSize) {
    int base = 1; // flag
    if (uncompressedSize % Integer.BYTES != 0) {
      throw new IllegalArgumentException("Invalid input size: must be multiple of 4 bytes for INT or 8 bytes for LONG");
    }
    if (uncompressedSize % Long.BYTES != 0) {
      int count = uncompressedSize / Integer.BYTES;
      if (count == 0) {
        return base + 4;
      }
      if (count == 1) {
        return base + 8; // flag + count + first value
      }
      int xorSize = (count - 1) * Integer.BYTES;
      return base + 12 + LZ4_FACTORY.fastCompressor().maxCompressedLength(xorSize);
    }
    int count = uncompressedSize / Long.BYTES;
    if (count == 0) {
      return base + 4;
    }
    if (count == 1) {
      return base + 12; // flag + count + first long
    }
    int xorSize = (count - 1) * Long.BYTES;
    return base + 16 + LZ4_FACTORY.fastCompressor().maxCompressedLength(xorSize);
  }

  @Override
  public ChunkCompressionType compressionType() {
    return ChunkCompressionType.XOR_LZ4;
  }
}


