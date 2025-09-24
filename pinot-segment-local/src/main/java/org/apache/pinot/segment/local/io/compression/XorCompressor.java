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
 * XOR-based chunk compressor for LONG-only sequences.
 *
 * Layout: [flag=1 byte][numLongs=4][firstValue=8][compressedSize=4][LZ4(xors...)]
 */
class XorCompressor implements ChunkCompressor {

  static final XorCompressor INSTANCE = new XorCompressor();
  static final LZ4Factory LZ4_FACTORY = LZ4Factory.fastestInstance();
  private static final byte LONG_FLAG = 1;

  private XorCompressor() {
  }

  @Override
  public int compress(ByteBuffer inUncompressed, ByteBuffer outCompressed)
      throws IOException {
    int outStart = outCompressed.position();

    int remaining = inUncompressed.remaining();
    if (remaining % Long.BYTES != 0) {
      throw new IOException("Invalid input size: must be multiple of 8 bytes for LONG");
    }

    outCompressed.put(LONG_FLAG);

    int numLongs = remaining / Long.BYTES;
    outCompressed.putInt(numLongs);
    if (numLongs == 0) {
      outCompressed.flip();
      return outCompressed.limit() - outStart;
    }

    long prev = inUncompressed.getLong();
    outCompressed.putLong(prev);

    if (numLongs == 1) {
      outCompressed.flip();
      return outCompressed.limit() - outStart;
    }

    // Prepare XOR buffer
    ByteBuffer xorBuffer = ByteBuffer.allocate((numLongs - 1) * Long.BYTES);
    for (int i = 1; i < numLongs; i++) {
      long cur = inUncompressed.getLong();
      xorBuffer.putLong(prev ^ cur);
      prev = cur;
    }
    xorBuffer.flip();

    outCompressed.position(outCompressed.position() + Integer.BYTES);
    int compressedStart = outCompressed.position();
    LZ4_FACTORY.fastCompressor().compress(xorBuffer, outCompressed);
    int compressedSize = outCompressed.position() - compressedStart;
    outCompressed.putInt(compressedStart - Integer.BYTES, compressedSize);

    outCompressed.flip();
    return outCompressed.limit() - outStart;
  }

  @Override
  public int maxCompressedSize(int uncompressedSize) {
    if (uncompressedSize % Long.BYTES != 0) {
      throw new IllegalArgumentException("Invalid input size: must be multiple of 8 bytes for LONG");
    }
    int base = 1 + 4; // flag + numLongs
    int numLongs = uncompressedSize / Long.BYTES;
    if (numLongs == 0) {
      return base;
    }
    if (numLongs == 1) {
      return base + 8; // first value
    }
    int xorBytes = (numLongs - 1) * Long.BYTES;
    return base + 8 + 4 + LZ4_FACTORY.fastCompressor().maxCompressedLength(xorBytes);
  }

  @Override
  public ChunkCompressionType compressionType() {
    return ChunkCompressionType.XOR;
  }
}


