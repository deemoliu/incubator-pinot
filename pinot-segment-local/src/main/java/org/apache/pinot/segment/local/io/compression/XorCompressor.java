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
 * Compressor that applies XOR on successive float/double values and then compresses the XOR stream using LZ4.
 * Format:
 * - 1 byte type flag: 0 = float, 1 = double
 * - 4 bytes count (number of values)
 * - first value written raw (4 or 8 bytes)
 * - 4 bytes compressed-size of XOR stream
 * - LZ4-compressed XOR stream of (count-1) elements (ints for float, longs for double)
 */
class XorCompressor implements ChunkCompressor {
  static final XorCompressor INSTANCE = new XorCompressor();
  static final LZ4Factory LZ4_FACTORY = LZ4Factory.fastestInstance();
  private static final byte FLOAT_FLAG = 0;
  private static final byte DOUBLE_FLAG = 1;

  private XorCompressor() {
  }

  @Override
  public int compress(ByteBuffer inUncompressed, ByteBuffer outCompressed)
      throws IOException {
    int outStartPosition = outCompressed.position();

    int remaining = inUncompressed.remaining();
    if (remaining % Float.BYTES != 0) {
      throw new IOException("Invalid input size: must be multiple of 4 bytes for FLOAT or 8 bytes for DOUBLE");
    }
    if (remaining % Double.BYTES != 0) {
      return compressForFloat(inUncompressed, outCompressed, outStartPosition);
    }
    return compressForDouble(inUncompressed, outCompressed, outStartPosition);
  }

  private int compressForFloat(ByteBuffer inUncompressed, ByteBuffer outCompressed, int outStartPosition) {
    outCompressed.put(FLOAT_FLAG);
    int numFloats = inUncompressed.remaining() / Float.BYTES;
    if (numFloats == 0) {
      outCompressed.putInt(0);
      outCompressed.flip();
      return 5; // 1 byte flag + 4 bytes for count
    }

    outCompressed.putInt(numFloats);
    float firstValue = inUncompressed.getFloat();
    outCompressed.putFloat(firstValue);

    if (numFloats == 1) {
      outCompressed.flip();
      return outCompressed.limit() - outStartPosition;
    }

    // Prepare XOR buffer (store XORs as ints)
    ByteBuffer xorBuffer = ByteBuffer.allocate((numFloats - 1) * Integer.BYTES);
    int prevBits = Float.floatToRawIntBits(firstValue);
    for (int i = 1; i < numFloats; i++) {
      float current = inUncompressed.getFloat();
      int currentBits = Float.floatToRawIntBits(current);
      int xorBits = currentBits ^ prevBits;
      xorBuffer.putInt(xorBits);
      prevBits = currentBits;
    }
    xorBuffer.flip();

    // Reserve space for compressed size
    outCompressed.position(outCompressed.position() + Integer.BYTES);
    int compressedStart = outCompressed.position();

    // LZ4-compress the XOR stream
    LZ4_FACTORY.fastCompressor().compress(xorBuffer, outCompressed);

    int compressedSize = outCompressed.position() - compressedStart;
    outCompressed.putInt(compressedStart - Integer.BYTES, compressedSize);

    outCompressed.flip();
    return outCompressed.limit() - outStartPosition;
  }

  private int compressForDouble(ByteBuffer inUncompressed, ByteBuffer outCompressed, int outStartPosition) {
    outCompressed.put(DOUBLE_FLAG);
    int numDoubles = inUncompressed.remaining() / Double.BYTES;
    if (numDoubles == 0) {
      outCompressed.putInt(0);
      outCompressed.flip();
      return 5; // 1 byte flag + 4 bytes for count
    }

    outCompressed.putInt(numDoubles);
    double firstValue = inUncompressed.getDouble();
    outCompressed.putDouble(firstValue);

    if (numDoubles == 1) {
      outCompressed.flip();
      return outCompressed.limit() - outStartPosition;
    }

    // Prepare XOR buffer (store XORs as longs)
    ByteBuffer xorBuffer = ByteBuffer.allocate((numDoubles - 1) * Long.BYTES);
    long prevBits = Double.doubleToRawLongBits(firstValue);
    for (int i = 1; i < numDoubles; i++) {
      double current = inUncompressed.getDouble();
      long currentBits = Double.doubleToRawLongBits(current);
      long xorBits = currentBits ^ prevBits;
      xorBuffer.putLong(xorBits);
      prevBits = currentBits;
    }
    xorBuffer.flip();

    // Reserve space for compressed size
    outCompressed.position(outCompressed.position() + Integer.BYTES);
    int compressedStart = outCompressed.position();

    // LZ4-compress the XOR stream
    LZ4_FACTORY.fastCompressor().compress(xorBuffer, outCompressed);

    int compressedSize = outCompressed.position() - compressedStart;
    outCompressed.putInt(compressedStart - Integer.BYTES, compressedSize);

    outCompressed.flip();
    return outCompressed.limit() - outStartPosition;
  }

  @Override
  public int maxCompressedSize(int uncompressedSize) {
    int flagSize = 1;
    if (uncompressedSize % Float.BYTES != 0) {
      throw new IllegalArgumentException("Invalid input size: must be multiple of 4 bytes for FLOAT or 8 bytes for DOUBLE");
    }
    if (uncompressedSize % Double.BYTES != 0) {
      int numFloats = uncompressedSize / Float.BYTES;
      if (numFloats == 0) {
        return flagSize + 4;
      }
      if (numFloats == 1) {
        return flagSize + 8; // flag + count + first float
      }
      int xorSize = (numFloats - 1) * Integer.BYTES;
      // flag + count + first value + compressed size + compressed XORs
      return flagSize + 4 + 4 + 4 + LZ4_FACTORY.fastCompressor().maxCompressedLength(xorSize);
    }
    int numDoubles = uncompressedSize / Double.BYTES;
    if (numDoubles == 0) {
      return flagSize + 4;
    }
    if (numDoubles == 1) {
      return flagSize + 4 + 8; // flag + count + first double
    }
    int xorSize = (numDoubles - 1) * Long.BYTES;
    // flag + count + first value + compressed size + compressed XORs
    return flagSize + 4 + 8 + 4 + LZ4_FACTORY.fastCompressor().maxCompressedLength(xorSize);
  }

  @Override
  public ChunkCompressionType compressionType() {
    return ChunkCompressionType.XOR;
  }
}
