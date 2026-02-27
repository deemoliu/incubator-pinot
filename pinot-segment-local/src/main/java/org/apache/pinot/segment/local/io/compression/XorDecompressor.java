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
import org.apache.pinot.segment.spi.compression.ChunkDecompressor;

/**
 * Decompressor for XOR compression of successive float/double values with LZ4-compressed XOR stream.
 */
class XorDecompressor implements ChunkDecompressor {
  static final XorDecompressor INSTANCE = new XorDecompressor();
  static final LZ4Factory LZ4_FACTORY = LZ4Factory.fastestInstance();
  private static final byte DOUBLE_FLAG = 1;

  private XorDecompressor() {
  }

  @Override
  public int decompress(ByteBuffer compressedInput, ByteBuffer decompressedOutput)
      throws IOException {
    byte flag = compressedInput.get();
    if (flag != DOUBLE_FLAG) {
      return decompressForFloat(compressedInput, decompressedOutput);
    }
    return decompressForDouble(compressedInput, decompressedOutput);
  }

  private int decompressForFloat(ByteBuffer compressedInput, ByteBuffer decompressedOutput) {
    int numFloats = compressedInput.getInt();
    if (numFloats == 0) {
      decompressedOutput.flip();
      return 0;
    }

    float first = compressedInput.getFloat();
    decompressedOutput.putFloat(first);
    if (numFloats == 1) {
      decompressedOutput.flip();
      return Float.BYTES;
    }

    int compressedSize = compressedInput.getInt();

    ByteBuffer xorBuffer = ByteBuffer.allocate((numFloats - 1) * Integer.BYTES);
    ByteBuffer compressedXors = compressedInput.slice();
    compressedXors.limit(compressedSize);

    LZ4_FACTORY.safeDecompressor().decompress(compressedXors, xorBuffer);
    xorBuffer.flip();

    int prevBits = Float.floatToRawIntBits(first);
    for (int i = 1; i < numFloats; i++) {
      int xorBits = xorBuffer.getInt();
      int currentBits = prevBits ^ xorBits;
      float current = Float.intBitsToFloat(currentBits);
      decompressedOutput.putFloat(current);
      prevBits = currentBits;
    }

    decompressedOutput.flip();
    return numFloats * Float.BYTES;
  }

  private int decompressForDouble(ByteBuffer compressedInput, ByteBuffer decompressedOutput) {
    int numDoubles = compressedInput.getInt();
    if (numDoubles == 0) {
      decompressedOutput.flip();
      return 0;
    }

    double first = compressedInput.getDouble();
    decompressedOutput.putDouble(first);
    if (numDoubles == 1) {
      decompressedOutput.flip();
      return Double.BYTES;
    }

    int compressedSize = compressedInput.getInt();

    ByteBuffer xorBuffer = ByteBuffer.allocate((numDoubles - 1) * Long.BYTES);
    ByteBuffer compressedXors = compressedInput.slice();
    compressedXors.limit(compressedSize);

    LZ4_FACTORY.safeDecompressor().decompress(compressedXors, xorBuffer);
    xorBuffer.flip();

    long prevBits = Double.doubleToRawLongBits(first);
    for (int i = 1; i < numDoubles; i++) {
      long xorBits = xorBuffer.getLong();
      long currentBits = prevBits ^ xorBits;
      double current = Double.longBitsToDouble(currentBits);
      decompressedOutput.putDouble(current);
      prevBits = currentBits;
    }

    decompressedOutput.flip();
    return numDoubles * Double.BYTES;
  }

  @Override
  public int decompressedLength(ByteBuffer compressedInput) {
    byte flag = compressedInput.get(compressedInput.position());
    int numValues = compressedInput.getInt(compressedInput.position() + 1);
    if (flag != DOUBLE_FLAG) {
      return numValues * Float.BYTES;
    }
    return numValues * Double.BYTES;
  }
}
