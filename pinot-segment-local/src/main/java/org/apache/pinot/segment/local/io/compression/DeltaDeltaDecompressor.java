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

import org.apache.pinot.segment.spi.compression.ChunkDecompressor;

import java.nio.ByteBuffer;


/**
 * A pass-through implementation of {@link ChunkDecompressor}, that simply returns the input data without
 * performing any de-compression. This is useful for cases where cost of de-compression out-weighs the benefits
 * of compression.
 */
class DeltaDeltaDecompressor implements ChunkDecompressor {

  static final DeltaDeltaDecompressor INSTANCE = new DeltaDeltaDecompressor();

  private DeltaDeltaDecompressor() {
  }

  @Override
  public int decompress(ByteBuffer compressedInput, ByteBuffer decompressedOutput) {
    // Get number of integers
    int numInts = compressedInput.getInt();
    if (numInts == 0) {
      decompressedOutput.flip();
      return 0;
    }

    // Get first value
    int prevValue = compressedInput.getInt();
    decompressedOutput.putInt(prevValue);

    if (numInts == 1) {
      decompressedOutput.flip();
      return Integer.BYTES;
    }

    // Get first delta
    int prevDelta = compressedInput.getInt();
    int currentValue = prevValue + prevDelta;
    decompressedOutput.putInt(currentValue);
    prevValue = currentValue;

    // Decompress remaining values
    for (int i = 2; i < numInts; i++) {
      int deltaOfDelta = compressedInput.getInt();
      int currentDelta = prevDelta + deltaOfDelta;
      currentValue = prevValue + currentDelta;

      decompressedOutput.putInt(currentValue);

      prevValue = currentValue;
      prevDelta = currentDelta;
    }

    // Make buffer ready for reading
    decompressedOutput.flip();
    return numInts * Integer.BYTES;
  }

  @Override
  public int decompressedLength(ByteBuffer compressedInput) {
    // First 4 bytes contain the number of integers
    return compressedInput.getInt(compressedInput.position()) * Integer.BYTES;
  }
}
