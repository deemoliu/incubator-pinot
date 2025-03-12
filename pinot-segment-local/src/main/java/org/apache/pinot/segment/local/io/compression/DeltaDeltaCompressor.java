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

import org.apache.pinot.segment.spi.compression.ChunkCompressionType;
import org.apache.pinot.segment.spi.compression.ChunkCompressor;

import java.io.IOException;
import java.nio.ByteBuffer;


/**
 * A Delta of delta implementation of {@link ChunkCompressor}, that simply returns the input uncompressed data
 * with performing delta of delta encoding. This is useful in cases where cost of de-compression out-weighs benefit of
 * compression.
 */
class DeltaDeltaCompressor implements ChunkCompressor {

  static final DeltaDeltaCompressor INSTANCE = new DeltaDeltaCompressor();

  private DeltaDeltaCompressor() {
  }

  /**
   * The compression works by:
   * (1) Storing the first value as-is
   * (2) Computing and storing the first delta (difference between second and first value)
   * (3) For all subsequent values, storing the difference between consecutive deltas (delta of delta)
   * (4) During decompression, the process is reversed to reconstruct the original values
   *
   * The following scenarios data will be benefited from delta of delta compression
   * The data is sorted
   * The differences between consecutive values are relatively constant
   * The data consists of integers (it's specifically designed for integer sequences)
   * */
  @Override
  public int compress(ByteBuffer inUncompressed, ByteBuffer outCompressed)
      throws IOException {
    // Store original position to calculate compressed size
    int startPosition = outCompressed.position();

    // Get number of integers to compress
    int numInts = inUncompressed.remaining() / Integer.BYTES;
    if (numInts == 0) {
      outCompressed.putInt(0);
      outCompressed.flip();
      return 4;
    }

    // Store number of integers at the start
    outCompressed.putInt(numInts);

    // Store first value as-is
    int prevValue = inUncompressed.getInt();
    outCompressed.putInt(prevValue);

    if (numInts == 1) {
      outCompressed.flip();
      return 8;
    }

    // Store first delta as-is
    int prevDelta = inUncompressed.getInt() - prevValue;
    outCompressed.putInt(prevDelta);
    prevValue += prevDelta;

    // Compress remaining values using delta of delta
    for (int i = 2; i < numInts; i++) {
      int currentValue = inUncompressed.getInt();
      int currentDelta = currentValue - prevValue;
      int deltaOfDelta = currentDelta - prevDelta;

      // Store delta of delta
      outCompressed.putInt(deltaOfDelta);

      prevValue = currentValue;
      prevDelta = currentDelta;
    }

    // Make buffer ready for reading
    outCompressed.flip();
    return outCompressed.limit() - startPosition;
  }

  @Override
  public int maxCompressedSize(int uncompressedSize) {
    // In worst case, we need:
    // 4 bytes for number of integers
    // 4 bytes for first value
    // 4 bytes for first delta
    // 4 bytes for each delta of delta
    // So total size is same as uncompressed size plus 4 bytes for count
    return uncompressedSize + Integer.BYTES;
  }

  @Override
  public ChunkCompressionType compressionType() {
    return ChunkCompressionType.DELTADELTA;
  }
}
