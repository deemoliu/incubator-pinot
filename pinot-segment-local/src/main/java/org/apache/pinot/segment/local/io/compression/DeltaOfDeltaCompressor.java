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
 * Implementation of {@link ChunkCompressor} using delta of delta compression with LZ4.
 * This compressor is optimized for sorted integer sequences where the differences between consecutive values
 * tend to be small and relatively constant. The delta values are further compressed using LZ4.
 */
class DeltaOfDeltaCompressor implements ChunkCompressor {

  static final DeltaOfDeltaCompressor INSTANCE = new DeltaOfDeltaCompressor();
  private static final LZ4Factory LZ4_FACTORY = LZ4Factory.fastestInstance();
  private static final byte INT_FLAG = 0;
  private static final byte LONG_FLAG = 1;

  private DeltaOfDeltaCompressor() {
  }

  @Override
  public int compress(ByteBuffer inUncompressed, ByteBuffer outCompressed) throws IOException {
    // Store original position to calculate compressed size
    int startPosition = outCompressed.position();
    
    // Determine if we're compressing ints or longs based on remaining bytes
    int remaining = inUncompressed.remaining();
    if (remaining % Long.BYTES == 0 && remaining > 0) {
      outCompressed.put(LONG_FLAG);
      return compressForLong(inUncompressed, outCompressed, startPosition);
    } else if (remaining % Integer.BYTES == 0) {
      outCompressed.put(INT_FLAG);
      return compressForInt(inUncompressed, outCompressed, startPosition);
    } else {
      throw new IOException("Input buffer size must be multiple of 4 (for ints) or 8 (for longs) bytes");
    }
  }

  private int compressForInt(ByteBuffer inUncompressed, ByteBuffer outCompressed, int startPosition) throws IOException {
    // Get number of integers to compress
    int numInts = inUncompressed.remaining() / Integer.BYTES;
    if (numInts == 0) {
      outCompressed.putInt(0);
      outCompressed.flip();
      return 5; // 1 byte flag + 4 bytes for numInts
    }

    // Store number of integers at the start
    outCompressed.putInt(numInts);

    // Store first value as-is
    int prevValue = inUncompressed.getInt();
    outCompressed.putInt(prevValue);

    if (numInts == 1) {
      outCompressed.flip();
      return outCompressed.position() - startPosition;
    }

    // Create temporary buffer for delta values before LZ4 compression
    ByteBuffer deltaBuffer = ByteBuffer.allocate((numInts - 1) * Integer.BYTES);

    // Store first delta
    int prevDelta = inUncompressed.getInt() - prevValue;
    deltaBuffer.putInt(prevDelta);
    prevValue += prevDelta;

    // Calculate remaining deltas
    for (int i = 2; i < numInts; i++) {
      int currentValue = inUncompressed.getInt();
      int currentDelta = currentValue - prevValue;
      int deltaOfDelta = currentDelta - prevDelta;
      
      deltaBuffer.putInt(deltaOfDelta);
      
      prevValue = currentValue;
      prevDelta = currentDelta;
    }

    // Prepare delta buffer for reading
    deltaBuffer.flip();

    // Reserve space for compressed size
    outCompressed.position(outCompressed.position() + Integer.BYTES);
    int compressedStart = outCompressed.position();

    // Compress delta values using LZ4
    LZ4_FACTORY.fastCompressor().compress(deltaBuffer, outCompressed);
    
    // Record compressed size
    int compressedSize = outCompressed.position() - compressedStart;
    outCompressed.putInt(compressedStart - Integer.BYTES, compressedSize);

    // Make buffer ready for reading
    outCompressed.flip();
    return outCompressed.limit() - startPosition;
  }

  private int compressForLong(ByteBuffer inUncompressed, ByteBuffer outCompressed, int startPosition) throws IOException {
    // Get number of longs to compress
    int numLongs = inUncompressed.remaining() / Long.BYTES;
    if (numLongs == 0) {
      outCompressed.putInt(0);
      outCompressed.flip();
      return 5; // 1 byte flag + 4 bytes for numLongs
    }

    // Store number of longs at the start
    outCompressed.putInt(numLongs);

    // Store first value as-is
    long prevValue = inUncompressed.getLong();
    outCompressed.putLong(prevValue);

    if (numLongs == 1) {
      outCompressed.flip();
      return outCompressed.position() - startPosition;
    }

    // Create temporary buffer for delta values before LZ4 compression
    ByteBuffer deltaBuffer = ByteBuffer.allocate((numLongs - 1) * Long.BYTES);

    // Store first delta
    long prevDelta = inUncompressed.getLong() - prevValue;
    deltaBuffer.putLong(prevDelta);
    prevValue += prevDelta;

    // Calculate remaining deltas
    for (int i = 2; i < numLongs; i++) {
      long currentValue = inUncompressed.getLong();
      long currentDelta = currentValue - prevValue;
      long deltaOfDelta = currentDelta - prevDelta;
      
      deltaBuffer.putLong(deltaOfDelta);
      
      prevValue = currentValue;
      prevDelta = currentDelta;
    }

    // Prepare delta buffer for reading
    deltaBuffer.flip();

    // Reserve space for compressed size
    outCompressed.position(outCompressed.position() + Integer.BYTES);
    int compressedStart = outCompressed.position();

    // Compress delta values using LZ4
    LZ4_FACTORY.fastCompressor().compress(deltaBuffer, outCompressed);
    
    // Record compressed size
    int compressedSize = outCompressed.position() - compressedStart;
    outCompressed.putInt(compressedStart - Integer.BYTES, compressedSize);

    // Make buffer ready for reading
    outCompressed.flip();
    return outCompressed.limit() - startPosition;
  }

  @Override
  public int maxCompressedSize(int uncompressedSize) {
    // Add 1 byte for type flag
    int baseSize = 1;
    
    // Determine if we're handling longs or ints
    if (uncompressedSize % Long.BYTES == 0 && uncompressedSize > 0) {
      int numLongs = uncompressedSize / Long.BYTES;
      if (numLongs == 0) {
        return baseSize + 4; // flag + numLongs
      }
      if (numLongs == 1) {
        return baseSize + 12; // flag + numLongs + one long value
      }
      int deltaSize = (numLongs - 1) * Long.BYTES;
      return baseSize + 16 + LZ4_FACTORY.fastCompressor().maxCompressedLength(deltaSize); // flag + numLongs + first value + compressed size + compressed data
    } else {
      int numInts = uncompressedSize / Integer.BYTES;
      if (numInts == 0) {
        return baseSize + 4; // flag + numInts
      }
      if (numInts == 1) {
        return baseSize + 8; // flag + numInts + one int value
      }
      int deltaSize = (numInts - 1) * Integer.BYTES;
      return baseSize + 12 + LZ4_FACTORY.fastCompressor().maxCompressedLength(deltaSize); // flag + numInts + first value + compressed size + compressed data
    }
  }

  @Override
  public ChunkCompressionType compressionType() {
    return ChunkCompressionType.DELTA_OF_DELTA;
  }
} 