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
import org.apache.pinot.segment.spi.compression.ChunkCompressor;
import org.apache.pinot.segment.spi.compression.ChunkDecompressor;
import org.testng.Assert;
import org.testng.annotations.Test;

public class DeltaDeltaCompressionTest {
  ChunkCompressor deltaCompressor = DeltaCompressor.INSTANCE;
  ChunkDecompressor deltaDecompressor = DeltaDecompressor.INSTANCE;
  ChunkCompressor compressor = DeltaDeltaCompressor.INSTANCE;
  ChunkDecompressor decompressor = DeltaDeltaDecompressor.INSTANCE;

  @Test
  public void testEmptyInput() throws IOException {
    // Create empty input buffer
    ByteBuffer input = ByteBuffer.allocate(0);
    ByteBuffer compressed = ByteBuffer.allocate(compressor.maxCompressedSize(0));

    // Compress
    int compressedSize = compressor.compress(input, compressed);
    Assert.assertEquals(compressedSize, 4); // Should only contain count (0)

    // Decompress
    ByteBuffer decompressed = ByteBuffer.allocate(decompressor.decompressedLength(compressed));
    int decompressedSize = decompressor.decompress(compressed, decompressed);
    Assert.assertEquals(decompressedSize, 0);
  }

  @Test
  public void testSingleValue() throws IOException {
    // Create input with single value
    ByteBuffer input = ByteBuffer.allocate(Integer.BYTES);
    input.putInt(22993);
    input.flip();

    // Compress
    ByteBuffer compressed = ByteBuffer.allocate(compressor.maxCompressedSize(input.capacity()));
    int compressedSize = compressor.compress(input, compressed);
    Assert.assertEquals(compressedSize, 8); // count(4bytes) + value(4)

    // Decompress
    ByteBuffer decompressed = ByteBuffer.allocate(decompressor.decompressedLength(compressed));
    int decompressedSize = decompressor.decompress(compressed, decompressed);
    Assert.assertEquals(decompressedSize, Integer.BYTES);
    Assert.assertEquals(decompressed.getInt(0), 22993);
  }

  @Test
  public void testSequentialValues() throws IOException {
    // Create input with sequential values (1,2,3,4,5)
    int[] values = {1, 2, 3, 4, 5};
    ByteBuffer input = ByteBuffer.allocate(values.length * Integer.BYTES);
    for (int value : values) {
      input.putInt(value);
    }
    input.flip();

    // Compress
    ByteBuffer compressed = ByteBuffer.allocate(compressor.maxCompressedSize(input.capacity()));
    int compressedSize = compressor.compress(input, compressed);
    Assert.assertEquals(compressedSize, 24);

    // Decompress
    ByteBuffer decompressed = ByteBuffer.allocate(decompressor.decompressedLength(compressed));
    decompressor.decompress(compressed, decompressed);

    // Verify values
    for (int value : values) {
      Assert.assertEquals(decompressed.getInt(), value);
    }
  }

  @Test
  public void testNonSequentialValues() throws IOException {
    // Create input with non-sequential values
    int[] values = {10, 15, 12, 18, 13};
    ByteBuffer input = ByteBuffer.allocate(values.length * Integer.BYTES);
    for (int value : values) {
      input.putInt(value);
    }
    input.flip();

    // Compress
    ByteBuffer compressed = ByteBuffer.allocate(compressor.maxCompressedSize(input.capacity()));
    compressor.compress(input, compressed);

    // Decompress
    ByteBuffer decompressed = ByteBuffer.allocate(decompressor.decompressedLength(compressed));
    decompressor.decompress(compressed, decompressed);

    // Verify values
    for (int value : values) {
      Assert.assertEquals(decompressed.getInt(), value);
    }
  }

  @Test
  public void testLargeDeltas() throws IOException {
    // Create input with large deltas
    int[] values = {1000, 5000, 20000, 100000, 500000};
    ByteBuffer input = ByteBuffer.allocate(values.length * Integer.BYTES);
    for (int value : values) {
      input.putInt(value);
    }
    input.flip();

    // Compress
    ByteBuffer compressed = ByteBuffer.allocate(compressor.maxCompressedSize(input.capacity()));
    compressor.compress(input, compressed);

    // Decompress
    ByteBuffer decompressed = ByteBuffer.allocate(decompressor.decompressedLength(compressed));
    decompressor.decompress(compressed, decompressed);

    // Verify values
    for (int value : values) {
      Assert.assertEquals(decompressed.getInt(), value);
    }
  }

  @Test
  public void testNegativeValues() throws IOException {
    // Create input with negative values
    int[] values = {-10, -5, 0, 5, 10};
    ByteBuffer input = ByteBuffer.allocate(values.length * Integer.BYTES);
    for (int value : values) {
      input.putInt(value);
    }
    input.flip();

    // Compress
    ByteBuffer compressed = ByteBuffer.allocate(compressor.maxCompressedSize(input.capacity()));
    compressor.compress(input, compressed);

    // Decompress
    ByteBuffer decompressed = ByteBuffer.allocate(decompressor.decompressedLength(compressed));
    decompressor.decompress(compressed, decompressed);

    // Verify values
    for (int value : values) {
      Assert.assertEquals(decompressed.getInt(), value);
    }
  }

  @Test
  public void testMaxCompressedSize() {
    // Test various input sizes
    Assert.assertEquals(compressor.maxCompressedSize(0), Integer.BYTES);
    Assert.assertEquals(compressor.maxCompressedSize(Integer.BYTES), Integer.BYTES + Integer.BYTES);
    Assert.assertEquals(compressor.maxCompressedSize(Integer.BYTES * 10), Integer.BYTES * 11);
  }

  @Test
  public void testDecompressedLength() throws IOException {
    // Create test input
    int[] values = {1, 2, 3, 4, 5};
    ByteBuffer input = ByteBuffer.allocate(values.length * Integer.BYTES);
    for (int value : values) {
      input.putInt(value);
    }
    input.flip();

    // Compress
    ByteBuffer compressed = ByteBuffer.allocate(compressor.maxCompressedSize(input.capacity()));
    compressor.compress(input, compressed);

    // Test decompressed length
    Assert.assertEquals(decompressor.decompressedLength(compressed), values.length * Integer.BYTES);
  }

}
