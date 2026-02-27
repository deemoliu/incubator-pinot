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
import org.apache.pinot.segment.spi.compression.ChunkCompressionType;
import org.apache.pinot.segment.spi.compression.ChunkCompressor;
import org.apache.pinot.segment.spi.compression.ChunkDecompressor;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;


public class XorCompressionTest {

  @Test
  public void testRoundTripEmptyFloat()
      throws IOException {
    assertFloatRoundTrip(new float[]{});
  }

  @Test
  public void testRoundTripSingleValueFloat()
      throws IOException {
    assertFloatRoundTrip(new float[]{42.5f});
  }

  @Test
  public void testRoundTripMultiValuesFloat()
      throws IOException {
    assertFloatRoundTrip(new float[]{1.0f, 1.25f, -0.0f, 3.14159f, Float.NaN, Float.POSITIVE_INFINITY});
  }

  @Test
  public void testRoundTripEmptyDouble()
      throws IOException {
    assertDoubleRoundTrip(new double[]{});
  }

  @Test
  public void testRoundTripSingleValueDouble()
      throws IOException {
    assertDoubleRoundTrip(new double[]{42.5d});
  }

  @Test
  public void testRoundTripMultiValuesDouble()
      throws IOException {
    assertDoubleRoundTrip(new double[]{1.0d, 1.25d, -0.0d, Math.PI, Double.NaN, Double.POSITIVE_INFINITY});
  }

  private static void assertFloatRoundTrip(float[] values)
      throws IOException {
    ByteBuffer input = ByteBuffer.allocateDirect(values.length * Float.BYTES);
    for (float v : values) {
      input.putFloat(v);
    }
    input.flip();

    try (ChunkCompressor compressor = ChunkCompressorFactory.getCompressor(ChunkCompressionType.XOR)) {
      ByteBuffer compressed = ByteBuffer.allocateDirect(compressor.maxCompressedSize(input.limit()));
      compressor.compress(input.slice(), compressed);

      try (ChunkDecompressor decompressor = ChunkCompressorFactory.getDecompressor(ChunkCompressionType.XOR)) {
        int decompressedSize = decompressor.decompressedLength(compressed);
        ByteBuffer decompressed = ByteBuffer.allocateDirect(decompressedSize);
        int actualSize = decompressor.decompress(compressed, decompressed);
        assertEquals(actualSize, values.length * Float.BYTES);

        for (float expected : values) {
          assertEquals(Float.floatToRawIntBits(decompressed.getFloat()), Float.floatToRawIntBits(expected));
        }
      }
    }
  }

  private static void assertDoubleRoundTrip(double[] values)
      throws IOException {
    ByteBuffer input = ByteBuffer.allocateDirect(values.length * Double.BYTES);
    for (double v : values) {
      input.putDouble(v);
    }
    input.flip();

    try (ChunkCompressor compressor = ChunkCompressorFactory.getCompressor(ChunkCompressionType.XOR)) {
      ByteBuffer compressed = ByteBuffer.allocateDirect(compressor.maxCompressedSize(input.limit()));
      compressor.compress(input.slice(), compressed);

      try (ChunkDecompressor decompressor = ChunkCompressorFactory.getDecompressor(ChunkCompressionType.XOR)) {
        int decompressedSize = decompressor.decompressedLength(compressed);
        ByteBuffer decompressed = ByteBuffer.allocateDirect(decompressedSize);
        int actualSize = decompressor.decompress(compressed, decompressed);
        assertEquals(actualSize, values.length * Double.BYTES);

        for (double expected : values) {
          assertEquals(Double.doubleToRawLongBits(decompressed.getDouble()), Double.doubleToRawLongBits(expected));
        }
      }
    }
  }
}
