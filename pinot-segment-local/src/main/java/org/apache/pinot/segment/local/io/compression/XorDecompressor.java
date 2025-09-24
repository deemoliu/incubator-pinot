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


class XorDecompressor implements ChunkDecompressor {

  static final XorDecompressor INSTANCE = new XorDecompressor();
  static final LZ4Factory LZ4_FACTORY = LZ4Factory.fastestInstance();
  private static final byte LONG_FLAG = 1;

  private XorDecompressor() {
  }

  @Override
  public int decompress(ByteBuffer compressedInput, ByteBuffer decompressedOutput)
      throws IOException {
    byte flag = compressedInput.get();
    if (flag != LONG_FLAG) {
      throw new IOException("Invalid input: only LONG flag supported, got " + flag);
    }
    int numLongs = compressedInput.getInt();
    if (numLongs == 0) {
      decompressedOutput.flip();
      return 0;
    }

    long prev = compressedInput.getLong();
    decompressedOutput.putLong(prev);
    if (numLongs == 1) {
      decompressedOutput.flip();
      return Long.BYTES;
    }

    int compressedSize = compressedInput.getInt();
    ByteBuffer compressedXors = compressedInput.slice();
    compressedXors.limit(compressedSize);
    ByteBuffer xorBuffer = ByteBuffer.allocate((numLongs - 1) * Long.BYTES);
    LZ4_FACTORY.safeDecompressor().decompress(compressedXors, xorBuffer);
    xorBuffer.flip();

    for (int i = 1; i < numLongs; i++) {
      long x = xorBuffer.getLong();
      long cur = prev ^ x;
      decompressedOutput.putLong(cur);
      prev = cur;
    }
    decompressedOutput.flip();
    return numLongs * Long.BYTES;
  }

  @Override
  public int decompressedLength(ByteBuffer compressedInput) {
    byte flag = compressedInput.get(compressedInput.position());
    if (flag != LONG_FLAG) {
      throw new IllegalStateException("Invalid input: only LONG flag supported, got " + flag);
    }
    int numValues = compressedInput.getInt(compressedInput.position() + 1);
    return numValues * Long.BYTES;
  }
}


