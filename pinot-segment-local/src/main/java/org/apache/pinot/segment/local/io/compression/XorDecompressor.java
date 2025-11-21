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
 * Decompressor for XOR-compressed integer/long sequences produced by {@link XorCompressor}.
 */
class XorDecompressor implements ChunkDecompressor {

  static final XorDecompressor INSTANCE = new XorDecompressor();
  static final LZ4Factory LZ4_FACTORY = LZ4Factory.fastestInstance();
  private static final byte INT_FLAG = 0;
  private static final byte LONG_FLAG = 1;

  private XorDecompressor() {
  }

  @Override
  public int decompress(ByteBuffer compressedInput, ByteBuffer decompressedOutput)
      throws IOException {
    byte flag = compressedInput.get();
    if (flag == LONG_FLAG) {
      return decompressLongs(compressedInput, decompressedOutput);
    } else if (flag == INT_FLAG) {
      return decompressInts(compressedInput, decompressedOutput);
    } else {
      throw new IOException("Unknown XOR compression type flag: " + flag);
    }
  }

  private int decompressInts(ByteBuffer compressedInput, ByteBuffer out) {
    int count = compressedInput.getInt();
    if (count == 0) {
      out.flip();
      return 0;
    }
    int prev = compressedInput.getInt();
    out.putInt(prev);
    if (count == 1) {
      out.flip();
      return Integer.BYTES;
    }
    int compSize = compressedInput.getInt();
    ByteBuffer comp = compressedInput.slice();
    comp.limit(compSize);
    ByteBuffer xorBuf = ByteBuffer.allocate((count - 1) * Integer.BYTES);
    LZ4_FACTORY.safeDecompressor().decompress(comp, xorBuf);
    xorBuf.flip();
    for (int i = 1; i < count; i++) {
      int x = xorBuf.getInt();
      int cur = prev ^ x;
      out.putInt(cur);
      prev = cur;
    }
    out.flip();
    return count * Integer.BYTES;
  }

  private int decompressLongs(ByteBuffer compressedInput, ByteBuffer out) {
    int count = compressedInput.getInt();
    if (count == 0) {
      out.flip();
      return 0;
    }
    long prev = compressedInput.getLong();
    out.putLong(prev);
    if (count == 1) {
      out.flip();
      return Long.BYTES;
    }
    int compSize = compressedInput.getInt();
    ByteBuffer comp = compressedInput.slice();
    comp.limit(compSize);
    ByteBuffer xorBuf = ByteBuffer.allocate((count - 1) * Long.BYTES);
    LZ4_FACTORY.safeDecompressor().decompress(comp, xorBuf);
    xorBuf.flip();
    for (int i = 1; i < count; i++) {
      long x = xorBuf.getLong();
      long cur = prev ^ x;
      out.putLong(cur);
      prev = cur;
    }
    out.flip();
    return count * Long.BYTES;
  }

  @Override
  public int decompressedLength(ByteBuffer compressedInput) {
    byte flag = compressedInput.get(compressedInput.position());
    int numValues = compressedInput.getInt(compressedInput.position() + 1);
    return flag == LONG_FLAG ? numValues * Long.BYTES : numValues * Integer.BYTES;
  }
}


