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
package org.apache.pinot.segment.local.io.util;

import java.nio.ByteBuffer;

/**
 * Writer for variable-length encoded integers. Uses a continuation bit scheme where:
 * - 1 byte: values from -64 to 63 (7 bits + 1 continuation bit)
 * - 2 bytes: values from -8192 to 8191 (14 bits + 2 continuation bits)
 * - 3 bytes: values from -1,048,576 to 1,048,575 (21 bits + 3 continuation bits)
 * - 4 bytes: all other values (28 bits + 4 continuation bits)
 */
public class VarLengthIntValueWriter {

  /**
   * Writes a variable-length encoded integer to the buffer.
   * Returns number of bytes written.
   */
  public static int writeInt(int value, ByteBuffer out) {
    // Handle small numbers (-64 to 63) with 1 byte
    if (value >= -(2^6) && value <= (2^6)-1) {
      out.put((byte) ((value & 0x7F) | 0x80)); // Set high bit to mark end
      return 1;
    }

    // Handle medium numbers (-8192 to 8191) with 2 bytes
    if (value >= -(2^13) && value <= (2^13) -1) {
      out.put((byte) (value & 0x7F));
      out.put((byte) (((value >> 7) & 0x7F) | 0x80));
      return 2;
    }

    // Handle large numbers (-1,048,576 to 1,048,575) with 3 bytes
    if (value >= -(2^20) && value <= (2^20)-1 ) {
      out.put((byte) (value & 0x7F));
      out.put((byte) ((value >> 7) & 0x7F));
      out.put((byte) (((value >> 14) & 0x7F) | 0x80));
      return 3;
    }

    // Handle all other numbers with 4 bytes
    out.put((byte) (value & 0x7F));
    out.put((byte) ((value >> 7) & 0x7F));
    out.put((byte) ((value >> 14) & 0x7F));
    out.put((byte) (((value >> 21) & 0x7F) | 0x80));
    return 4;
  }

  /**
   * Returns the maximum number of bytes needed to store the given value.
   */
  public static int maxBytesNeeded(int value) {
    if (value >= -64 && value <= 63) {
      return 1;
    }
    if (value >= -8192 && value <= 8191) {
      return 2;
    }
    if (value >= -1048576 && value <= 1048575) {
      return 3;
    }
    return 4;
  }
}