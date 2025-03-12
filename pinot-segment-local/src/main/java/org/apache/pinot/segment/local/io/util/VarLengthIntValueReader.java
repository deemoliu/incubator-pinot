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
 * Reader for variable-length encoded integers. Uses a continuation bit scheme where:
 * - 1 byte: values from -64 to 63 (7 bits + 1 continuation bit)
 * - 2 bytes: values from -8192 to 8191 (14 bits + 2 continuation bits)
 * - 3 bytes: values from -1,048,576 to 1,048,575 (21 bits + 3 continuation bits)
 * - 4 bytes: all other values (28 bits + 4 continuation bits)
 */
public class VarLengthIntValueReader {

  /**
   * Reads a variable-length encoded integer from the buffer.
   * The high bit of the last byte is set to 1, all other continuation bits are 0.
   */
  public static int readInt(ByteBuffer in) {
    int value = 0;
    int shift = 0;
    byte b;
    do {
      b = in.get();
      value |= (b & 0x7F) << shift;
      shift += 7;
    } while ((b & 0x80) == 0);

    // Sign extend if necessary (if highest bit of last 7-bit group is 1)
    if ((b & 0x40) != 0 && shift < 32) {
      value |= (~0 << shift);
    }
    return value;
  }

  /**
   * Returns the number of bytes used to encode the next integer in the buffer without advancing the buffer position.
   */
  public static int bytesNeeded(ByteBuffer in) {
    int count = 1;
    int position = in.position();
    while ((in.get(position++) & 0x80) == 0) {
      count++;
    }
    return count;
  }
}