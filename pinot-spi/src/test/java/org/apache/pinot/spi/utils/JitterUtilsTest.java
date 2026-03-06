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
package org.apache.pinot.spi.utils;

import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;


public class JitterUtilsTest {

  @Test
  public void testApplyVarianceWithZeroFraction() {
    assertEquals(JitterUtils.applyVariance(1000L, 0), 1000L);
    assertEquals(JitterUtils.applyVariance(1000L, -0.5), 1000L);
  }

  @Test
  public void testApplyVarianceRange() {
    long value = 10000L;
    double fraction = 0.1;
    long min = (long) (value * (1 - fraction));
    long max = (long) (value * (1 + fraction));

    for (int i = 0; i < 10000; i++) {
      long result = JitterUtils.applyVariance(value, fraction);
      assertTrue(result >= min, "Result " + result + " should be >= " + min);
      assertTrue(result <= max, "Result " + result + " should be <= " + max);
    }
  }

  @Test
  public void testRandomInRangeBasic() {
    long min = 100L;
    long max = 200L;

    for (int i = 0; i < 10000; i++) {
      long result = JitterUtils.randomInRange(min, max);
      assertTrue(result >= min, "Result " + result + " should be >= " + min);
      assertTrue(result < max, "Result " + result + " should be < " + max);
    }
  }

  @Test
  public void testRandomInRangeEqualBounds() {
    assertEquals(JitterUtils.randomInRange(100L, 100L), 100L);
  }

  @Test
  public void testRandomInRangeInvertedBounds() {
    assertEquals(JitterUtils.randomInRange(200L, 100L), 200L);
  }
}
