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

import java.util.concurrent.ThreadLocalRandom;


/**
 * Utility methods for applying jitter/randomization to values, useful for avoiding thundering herd problems
 * in retry policies and threshold computations.
 */
public class JitterUtils {
  private JitterUtils() {
  }

  /**
   * Returns the given value jittered within {@code [value * (1 - varianceFraction), value * (1 + varianceFraction)]}.
   * If {@code varianceFraction} is zero or negative, the value is returned unchanged.
   *
   * @param value the base value to jitter
   * @param varianceFraction the fraction of variance to apply (e.g. 0.1 for +/-10%)
   * @return the jittered value
   */
  public static long applyVariance(long value, double varianceFraction) {
    if (varianceFraction <= 0) {
      return value;
    }
    double variation = (1 - varianceFraction) + 2 * varianceFraction * ThreadLocalRandom.current().nextDouble();
    return (long) (value * variation);
  }

  /**
   * Returns a random long in {@code [minInclusive, maxExclusive)}. If {@code minInclusive >= maxExclusive},
   * returns {@code minInclusive}.
   *
   * @param minInclusive the lower bound (inclusive)
   * @param maxExclusive the upper bound (exclusive)
   * @return a random long in the specified range
   */
  public static long randomInRange(long minInclusive, long maxExclusive) {
    if (minInclusive >= maxExclusive) {
      return minInclusive;
    }
    return ThreadLocalRandom.current().nextLong(minInclusive, maxExclusive);
  }
}
