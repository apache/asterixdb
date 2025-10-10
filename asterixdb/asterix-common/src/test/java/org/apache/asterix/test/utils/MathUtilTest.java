/*
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

package org.apache.asterix.test.utils;

import org.apache.asterix.common.utils.MathUtil;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Assert;
import org.junit.Test;

public class MathUtilTest {
    private static final Logger LOGGER = LogManager.getLogger();
    private static final int BENCHMARK_ITERATIONS = 5000000;
    private final double LOG_2 = Math.log(2);

    @Test
    public void test() {
        for (int i = 0; i < Long.SIZE; i++) {
            long value = 1L << i;
            Assert.assertEquals(Long.toUnsignedString(value, 16),
                    value == Long.MIN_VALUE ? Long.SIZE - 1 : (long) (Math.log(value) / LOG_2),
                    MathUtil.log2Unsigned(value));
        }
    }

    @Test
    public void benchmarkLookupTable() {
        lookupTableRun();
        lookupTableRun();
    }

    @Test
    public void benchmarkMathLog() {
        mathLogRun();
        mathLogRun();
    }

    private void lookupTableRun() {
        long start = System.nanoTime();
        for (int j = 0; j < BENCHMARK_ITERATIONS; j++) {
            for (int i = 0; i < Long.SIZE; i++) {
                final long value = 1L << i;
                MathUtil.log2Unsigned(value);
            }
        }
        LOGGER.info("total time for {} iterations of lookup table: {}ns", BENCHMARK_ITERATIONS * Long.SIZE,
                System.nanoTime() - start);
    }

    private void mathLogRun() {
        long ignored = 0;
        long start = System.nanoTime();
        for (int j = 0; j < BENCHMARK_ITERATIONS; j++) {
            for (int i = 0; i < Long.SIZE; i++) {
                final long value = 1L << i;
                ignored += (long) (Math.log(value) / LOG_2);
            }
        }
        LOGGER.info("total time for {} iterations of Math.log(n) / Math.log(2): {}ns", BENCHMARK_ITERATIONS * Long.SIZE,
                System.nanoTime() - start);
    }
}
