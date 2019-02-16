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
package org.apache.asterix.runtime;

import org.apache.asterix.common.api.Duration;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.junit.Assert;
import org.junit.Test;

public class ParseDurationTest {

    @Test
    public void test() throws Exception {
        // simple
        Assert.assertEquals(0, Duration.parseDurationStringToNanos("0"));
        Assert.assertEquals(java.util.concurrent.TimeUnit.SECONDS.toNanos(5),
                Duration.parseDurationStringToNanos("5s"));
        Assert.assertEquals(java.util.concurrent.TimeUnit.SECONDS.toNanos(30),
                Duration.parseDurationStringToNanos("30s"));
        Assert.assertEquals(java.util.concurrent.TimeUnit.SECONDS.toNanos(1478),
                Duration.parseDurationStringToNanos("1478s"));
        Assert.assertEquals(java.util.concurrent.TimeUnit.SECONDS.toNanos(-5),
                Duration.parseDurationStringToNanos("-5s"));
        Assert.assertEquals(java.util.concurrent.TimeUnit.SECONDS.toNanos(5),
                Duration.parseDurationStringToNanos("+5s"));
        Assert.assertEquals(java.util.concurrent.TimeUnit.SECONDS.toNanos(0),
                Duration.parseDurationStringToNanos("-0"));
        Assert.assertEquals(java.util.concurrent.TimeUnit.SECONDS.toNanos(0),
                Duration.parseDurationStringToNanos("+0"));
        Assert.assertEquals(java.util.concurrent.TimeUnit.SECONDS.toNanos(5),
                Duration.parseDurationStringToNanos("5.0s"));
        Assert.assertEquals(
                java.util.concurrent.TimeUnit.SECONDS.toNanos(5)
                        + java.util.concurrent.TimeUnit.MILLISECONDS.toNanos(600),
                Duration.parseDurationStringToNanos("5.6s"));
        Assert.assertEquals(java.util.concurrent.TimeUnit.SECONDS.toNanos(5),
                Duration.parseDurationStringToNanos("5.s"));
        Assert.assertEquals(java.util.concurrent.TimeUnit.MILLISECONDS.toNanos(500),
                Duration.parseDurationStringToNanos(".5s"));
        Assert.assertEquals(java.util.concurrent.TimeUnit.SECONDS.toNanos(1),
                Duration.parseDurationStringToNanos("1.0s"));
        Assert.assertEquals(java.util.concurrent.TimeUnit.SECONDS.toNanos(1),
                Duration.parseDurationStringToNanos("1.00s"));
        Assert.assertEquals(
                java.util.concurrent.TimeUnit.SECONDS.toNanos(1)
                        + java.util.concurrent.TimeUnit.MILLISECONDS.toNanos(4),
                Duration.parseDurationStringToNanos("1.004s"));
        Assert.assertEquals(
                java.util.concurrent.TimeUnit.SECONDS.toNanos(1)
                        + java.util.concurrent.TimeUnit.MILLISECONDS.toNanos(4),
                Duration.parseDurationStringToNanos("1.0040s"));
        Assert.assertEquals(
                java.util.concurrent.TimeUnit.SECONDS.toNanos(100)
                        + java.util.concurrent.TimeUnit.MILLISECONDS.toNanos(1),
                Duration.parseDurationStringToNanos("100.00100s"));
        Assert.assertEquals(java.util.concurrent.TimeUnit.NANOSECONDS.toNanos(10),
                Duration.parseDurationStringToNanos("10ns"));
        Assert.assertEquals(java.util.concurrent.TimeUnit.MICROSECONDS.toNanos(11),
                Duration.parseDurationStringToNanos("11us"));
        Assert.assertEquals(java.util.concurrent.TimeUnit.MICROSECONDS.toNanos(12),
                Duration.parseDurationStringToNanos("12µs"));
        Assert.assertEquals(java.util.concurrent.TimeUnit.MICROSECONDS.toNanos(12),
                Duration.parseDurationStringToNanos("12μs"));
        Assert.assertEquals(java.util.concurrent.TimeUnit.MILLISECONDS.toNanos(13),
                Duration.parseDurationStringToNanos("13ms"));
        Assert.assertEquals(java.util.concurrent.TimeUnit.SECONDS.toNanos(14),
                Duration.parseDurationStringToNanos("14s"));
        Assert.assertEquals(java.util.concurrent.TimeUnit.MINUTES.toNanos(15),
                Duration.parseDurationStringToNanos("15m"));
        Assert.assertEquals(java.util.concurrent.TimeUnit.HOURS.toNanos(16),
                Duration.parseDurationStringToNanos("16h"));
        Assert.assertEquals(
                java.util.concurrent.TimeUnit.HOURS.toNanos(3) + java.util.concurrent.TimeUnit.MINUTES.toNanos(30),
                Duration.parseDurationStringToNanos("3h30m"));
        Assert.assertEquals(
                java.util.concurrent.TimeUnit.SECONDS.toNanos(10)
                        + java.util.concurrent.TimeUnit.MILLISECONDS.toNanos(500)
                        + java.util.concurrent.TimeUnit.MINUTES.toNanos(4),
                Duration.parseDurationStringToNanos("10.5s4m"));
        Assert.assertEquals(
                java.util.concurrent.TimeUnit.MINUTES.toNanos(-2) + java.util.concurrent.TimeUnit.SECONDS.toNanos(-3)
                        + java.util.concurrent.TimeUnit.MILLISECONDS.toNanos(-400),
                Duration.parseDurationStringToNanos("-2m3.4s"));
        Assert.assertEquals(
                java.util.concurrent.TimeUnit.HOURS.toNanos(1) + java.util.concurrent.TimeUnit.MINUTES.toNanos(2)
                        + java.util.concurrent.TimeUnit.SECONDS.toNanos(3)
                        + java.util.concurrent.TimeUnit.MILLISECONDS.toNanos(4)
                        + java.util.concurrent.TimeUnit.MICROSECONDS.toNanos(5)
                        + java.util.concurrent.TimeUnit.NANOSECONDS.toNanos(6),
                Duration.parseDurationStringToNanos("1h2m3s4ms5us6ns"));
        Assert.assertEquals(
                java.util.concurrent.TimeUnit.HOURS.toNanos(39) + java.util.concurrent.TimeUnit.MINUTES.toNanos(9)
                        + java.util.concurrent.TimeUnit.SECONDS.toNanos(14)
                        + java.util.concurrent.TimeUnit.MILLISECONDS.toNanos(425),
                Duration.parseDurationStringToNanos("39h9m14.425s"));
        Assert.assertEquals(java.util.concurrent.TimeUnit.NANOSECONDS.toNanos(52763797000L),
                Duration.parseDurationStringToNanos("52763797000ns"));
        Assert.assertEquals(1199999998800L, Duration.parseDurationStringToNanos("0.3333333333333333333333h"));
        Assert.assertEquals(9007199254740993L, Duration.parseDurationStringToNanos("9007199254740993ns"));
        Assert.assertEquals(9223372036854775807L, Duration.parseDurationStringToNanos("9223372036854775807ns"));
        Assert.assertEquals(9223372036854775807L, Duration.parseDurationStringToNanos("9223372036854775.807us"));
        Assert.assertEquals(9223372036854775807L, Duration.parseDurationStringToNanos("9223372036s854ms775us807ns"));
        Assert.assertEquals(-9223372036854775807L, Duration.parseDurationStringToNanos("-9223372036854775807ns"));
        assertFail("");
        assertFail("3");
        assertFail("-");
        assertFail("s");
        assertFail(".");
        assertFail("-.");
        assertFail(".s");
        assertFail("+.s");
        assertFail("3000000h");
        assertFail("9223372036854775808ns");
        assertFail("9223372036854775.808us");
        assertFail("9223372036854ms775us808ns");
        assertFail("-9223372036854775808ns");
    }

    @Test
    public void testDurationFormatNanos() throws Exception {
        Assert.assertEquals("123.456789012s", Duration.formatNanos(123456789012l));
        Assert.assertEquals("12.345678901s", Duration.formatNanos(12345678901l));
        Assert.assertEquals("1.23456789s", Duration.formatNanos(1234567890l));
        Assert.assertEquals("123.456789ms", Duration.formatNanos(123456789l));
        Assert.assertEquals("12.345678ms", Duration.formatNanos(12345678l));
        Assert.assertEquals("1.234567ms", Duration.formatNanos(1234567l));
        Assert.assertEquals("123.456µs", Duration.formatNanos(123456l));
        Assert.assertEquals("12.345µs", Duration.formatNanos(12345l));
        Assert.assertEquals("12.345us", Duration.formatNanos(12345l, true));
        Assert.assertEquals("1.234µs", Duration.formatNanos(1234l));
        Assert.assertEquals("123ns", Duration.formatNanos(123l));
        Assert.assertEquals("12ns", Duration.formatNanos(12l));
        Assert.assertEquals("1ns", Duration.formatNanos(1l));
        Assert.assertEquals("-123.456789012s", Duration.formatNanos(-123456789012l));
        Assert.assertEquals("120s", Duration.formatNanos(120000000000l));
        Assert.assertEquals("-12ns", Duration.formatNanos(-12l));
    }

    private void assertFail(String duration) {
        try {
            Duration.parseDurationStringToNanos(duration);
            Assert.fail("Expected parseDuration(" + duration + ") to fail but it didn't");
        } catch (HyracksDataException hde) {
        }
    }
}