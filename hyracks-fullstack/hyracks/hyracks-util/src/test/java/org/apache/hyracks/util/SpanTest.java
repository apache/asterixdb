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

package org.apache.hyracks.util;

import static org.apache.hyracks.util.annotations.AiProvenance.Agent.CLAUDE_SONNET_4_6;
import static org.apache.hyracks.util.annotations.AiProvenance.ContributionKind.TEST_GENERATED;
import static org.apache.hyracks.util.annotations.AiProvenance.Tool.GITHUB_COPILOT;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.hyracks.util.annotations.AiProvenance;
import org.junit.Test;

@AiProvenance(agent = CLAUDE_SONNET_4_6, tool = GITHUB_COPILOT, contributionKind = TEST_GENERATED, notes = "Tests for startElapsed(), immutable ELAPSED sentinel, and elapsed() >= boundary fix")
public class SpanTest {

    // --- Span.ELAPSED ---

    @Test
    public void elapsedConstant_alwaysElapsed() {
        assertTrue(Span.ELAPSED.elapsed());
    }

    @Test
    public void elapsedConstant_resetIsNoOp() {
        Span.ELAPSED.reset();
        assertTrue("ELAPSED.reset() should be a no-op; span should still be elapsed", Span.ELAPSED.elapsed());
    }

    @Test
    public void elapsedConstant_remainingIsZero() {
        assertEquals(0, Span.ELAPSED.remaining(TimeUnit.NANOSECONDS));
        assertEquals(0, Span.ELAPSED.remaining(TimeUnit.SECONDS));
    }

    @Test
    public void elapsedConstant_elapsedUnitIsMaxValue() {
        assertEquals(Long.MAX_VALUE, Span.ELAPSED.elapsed(TimeUnit.NANOSECONDS));
    }

    @Test
    public void elapsedConstant_awaitReturnsFalseImmediately() throws InterruptedException {
        CountDownLatch latch = new CountDownLatch(1);
        assertFalse(Span.ELAPSED.await(latch));
    }

    @Test
    public void elapsedConstant_toStringIsElapsed() {
        assertEquals("<ELAPSED>", Span.ELAPSED.toString());
    }

    // --- Span.startElapsed() ---

    @Test
    public void startElapsed_immediatelyElapsed() {
        Span span = Span.startElapsed(5, TimeUnit.SECONDS);
        assertTrue("startElapsed span should immediately report elapsed()", span.elapsed());
    }

    @Test
    public void startElapsed_notElapsedAfterReset() {
        Span span = Span.startElapsed(5, TimeUnit.SECONDS);
        span.reset();
        assertFalse("startElapsed span should not be elapsed immediately after reset()", span.elapsed());
    }

    @Test
    public void startElapsed_elapsedAfterSpanDuration() throws InterruptedException {
        Span span = Span.startElapsed(10, TimeUnit.MILLISECONDS);
        span.reset();
        assertFalse(span.elapsed());
        TimeUnit.MILLISECONDS.sleep(15);
        assertTrue("startElapsed span should be elapsed after duration has passed", span.elapsed());
    }

    @Test
    public void startElapsed_remainingIsZeroBeforeReset() {
        Span span = Span.startElapsed(5, TimeUnit.SECONDS);
        assertEquals(0, span.remaining(TimeUnit.NANOSECONDS));
    }

    @Test
    public void startElapsed_remainingIsPositiveAfterReset() {
        Span span = Span.startElapsed(5, TimeUnit.SECONDS);
        span.reset();
        assertTrue("remaining should be positive after reset", span.remaining(TimeUnit.MILLISECONDS) > 0);
    }

    // --- elapsed() boundary: >= ensures span is elapsed at exactly spanNanos ---

    @Test
    public void start_elapsedAtExactlySpanDuration() {
        // A span of 0ns should be immediately elapsed (>= 0)
        Span span = Span.start(0, TimeUnit.NANOSECONDS);
        assertTrue("0ns span should be elapsed immediately (elapsed >= spanNanos)", span.elapsed());
    }

    // --- Span.INFINITE sanity ---

    @Test
    public void infiniteConstant_neverElapsed() {
        assertFalse(Span.INFINITE.elapsed());
    }

    @Test
    public void infiniteConstant_remainingIsMaxValue() {
        assertEquals(Long.MAX_VALUE, Span.INFINITE.remaining(TimeUnit.NANOSECONDS));
    }
}
