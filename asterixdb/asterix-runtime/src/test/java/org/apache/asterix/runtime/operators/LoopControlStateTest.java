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
package org.apache.asterix.runtime.operators;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.asterix.runtime.operators.kmeans.LoopControlState;
import org.apache.hyracks.api.dataflow.ActivityId;
import org.apache.hyracks.api.dataflow.OperatorDescriptorId;
import org.apache.hyracks.api.dataflow.TaskId;
import org.apache.hyracks.api.job.JobId;
import org.junit.Test;

/**
 * The loop-pacing contract of {@link LoopControlState}, and the abort path in particular.
 * <p>
 * Note what abort is and is not for. A job abort interrupts the head -- {@code Task.abort} interrupts every
 * thread {@code Task.run} registered, and {@code tryAcquire(long, TimeUnit)} is interruptible -- so a cancelled
 * query never waits out the turn timeout, with or without this mechanism. Abort covers the gap before that
 * interrupt arrives, failures that never reach {@code Task.abort}, and, whatever the timing, making the head
 * raise rather than proceed on a loop whose tail is gone. That last property is what these tests pin.
 */
public class LoopControlStateTest {

    private static LoopControlState newState() {
        return new LoopControlState(new JobId(1), "loop#0",
                new TaskId(new ActivityId(new OperatorDescriptorId(1), 0), 0));
    }

    /** A released turn is consumed by the waiter, which then proceeds. */
    @Test
    public void releasedTurnLetsTheHeadProceed() throws Exception {
        LoopControlState state = newState();
        state.releaseTurn();
        state.awaitTurn("test loop"); // returns, does not throw
    }

    /** abort() wakes a parked head promptly, and it raises rather than proceeding on a dead loop. */
    @Test
    public void abortWakesTheHeadImmediately() throws Exception {
        LoopControlState state = newState();
        CountDownLatch parked = new CountDownLatch(1);
        AtomicReference<Throwable> raised = new AtomicReference<>();

        Thread head = new Thread(() -> {
            parked.countDown();
            try {
                state.awaitTurn("test loop");
            } catch (Throwable t) {
                raised.set(t);
            }
        });
        // Daemon on purpose: if this ever regresses, the waiter parks for the full turn timeout, and a
        // non-daemon thread would hang the build for 30 minutes instead of failing in 10 seconds.
        head.setDaemon(true);
        head.start();
        assertTrue("head never started", parked.await(10, TimeUnit.SECONDS));

        long startNanos = System.nanoTime();
        state.abort();
        head.join(TimeUnit.SECONDS.toMillis(10));
        long elapsedMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNanos);

        assertTrue("head still parked after abort", !head.isAlive());
        assertNotNull("abort must make the head raise, not proceed", raised.get());
        assertTrue("error should name the abort, not a timeout: " + raised.get().getMessage(),
                raised.get().getMessage().contains("aborted"));
        // The point of the change: seconds, not the 30-minute turn timeout.
        assertTrue("abort took " + elapsedMillis + " ms; should be near-immediate", elapsedMillis < 10_000);
    }

    /** abort() is sticky, so a head that parks after the failure also raises instead of hanging. */
    @Test
    public void abortIsStickyForLaterWaiters() throws Exception {
        LoopControlState state = newState();
        state.abort();
        // On a daemon thread with a bounded join for the same reason as above: on the un-fixed code this
        // call parks for the turn timeout, and on the main thread that would hang the build rather than fail.
        AtomicReference<Throwable> raised = new AtomicReference<>();
        Thread late = new Thread(() -> {
            try {
                state.awaitTurn("test loop");
            } catch (Throwable t) {
                raised.set(t);
            }
        });
        late.setDaemon(true);
        late.start();
        late.join(TimeUnit.SECONDS.toMillis(10));
        assertNotNull("a head parking after the abort must still raise", raised.get());
        assertTrue(raised.get().getMessage().contains("aborted"));
    }

    /** Every co-located waiter is woken, not just the first. */
    @Test
    public void abortWakesEveryWaiter() throws Exception {
        LoopControlState state = newState();
        int waiters = 4;
        CountDownLatch parked = new CountDownLatch(waiters);
        CountDownLatch raised = new CountDownLatch(waiters);
        for (int i = 0; i < waiters; i++) {
            Thread w = new Thread(() -> {
                parked.countDown();
                try {
                    state.awaitTurn("test loop");
                } catch (Throwable t) {
                    raised.countDown();
                }
            });
            w.setDaemon(true); // see abortWakesTheHeadImmediately: never let a regression hang the build
            w.start();
        }
        assertTrue(parked.await(10, TimeUnit.SECONDS));
        state.abort();
        assertTrue("not every waiter was woken", raised.await(10, TimeUnit.SECONDS));
        assertEquals(0, raised.getCount());
    }
}
