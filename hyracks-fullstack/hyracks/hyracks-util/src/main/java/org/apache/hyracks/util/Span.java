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

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;

public class Span {

    public static final Span INFINITE = new Span() {
        @Override
        public void reset() {
            //no-op
        }

        @Override
        public long getSpanNanos() {
            return Long.MAX_VALUE;
        }

        @Override
        public long getSpan(TimeUnit unit) {
            return Long.MAX_VALUE;
        }

        @Override
        public boolean elapsed() {
            return false;
        }

        @Override
        public long elapsed(TimeUnit unit) {
            return -1;
        }

        @Override
        public void sleep() throws InterruptedException {
            new Semaphore(0).acquire();
        }

        @Override
        public void sleep(long sleep, TimeUnit unit) throws InterruptedException {
            unit.sleep(sleep);
        }

        @Override
        public long remaining(TimeUnit unit) {
            return Long.MAX_VALUE;
        }

        @Override
        public void wait(Object monitor) throws InterruptedException {
            monitor.wait();
        }

        @Override
        public boolean await(CountDownLatch latch) throws InterruptedException {
            latch.await();
            return true;
        }

        @Override
        public String toString() {
            return "<INFINITE>";
        }
    };

    public static final Span ELAPSED = start(0, TimeUnit.NANOSECONDS);

    private final long spanNanos;
    private volatile long startNanos;

    private Span() {
        spanNanos = startNanos = -1;
    }

    private Span(long span, TimeUnit unit) {
        spanNanos = unit.toNanos(span);
    }

    public void reset() {
        startNanos = System.nanoTime();
    }

    public long getSpanNanos() {
        return spanNanos;
    }

    public long getSpan(TimeUnit unit) {
        return unit.convert(spanNanos, TimeUnit.NANOSECONDS);
    }

    public static Span start(long span, TimeUnit unit) {
        Span s = new Span(span, unit);
        s.reset();
        return s;
    }

    public static Span init(long span, TimeUnit unit) {
        return new Span(span, unit);
    }

    public boolean elapsed() {
        return elapsed(TimeUnit.NANOSECONDS) > spanNanos;
    }

    public long elapsed(TimeUnit unit) {
        return unit.convert(System.nanoTime() - startNanos, TimeUnit.NANOSECONDS);
    }

    /**
     * Sleep for the remainder of this span
     *
     * @throws InterruptedException
     */
    public void sleep() throws InterruptedException {
        TimeUnit.NANOSECONDS.sleep(remaining(TimeUnit.NANOSECONDS));
    }

    /**
     * Sleep for the minimum of the duration or the remaining of this span
     *
     * @param sleep
     *            the amount to sleep
     * @param unit
     *            the unit of the amount
     * @throws InterruptedException
     */
    public void sleep(long sleep, TimeUnit unit) throws InterruptedException {
        TimeUnit.NANOSECONDS.sleep(Math.min(remaining(TimeUnit.NANOSECONDS), unit.toNanos(sleep)));
    }

    public long remaining(TimeUnit unit) {
        return unit.convert(Long.max(spanNanos - elapsed(TimeUnit.NANOSECONDS), 0L), TimeUnit.NANOSECONDS);
    }

    public void wait(Object monitor) throws InterruptedException {
        TimeUnit.NANOSECONDS.timedWait(monitor, remaining(TimeUnit.NANOSECONDS));
    }

    public boolean await(CountDownLatch latch) throws InterruptedException {
        return latch.await(remaining(TimeUnit.NANOSECONDS), TimeUnit.NANOSECONDS);
    }

    /**
     * tryAcquireUninterruptibly will attempt to acquire the semaphore until the span has elapsed or the semaphore is
     * acquired, disregarding any interruptions. If an interruption occurs, the thread will be re-interrupted upon
     * method completion.
     * @param semaphore the semaphore to acquire
     * @return true if the semaphore was acquired, false if the span elapsed
     */
    public boolean tryAcquireUninterruptibly(Semaphore semaphore) {
        return retryUninterruptibly(semaphore::tryAcquire, v -> v);
    }

    /**
     * retryUninterruptibly will attempt to execute the action until the span has elapsed or the action has succeeded
     * @param action the action to execute
     * @param test the predicate to determine if the action has succeeded
     * @return true if the action succeeded, false if the span elapsed
     */
    public <V> boolean retryUninterruptibly(InterruptableBiFunction<Long, TimeUnit, V> action, Predicate<V> test) {
        boolean interrupted = Thread.interrupted();
        try {
            while (!elapsed()) {
                try {
                    if (test.test(action.process(remaining(TimeUnit.NANOSECONDS), TimeUnit.NANOSECONDS))) {
                        return true;
                    }
                } catch (InterruptedException e) { // NOSONAR- we will re-interrupt the thread during unwind
                    interrupted = true;
                }
            }
            return false;
        } finally {
            if (interrupted) {
                Thread.currentThread().interrupt();
            }
        }
    }

    public void loopUntilExhausted(ThrowingAction action) throws Exception {
        loopUntilExhausted(action, 0, TimeUnit.NANOSECONDS);
    }

    public void loopUntilExhausted(ThrowingAction action, long delay, TimeUnit delayUnit) throws Exception {
        while (!elapsed()) {
            action.run();
            if (elapsed(delayUnit) < delay) {
                break;
            }
            sleep(delay, delayUnit);
        }
    }

    @Override
    public String toString() {
        long nanos = spanNanos % 1000;
        long micros = TimeUnit.MICROSECONDS.convert(spanNanos, TimeUnit.NANOSECONDS) % 1000;
        long millis = TimeUnit.MILLISECONDS.convert(spanNanos, TimeUnit.NANOSECONDS) % 1000;
        long seconds = TimeUnit.SECONDS.convert(spanNanos, TimeUnit.NANOSECONDS) % 60;
        long minutes = TimeUnit.MINUTES.convert(spanNanos, TimeUnit.NANOSECONDS) % 60;
        long hours = TimeUnit.HOURS.convert(spanNanos, TimeUnit.NANOSECONDS) % 24;
        long days = TimeUnit.DAYS.convert(spanNanos, TimeUnit.NANOSECONDS);
        StringBuilder builder = new StringBuilder();
        if (days > 0) {
            builder.append(days).append("d");
        }
        if (hours > 0) {
            builder.append(hours).append("hr");
        }
        if (minutes > 0) {
            builder.append(minutes).append("m");
        }
        if (seconds > 0) {
            builder.append(seconds).append("s");
        }
        if (millis > 0) {
            builder.append(millis).append("ms");
        }
        if (micros > 0) {
            builder.append(micros).append("us");
        }
        if (nanos > 0 || builder.length() == 0) {
            builder.append(nanos).append("ns");
        }
        return builder.toString();
    }
}
