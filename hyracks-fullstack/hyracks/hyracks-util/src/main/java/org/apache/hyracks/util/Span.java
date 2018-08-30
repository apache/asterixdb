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

import java.util.concurrent.TimeUnit;

public class Span {
    private final long startNanos;
    private final long spanNanos;

    private Span(long span, TimeUnit unit) {
        startNanos = System.nanoTime();
        spanNanos = unit.toNanos(span);
    }

    public long getSpanNanos() {
        return spanNanos;
    }

    public static Span start(long span, TimeUnit unit) {
        return new Span(span, unit);
    }

    public boolean elapsed() {
        return elapsed(TimeUnit.NANOSECONDS) > spanNanos;
    }

    public long elapsed(TimeUnit unit) {
        return unit.convert(System.nanoTime() - startNanos, TimeUnit.NANOSECONDS);
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
