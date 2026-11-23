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
package org.apache.hyracks.storage.am.lsm.common.impls;

import java.util.concurrent.atomic.LongAdder;

/**
 * Process-wide totals of the time writers spend blocked trying to enter an LSM index's mutable memory component.
 * Only the slow path records anything: a writer that enters the component on its first try costs nothing here.
 */
public final class LSMWriterBlockedStats {

    public enum Reason {
        /**
         * the index's memory components are all full or being flushed, so the writer waits for a flush to finish
         */
        MEMORY_COMPONENT,
        /**
         * the mutable component is writable, but the memory budget it draws from is exhausted, or the component
         * has been chosen to be flushed to relieve it
         */
        MEMORY_BUDGET
    }

    private static final Reason[] REASONS = Reason.values();
    private static final LongAdder[] BLOCKED_NANOS = newAdders();
    private static final LongAdder[] BLOCKED_COUNT = newAdders();

    private LSMWriterBlockedStats() {
    }

    private static LongAdder[] newAdders() {
        LongAdder[] adders = new LongAdder[REASONS.length];
        for (int i = 0; i < adders.length; i++) {
            adders[i] = new LongAdder();
        }
        return adders;
    }

    /**
     * Records that a writer started to block for the given reason
     */
    public static void blocked(Reason reason) {
        BLOCKED_COUNT[reason.ordinal()].increment();
    }

    /**
     * Records time a writer spent blocked for the given reason
     */
    public static void blockedFor(Reason reason, long nanos) {
        BLOCKED_NANOS[reason.ordinal()].add(nanos);
    }

    public static long getBlockedNanos(Reason reason) {
        return BLOCKED_NANOS[reason.ordinal()].sum();
    }

    public static long getBlockedCount(Reason reason) {
        return BLOCKED_COUNT[reason.ordinal()].sum();
    }
}
