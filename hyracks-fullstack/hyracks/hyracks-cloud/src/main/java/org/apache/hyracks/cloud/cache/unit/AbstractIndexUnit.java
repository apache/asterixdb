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
package org.apache.hyracks.cloud.cache.unit;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.hyracks.storage.common.LocalResource;

public abstract class AbstractIndexUnit {
    protected final LocalResource localResource;
    private final AtomicLong lastAccessTime;
    private final AtomicInteger openCounter;

    AbstractIndexUnit(LocalResource localResource) {
        this.localResource = localResource;
        // System.nanoTime() has an arbitrary origin, so 0 would not read as long ago
        this.lastAccessTime = new AtomicLong(System.nanoTime());
        this.openCounter = new AtomicInteger(0);
    }

    public final void open() {
        lastAccessTime.set(System.nanoTime());
        openCounter.incrementAndGet();
    }

    public final void close() {
        lastAccessTime.set(System.nanoTime());
        openCounter.decrementAndGet();
    }

    /**
     * @return whether the index is in use now: a job has it open, as a feed may do for days
     */
    public final boolean isOpen() {
        return openCounter.get() > 0;
    }

    /**
     * @return the {@link System#nanoTime()} of the last time the index was registered, opened or closed
     */
    public final long getLastAccessTime() {
        return lastAccessTime.get();
    }

    /**
     * @param now       {@link System#nanoTime()}
     * @param threshold how long, in nanoseconds, an index must go unused to be inactive
     * @return whether the index is closed, and has been since {@code threshold} before {@code now}
     */
    public final boolean isInactive(long now, long threshold) {
        return !isOpen() && now - lastAccessTime.get() >= threshold;
    }

    public abstract void drop();

    @Override
    public String toString() {
        return "(id: " + localResource.getId() + ", path: " + localResource.getPath() + "sweepable: " + isSweepable()
                + ")";
    }

    protected abstract boolean isSweepable();
}
