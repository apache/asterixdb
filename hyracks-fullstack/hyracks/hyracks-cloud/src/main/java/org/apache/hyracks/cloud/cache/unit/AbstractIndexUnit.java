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
        this.lastAccessTime = new AtomicLong(0);
        this.openCounter = new AtomicInteger(0);
    }

    public final void open() {
        lastAccessTime.set(System.nanoTime());
        openCounter.get();
    }

    public final void close() {
        openCounter.decrementAndGet();
    }

    public final long getLastAccessTime() {
        return lastAccessTime.get();
    }

    public abstract void drop();

    @Override
    public String toString() {
        return "(id: " + localResource.getId() + ", path: " + localResource.getPath() + "sweepable: " + isSweepable()
                + ")";
    }

    protected abstract boolean isSweepable();
}
