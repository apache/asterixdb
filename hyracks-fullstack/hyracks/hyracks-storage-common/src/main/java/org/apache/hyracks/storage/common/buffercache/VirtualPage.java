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
package org.apache.hyracks.storage.common.buffercache;

import java.nio.ByteBuffer;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class VirtualPage implements ICachedPage {
    private final ReentrantReadWriteLock latch;
    private final int pageSize;
    private ByteBuffer buffer;
    private volatile long dpid;
    private int multiplier;
    private VirtualPage next;

    public VirtualPage(ByteBuffer buffer, int pageSize) {
        this.buffer = buffer;
        this.pageSize = pageSize;
        latch = new ReentrantReadWriteLock(true);
        dpid = -1;
        next = null;
    }

    public void reset() {
        dpid = -1;
        next = null;
    }

    @Override
    public ByteBuffer getBuffer() {
        return buffer;
    }

    @Override
    public void acquireReadLatch() {
        latch.readLock().lock();
    }

    @Override
    public void releaseReadLatch() {
        latch.readLock().unlock();
    }

    @Override
    public void acquireWriteLatch() {
        latch.writeLock().lock();
    }

    @Override
    public void releaseWriteLatch(boolean markDirty) {
        latch.writeLock().unlock();
    }

    @Override
    public boolean confiscated() {
        return false;
    }

    @Override
    public int getPageSize() {
        return pageSize;
    }

    @Override
    public int getFrameSizeMultiplier() {
        return multiplier;
    }

    public void multiplier(int multiplier) {
        this.multiplier = multiplier;
    }

    public long dpid() {
        return dpid;
    }

    public void dpid(long dpid) {
        this.dpid = dpid;
    }

    public VirtualPage next() {
        return next;
    }

    public void next(VirtualPage next) {
        this.next = next;
    }

    public ByteBuffer buffer() {
        return buffer;
    }

    public void buffer(ByteBuffer buffer) {
        this.buffer = buffer;
    }

    @Override
    public void setDiskPageId(long dpid) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isLargePage() {
        return multiplier > 1;
    }

    public int getReadLatchCount() {
        return latch.getReadLockCount();
    }

    public boolean isWriteLatched() {
        return latch.isWriteLocked();
    }

    @Override
    public String toString() {
        StringBuilder str = new StringBuilder();
        str.append("{\"class\":\"").append(getClass().getSimpleName()).append("\", \"readers\":")
                .append(getReadLatchCount()).append(",\"writers\":").append(isWriteLatched());
        str.append(",\"next\":").append(next);
        str.append("}");
        return str.toString();
    }
}
