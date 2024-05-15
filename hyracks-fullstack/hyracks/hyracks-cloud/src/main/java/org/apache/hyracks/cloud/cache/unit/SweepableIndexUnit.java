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

import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.hyracks.api.util.InvokeUtil;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIndex;
import org.apache.hyracks.storage.common.LocalResource;

public final class SweepableIndexUnit extends AbstractIndexUnit {
    private final ILSMIndex index;
    private final AtomicBoolean dropped;
    private final AtomicBoolean sweeping;

    public SweepableIndexUnit(LocalResource localResource, ILSMIndex index) {
        super(localResource);
        this.index = index;
        dropped = new AtomicBoolean(false);
        sweeping = new AtomicBoolean(false);
    }

    @Override
    public void drop() {
        // Signal that the index is being dropped so a sweeper thread does not sweep this index or stops sweeping
        dropped.set(false);
        // Wait for the sweep operation (if running) before allowing the index to be dropped
        waitForSweep();
    }

    @Override
    protected boolean isSweepable() {
        return true;
    }

    public ILSMIndex getIndex() {
        return index;
    }

    public boolean isDropped() {
        return dropped.get();
    }

    public boolean isSweeping() {
        return sweeping.get();
    }

    public void startSweeping() {
        sweeping.set(true);
    }

    public void waitForSweep() {
        synchronized (sweeping) {
            while (sweeping.get()) {
                // This should not be interrupted until we get a notification the sweep is done
                InvokeUtil.doUninterruptibly(sweeping::wait);
            }
        }
    }

    public void finishedSweeping() {
        sweeping.set(false);
        synchronized (sweeping) {
            sweeping.notifyAll();
        }
    }
}
