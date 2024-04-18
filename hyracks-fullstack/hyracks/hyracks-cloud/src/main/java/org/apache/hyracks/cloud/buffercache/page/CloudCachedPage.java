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
package org.apache.hyracks.cloud.buffercache.page;

import java.nio.ByteBuffer;

import org.apache.hyracks.storage.common.buffercache.CachedPage;
import org.apache.hyracks.storage.common.buffercache.IPageReplacementStrategy;

public final class CloudCachedPage extends CachedPage {
    private ISweepLockInfo lockInfo;

    private CloudCachedPage() {
        // Disable default constructor
    }

    CloudCachedPage(int cpid, ByteBuffer buffer, IPageReplacementStrategy replacementStrategy) {
        super(cpid, buffer, replacementStrategy);
        lockInfo = UnlockedSweepLockInfo.INSTANCE;
    }

    public ISweepLockInfo beforeRead() {
        latch.readLock().lock();
        return lockInfo;
    }

    public void afterRead() {
        latch.readLock().unlock();
    }

    public boolean trySweepLock(ISweepLockInfo lockInfo) {
        if (!latch.writeLock().tryLock()) {
            return false;
        }

        try {
            this.lockInfo = lockInfo;
        } finally {
            latch.writeLock().unlock();
        }

        return true;
    }

    public void sweepUnlock() {
        latch.writeLock().lock();
        try {
            this.lockInfo = UnlockedSweepLockInfo.INSTANCE;
        } finally {
            latch.writeLock().unlock();
        }
    }

    public boolean skipCloudStream() {
        return valid;
    }

}
