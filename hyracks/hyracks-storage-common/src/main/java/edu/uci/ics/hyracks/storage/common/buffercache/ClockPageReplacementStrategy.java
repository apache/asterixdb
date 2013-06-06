/*
 * Copyright 2009-2013 by The Regents of the University of California
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * you may obtain a copy of the License from
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.uci.ics.hyracks.storage.common.buffercache;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class ClockPageReplacementStrategy implements IPageReplacementStrategy {
    private static final int MAX_UNSUCCESSFUL_CYCLE_COUNT = 3;

    private final Lock lock;
    private IBufferCacheInternal bufferCache;
    private int clockPtr;

    public ClockPageReplacementStrategy() {
        this.lock = new ReentrantLock();
        clockPtr = 0;
    }

    @Override
    public Object createPerPageStrategyObject(int cpid) {
        return new AtomicBoolean();
    }

    @Override
    public void setBufferCache(IBufferCacheInternal bufferCache) {
        this.bufferCache = bufferCache;
    }

    @Override
    public void notifyCachePageReset(ICachedPageInternal cPage) {
        getPerPageObject(cPage).set(false);
    }

    @Override
    public void notifyCachePageAccess(ICachedPageInternal cPage) {
        getPerPageObject(cPage).set(true);
    }

    @Override
    public ICachedPageInternal findVictim() {
        lock.lock();
        try {
            int startClockPtr = clockPtr;
            int cycleCount = 0;
            do {
                ICachedPageInternal cPage = bufferCache.getPage(clockPtr);

                /*
                 * We do two things here:
                 * 1. If the page has been accessed, then we skip it -- The CAS would return
                 * false if the current value is false which makes the page a possible candidate
                 * for replacement.
                 * 2. We check with the buffer manager if it feels its a good idea to use this
                 * page as a victim.
                 */
                AtomicBoolean accessedFlag = getPerPageObject(cPage);
                if (!accessedFlag.compareAndSet(true, false)) {
                    if (cPage.pinIfGoodVictim()) {
                        return cPage;
                    }
                }
                clockPtr = (clockPtr + 1) % bufferCache.getNumPages();
                if (clockPtr == startClockPtr) {
                    ++cycleCount;
                }
            } while (cycleCount < MAX_UNSUCCESSFUL_CYCLE_COUNT);
        } finally {
            lock.unlock();
        }
        return null;
    }

    private AtomicBoolean getPerPageObject(ICachedPageInternal cPage) {
        return (AtomicBoolean) cPage.getReplacementStrategyObject();
    }
}