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

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class ClockPageReplacementStrategy implements IPageReplacementStrategy {
    private static final int MAX_UNSUCCESSFUL_CYCLE_COUNT = 3;

    private IBufferCacheInternal bufferCache;
    private AtomicInteger clockPtr;
    private ICacheMemoryAllocator allocator;
    private AtomicInteger numPages;
    private AtomicInteger cpIdCounter;
    private final int pageSize;
    private final int maxAllowedNumPages;

    public ClockPageReplacementStrategy(ICacheMemoryAllocator allocator, int pageSize, int maxAllowedNumPages) {
        this.allocator = allocator;
        this.pageSize = pageSize;
        this.maxAllowedNumPages = maxAllowedNumPages;
        this.clockPtr = new AtomicInteger(0);
        this.numPages = new AtomicInteger(0);
        this.cpIdCounter = new AtomicInteger(0);
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
        ICachedPageInternal cachedPage = null;
        if (numPages.get() >= maxAllowedNumPages) {
            cachedPage = findVictimByEviction();
        } else {
            cachedPage = allocatePage();
        }
        return cachedPage;
    }

    private ICachedPageInternal findVictimByEviction() {
        //check if we're starved from confiscation
        assert (maxAllowedNumPages > 0);
        int startClockPtr = clockPtr.get();
        int cycleCount = 0;
        do {
            ICachedPageInternal cPage = bufferCache.getPage(clockPtr.get());

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
            advanceClock();
            if (clockPtr.get() == startClockPtr) {
                ++cycleCount;
            }
        } while (cycleCount < MAX_UNSUCCESSFUL_CYCLE_COUNT);
        return null;
    }

    @Override
    public int getNumPages() {
        return numPages.get();
    }

    private ICachedPageInternal allocatePage() {
        CachedPage cPage = new CachedPage(cpIdCounter.getAndIncrement(), allocator.allocate(pageSize, 1)[0], this);
        bufferCache.addPage(cPage);
        numPages.incrementAndGet();
        AtomicBoolean accessedFlag = getPerPageObject(cPage);
        if (!accessedFlag.compareAndSet(true, false)) {
            if (cPage.pinIfGoodVictim()) {
                return cPage;
            }
        }
        return null;
    }

    //derived from RoundRobinAllocationPolicy in Apache directmemory
    private int advanceClock(){
        boolean clockInDial = false;
        int newClockPtr = 0;
        do
        {
            int currClockPtr = clockPtr.get();
            newClockPtr = ( currClockPtr + 1 ) % numPages.get();
            clockInDial = clockPtr.compareAndSet( currClockPtr, newClockPtr );
        }
        while ( !clockInDial );
        return newClockPtr;

    }

    private AtomicBoolean getPerPageObject(ICachedPageInternal cPage) {
        return (AtomicBoolean) cPage.getReplacementStrategyObject();
    }

    @Override
    public int getPageSize() {
        return pageSize;
    }

    @Override
    public int getMaxAllowedNumPages() {
        return maxAllowedNumPages;
    }

    @Override
    public void adviseWontNeed(ICachedPageInternal cPage) {
        //make the page appear as if it wasn't accessed even if it was
        getPerPageObject(cPage).set(false);
    }
}
