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
        ICachedPageInternal cachedPage;
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
        int clockPtr = advanceClock();
        int startClockPtr = clockPtr;
        int lastClockPtr = -1;
        int cycleCount = 0;
        boolean looped = false;
        while (true) {
            ICachedPageInternal cPage = bufferCache.getPage(clockPtr);

            /*
             * We do two things here:
             * 1. If the page has been accessed, then we skip it -- The CAS would return
             * false if the current value is false which makes the page a possible candidate
             * for replacement.
             * 2. We check with the buffer manager if it feels it's a good idea to use this
             * page as a victim.
             */
            AtomicBoolean accessedFlag = getPerPageObject(cPage);
            if (!accessedFlag.compareAndSet(true, false)) {
                if (cPage.isGoodVictim()) {
                    return cPage;
                }
            }
            if (clockPtr < lastClockPtr) {
                looped = true;
            }
            if (looped && clockPtr >= startClockPtr) {
                if (++cycleCount >= MAX_UNSUCCESSFUL_CYCLE_COUNT) {
                    return null;
                }
                looped = false;
            }
            lastClockPtr = clockPtr;
            clockPtr = advanceClock();
        }
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
            if (cPage.isGoodVictim()) {
                return cPage;
            }
        }
        return null;
    }

    //derived from RoundRobinAllocationPolicy in Apache directmemory
    private int advanceClock() {

        boolean clockInDial;
        int currClockPtr;
        do
        {
            currClockPtr = clockPtr.get();
            int newClockPtr = ( currClockPtr + 1 ) % numPages.get();
            clockInDial = clockPtr.compareAndSet( currClockPtr, newClockPtr );
        }
        while ( !clockInDial );
        return currClockPtr;

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
