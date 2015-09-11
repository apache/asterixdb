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
    private int clockPtr;
    private ICacheMemoryAllocator allocator;
    private AtomicInteger numPages = new AtomicInteger(0);
    private final int pageSize;
    private final int maxAllowedNumPages;

    public ClockPageReplacementStrategy(ICacheMemoryAllocator allocator, int pageSize, int maxAllowedNumPages) {
        this.allocator = allocator;
        this.pageSize = pageSize;
        this.maxAllowedNumPages = maxAllowedNumPages;
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
        ICachedPageInternal cachedPage = null;
        int pageCount = getNumPages();
        // pageCount is a lower-bound of numPages.
        if (pageCount >= maxAllowedNumPages) {
            cachedPage = findVictimByEviction();
        } else {
            cachedPage = allocatePage();
        }
        return cachedPage;
    }

    private ICachedPageInternal findVictimByEviction() {
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
            /**
             * The clockPtr may miss the last added pages in this round.
             */
            clockPtr = (clockPtr + 1) % getNumPages();
            if (clockPtr == startClockPtr) {
                ++cycleCount;
            }
        } while (cycleCount < MAX_UNSUCCESSFUL_CYCLE_COUNT);
        return null;
    }

    /**
     * The number returned here could only be smaller or equal to the actual number
     * of pages, because numPages is monotonically incremented.
     */
    @Override
    public int getNumPages() {
        return numPages.get();
    }

    private ICachedPageInternal allocatePage() {
        CachedPage cPage = null;
        synchronized (this) {
            cPage = new CachedPage(numPages.get(), allocator.allocate(pageSize, 1)[0], this);
            bufferCache.addPage(cPage);
            numPages.incrementAndGet();
        }
        AtomicBoolean accessedFlag = getPerPageObject(cPage);
        if (!accessedFlag.compareAndSet(true, false)) {
            if (cPage.pinIfGoodVictim()) {
                return cPage;
            }
        }
        return null;
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
}