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
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class ClockPageReplacementStrategy implements IPageReplacementStrategy {
    private static final Logger LOGGER = LogManager.getLogger();
    private static final int MAX_UNSUCCESSFUL_CYCLE_COUNT = 3;

    private IBufferCacheInternal bufferCache;
    private AtomicInteger clockPtr;
    private ICacheMemoryAllocator allocator;
    private AtomicInteger numPages;
    private AtomicInteger cpIdCounter;
    private final int pageSize;
    private final int maxAllowedNumPages;
    private final ConcurrentLinkedQueue<Integer> cpIdFreeList;

    public ClockPageReplacementStrategy(ICacheMemoryAllocator allocator, int pageSize, int maxAllowedNumPages) {
        this.allocator = allocator;
        this.pageSize = pageSize;
        this.maxAllowedNumPages = maxAllowedNumPages;
        this.clockPtr = new AtomicInteger(0);
        this.numPages = new AtomicInteger(0);
        this.cpIdCounter = new AtomicInteger(0);
        cpIdFreeList = new ConcurrentLinkedQueue<>();
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
    public IBufferCacheInternal getBufferCache() {
        return bufferCache;
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
        return findVictim(1);
    }

    @Override
    public ICachedPageInternal findVictim(int multiplier) {
        while (numPages.get() + multiplier > maxAllowedNumPages) {
            // TODO: is dropping pages on the floor enough to adhere to memory budget?
            ICachedPageInternal victim = findVictimByEviction();
            if (victim == null) {
                return null;
            }
            int multiple = victim.getFrameSizeMultiplier();
            if (multiple == multiplier) {
                return victim;
            } else if (bufferCache.removePage(victim)) {
                cpIdFreeList.add(victim.getCachedPageId());
                numPages.getAndAdd(-multiple);
            }
        }
        return allocatePage(multiplier);
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
            if (cPage != null) {
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
            }
            if (clockPtr < lastClockPtr) {
                looped = true;
            }
            if (looped && clockPtr >= startClockPtr) {
                cycleCount++;
                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug("completed " + cycleCount + "/" + MAX_UNSUCCESSFUL_CYCLE_COUNT
                            + " clock cycle(s) without finding victim");
                }
                if (cycleCount >= MAX_UNSUCCESSFUL_CYCLE_COUNT) {
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

    private ICachedPageInternal allocatePage(int multiplier) {
        Integer cpId = cpIdFreeList.poll();
        if (cpId == null) {
            cpId = cpIdCounter.getAndIncrement();
        }
        CachedPage cPage = new CachedPage(cpId, allocator.allocate(pageSize * multiplier, 1)[0], this);
        cPage.setFrameSizeMultiplier(multiplier);
        bufferCache.addPage(cPage);
        numPages.getAndAdd(multiplier);
        return cPage;
    }

    @Override
    public void resizePage(ICachedPageInternal cPage, int multiplier, IExtraPageBlockHelper extraPageBlockHelper)
            throws HyracksDataException {
        int origMultiplier = cPage.getFrameSizeMultiplier();
        if (origMultiplier == multiplier) {
            // no-op
            return;
        }
        final int newSize = pageSize * multiplier;
        ByteBuffer oldBuffer = ((CachedPage) cPage).buffer;
        oldBuffer.position(0);
        final int delta = multiplier - origMultiplier;
        if (multiplier < origMultiplier) {
            oldBuffer.limit(newSize);
            final int gap = -delta;
            // we return the unused portion of our block to the page manager
            extraPageBlockHelper.returnFreePageBlock(cPage.getExtraBlockPageId() + gap, gap);
        } else {
            ensureBudgetForLargePages(delta);
            if (origMultiplier != 1) {
                // return the old block to the page manager
                extraPageBlockHelper.returnFreePageBlock(cPage.getExtraBlockPageId(), origMultiplier);
            }
            cPage.setExtraBlockPageId(extraPageBlockHelper.getFreeBlock(multiplier));
        }
        cPage.setFrameSizeMultiplier(multiplier);
        ByteBuffer newBuffer = allocator.allocate(newSize, 1)[0];
        newBuffer.put(oldBuffer);
        numPages.getAndAdd(delta);
        ((CachedPage) cPage).buffer = newBuffer;
    }

    @Override
    public void fixupCapacityOnLargeRead(ICachedPageInternal cPage) throws HyracksDataException {
        ByteBuffer oldBuffer = ((CachedPage) cPage).buffer;
        final int multiplier = cPage.getFrameSizeMultiplier();
        final int newSize = pageSize * multiplier;
        final int delta = multiplier - 1;
        oldBuffer.position(0);
        ensureBudgetForLargePages(delta);
        ByteBuffer newBuffer = allocator.allocate(newSize, 1)[0];
        newBuffer.put(oldBuffer);
        numPages.getAndAdd(delta);
        ((CachedPage) cPage).buffer = newBuffer;
    }

    private void ensureBudgetForLargePages(int delta) {
        while (numPages.get() + delta > maxAllowedNumPages) {
            ICachedPageInternal victim = findVictimByEviction();
            if (victim != null) {
                final int victimMultiplier = victim.getFrameSizeMultiplier();
                if (bufferCache.removePage(victim)) {
                    cpIdFreeList.add(victim.getCachedPageId());
                    numPages.getAndAdd(-victimMultiplier);
                }
            } else {
                // we don't have the budget to resize- proceed anyway, but log
                if (LOGGER.isWarnEnabled()) {
                    LOGGER.warn("Exceeding buffer cache budget of " + maxAllowedNumPages + " by "
                            + (numPages.get() + delta - maxAllowedNumPages)
                            + " pages in order to satisfy large page read");
                }
                break;

            }
        }
    }

    //derived from RoundRobinAllocationPolicy in Apache directmemory
    private int advanceClock() {

        boolean clockInDial;
        int currClockPtr;
        do {
            currClockPtr = clockPtr.get();
            int newClockPtr = (currClockPtr + 1) % cpIdCounter.get();
            clockInDial = clockPtr.compareAndSet(currClockPtr, newClockPtr);
        } while (!clockInDial);
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
