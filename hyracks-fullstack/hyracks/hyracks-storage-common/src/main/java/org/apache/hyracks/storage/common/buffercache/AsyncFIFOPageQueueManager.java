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

import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.storage.common.file.BufferedFileHandle;
import org.apache.hyracks.util.ExitUtil;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class AsyncFIFOPageQueueManager implements Runnable {
    private static final boolean DEBUG = false;
    private static final Logger LOGGER = LogManager.getLogger();

    protected LinkedBlockingQueue<ICachedPage> queue = new LinkedBlockingQueue<>();
    volatile Thread writerThread;
    protected AtomicBoolean poisoned = new AtomicBoolean(false);
    protected BufferCache bufferCache;
    protected volatile PageQueue pageQueue;

    public AsyncFIFOPageQueueManager(BufferCache bufferCache) {
        this.bufferCache = bufferCache;
    }

    protected class PageQueue implements IFIFOPageQueue {
        final IBufferCache bufferCache;
        public final IFIFOPageWriter writer;

        protected PageQueue(IBufferCache bufferCache, IFIFOPageWriter writer) {
            if (DEBUG)
                System.out.println("[FIFO] New Queue");
            this.bufferCache = bufferCache;
            this.writer = writer;
        }

        protected IBufferCache getBufferCache() {
            return bufferCache;
        }

        protected IFIFOPageWriter getWriter() {
            return writer;
        }

        @SuppressWarnings("squid:S2142")
        @Override
        public void put(ICachedPage page, IPageWriteFailureCallback callback) throws HyracksDataException {
            failIfPreviousPageFailed(callback);
            page.setFailureCallback(callback);
            try {
                if (!poisoned.get()) {
                    queue.put(page);
                } else {
                    LOGGER.error("An attempt to write a page found buffer cache closed");
                    ExitUtil.halt(ExitUtil.EC_ABNORMAL_TERMINATION);
                }
            } catch (InterruptedException e) {
                LOGGER.error("IO Operation interrupted", e);
                ExitUtil.halt(ExitUtil.EC_ABNORMAL_TERMINATION);
            }
        }

        private void failIfPreviousPageFailed(IPageWriteFailureCallback callback) throws HyracksDataException {
            if (callback.hasFailed()) {
                throw HyracksDataException.create(callback.getFailure());
            }
        }
    }

    public PageQueue createQueue(IFIFOPageWriter writer) {
        if (pageQueue == null) {
            synchronized (this) {
                if (pageQueue == null) {
                    writerThread = new Thread(this);
                    writerThread.setName("FIFO Writer Thread");
                    writerThread.start();
                    pageQueue = new PageQueue(bufferCache, writer);
                }
            }
        }
        return pageQueue;
    }

    public void destroyQueue() {
        poisoned.set(true);
        if (writerThread == null) {
            synchronized (this) {
                if (writerThread == null) {
                    return;
                }
            }
        }

        //Dummy cached page to act as poison pill
        CachedPage poisonPill = new CachedPage();
        poisonPill.setQueueInfo(new QueueInfo(true, true));

        try {
            synchronized (poisonPill) {
                queue.put(poisonPill);
                while (queue.contains(poisonPill)) {
                    poisonPill.wait();
                }
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    public void finishQueue() throws HyracksDataException {
        if (writerThread == null) {
            synchronized (this) {
                if (writerThread == null) {
                    return;
                }
            }
        }
        try {
            //Dummy cached page to act as low water mark
            CachedPage lowWater = new CachedPage();
            lowWater.setQueueInfo(new QueueInfo(true, false));
            synchronized (lowWater) {
                queue.put(lowWater);
                while (queue.contains(lowWater)) {
                    lowWater.wait();
                }
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw HyracksDataException.create(e);
        }
    }

    @SuppressWarnings("squid:S2142")
    @Override
    public void run() {
        if (DEBUG) {
            LOGGER.info("[FIFO] Writer started");
        }
        boolean die = false;
        while (!die) {
            ICachedPage entry;
            try {
                entry = queue.take();
            } catch (InterruptedException e) {
                LOGGER.error("BufferCache Write Queue was interrupted", e);
                ExitUtil.halt(ExitUtil.EC_ABNORMAL_TERMINATION);
                return; // Keep compiler happy
            }
            if (entry.getQueueInfo() != null && entry.getQueueInfo().hasWaiters()) {
                synchronized (entry) {
                    if (entry.getQueueInfo().isPoison()) {
                        die = true;
                    }
                    entry.notifyAll();
                    continue;
                }
            }
            if (DEBUG) {
                LOGGER.info("[FIFO] Write " + BufferedFileHandle.getFileId(((CachedPage) entry).dpid) + ","
                        + BufferedFileHandle.getPageId(((CachedPage) entry).dpid));
            }
            pageQueue.getWriter().write(entry, bufferCache);
        }
    }
}
