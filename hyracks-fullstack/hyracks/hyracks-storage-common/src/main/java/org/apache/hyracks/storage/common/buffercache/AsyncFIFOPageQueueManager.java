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

public class AsyncFIFOPageQueueManager implements Runnable {
    private final static boolean DEBUG = false;

    protected LinkedBlockingQueue<ICachedPage> queue = new LinkedBlockingQueue<ICachedPage>();
    volatile Thread writerThread;
    protected AtomicBoolean poisoned = new AtomicBoolean(false);
    protected BufferCache bufferCache;
    volatile protected PageQueue pageQueue;

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

        @Override
        public void put(ICachedPage page) throws HyracksDataException {
            try {
                if (!poisoned.get()) {
                    queue.put(page);
                } else {
                    throw new HyracksDataException("Queue is closing");
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw HyracksDataException.create(e);
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

    @Override
    public void run() {
        if (DEBUG)
            System.out.println("[FIFO] Writer started");
        boolean die = false;
        while (!die) {
            ICachedPage entry;
            try {
                entry = queue.take();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
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

            if (DEBUG)
                System.out.println("[FIFO] Write " + BufferedFileHandle.getFileId(((CachedPage) entry).dpid) + ","
                        + BufferedFileHandle.getPageId(((CachedPage) entry).dpid));

            try {
                pageQueue.getWriter().write(entry, bufferCache);
            } catch (HyracksDataException e) {
                //TODO: What do we do, if we could not write the page?
                e.printStackTrace();
            }
        }
    }
}
