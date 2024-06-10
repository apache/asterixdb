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
package org.apache.hyracks.cloud.sweeper;

import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.util.InvokeUtil;
import org.apache.hyracks.cloud.cache.unit.SweepableIndexUnit;
import org.apache.hyracks.cloud.io.ICloudIOManager;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIndex;
import org.apache.hyracks.storage.am.lsm.common.cloud.IIndexDiskCacheManager;
import org.apache.hyracks.storage.common.buffercache.BufferCache;
import org.apache.hyracks.storage.common.file.BufferedFileHandle;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public final class Sweeper {
    private static final Logger LOGGER = LogManager.getLogger();
    private final int queueSize;
    private final BlockingQueue<SweepRequest> requests;
    private final BlockingQueue<SweepRequest> freeRequests;

    public Sweeper(AtomicBoolean paused, ExecutorService executor, ICloudIOManager cloudIOManager,
            BufferCache bufferCache, Map<Integer, BufferedFileHandle> fileInfoMap, int numOfSweepThreads,
            int queueSize) {
        this.queueSize = queueSize;
        requests = new ArrayBlockingQueue<>(queueSize);
        freeRequests = new ArrayBlockingQueue<>(queueSize);
        for (int i = 0; i < queueSize; i++) {
            SweepContext context = new SweepContext(cloudIOManager, bufferCache, fileInfoMap, paused);
            freeRequests.add(new SweepRequest(context));
        }
        for (int i = 0; i < numOfSweepThreads; i++) {
            executor.execute(new SweepThread(requests, freeRequests, i));
        }
    }

    public void sweep(SweepableIndexUnit indexUnit) throws InterruptedException {
        SweepRequest request = freeRequests.take();
        request.reset(indexUnit);
        requests.put(request);
    }

    public void waitForRunningRequests() {
        // Wait for all running requests if any
        while (freeRequests.size() != queueSize) {
            synchronized (freeRequests) {
                LOGGER.warn("Waiting for {} running sweep requests", queueSize - freeRequests.size());
                InvokeUtil.doUninterruptibly(freeRequests::wait);
            }
        }
    }

    private static class SweepThread implements Runnable {
        private final BlockingQueue<SweepRequest> requests;
        private final BlockingQueue<SweepRequest> freeRequests;
        private final int threadNumber;

        private SweepThread(BlockingQueue<SweepRequest> requests, BlockingQueue<SweepRequest> freeRequests,
                int threadNumber) {
            this.requests = requests;
            this.freeRequests = freeRequests;
            this.threadNumber = threadNumber;
        }

        @Override
        public void run() {
            Thread.currentThread().setName(getClass().getSimpleName() + "-" + threadNumber);
            while (true) {
                SweepRequest request = null;
                try {
                    request = requests.take();
                    request.handle();
                } catch (InterruptedException e) {
                    LOGGER.warn("Sweep thread interrupted");
                    break;
                } catch (Throwable t) {
                    LOGGER.error("Sweep failed", t);
                } finally {
                    if (request != null) {
                        freeRequests.add(request);
                    }

                    synchronized (freeRequests) {
                        // Notify if pause() is waiting for all requests to finish
                        freeRequests.notify();
                    }
                }
            }

            Thread.currentThread().interrupt();
        }
    }

    private static class SweepRequest {
        private final SweepContext context;

        SweepRequest(SweepContext context) {
            this.context = context;
        }

        void reset(SweepableIndexUnit indexUnit) {
            context.setIndexUnit(indexUnit);
        }

        void handle() throws HyracksDataException {
            if (context.stopSweeping()) {
                /*
                 * This could happen as the sweeper gets a copy of a list of all indexUnits at a certain point
                 * of time. However, after acquiring the list and the start of handling this sweep request, we found
                 * that the index of this request was dropped.
                 *
                 * To illustrate:
                 * 1- The Sweeper got a list of all indexUnits (say index_1 and index_2)
                 * 2a- The Sweeper started sweeping index_1
                 * 2b- index_2 was dropped
                 * 3- The sweeper finished sweeping index_1 and started sweeping index_2. However, index_2 was
                 * dropped at 2b.
                 */
                return;
            }

            SweepableIndexUnit indexUnit = context.getIndexUnit();
            indexUnit.startSweeping();
            try {
                ILSMIndex index = indexUnit.getIndex();
                IIndexDiskCacheManager diskCacheManager = index.getDiskCacheManager();
                if (!diskCacheManager.isActive()) {
                    return;
                }

                if (diskCacheManager.prepareSweepPlan()) {
                    // The Index sweep planner determined that a sweep can be performed. Sweep.
                    diskCacheManager.sweep(context);
                }
            } finally {
                indexUnit.finishedSweeping();
            }
        }
    }
}
