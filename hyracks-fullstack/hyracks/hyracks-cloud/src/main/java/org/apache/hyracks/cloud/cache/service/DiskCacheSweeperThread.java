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
package org.apache.hyracks.cloud.cache.service;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.io.IDiskSpaceMaker;
import org.apache.hyracks.cloud.cache.unit.SweepableIndexUnit;
import org.apache.hyracks.cloud.cache.unit.UnsweepableIndexUnit;
import org.apache.hyracks.cloud.io.ICloudIOManager;
import org.apache.hyracks.cloud.sweeper.Sweeper;
import org.apache.hyracks.storage.common.LocalResource;
import org.apache.hyracks.storage.common.buffercache.BufferCache;
import org.apache.hyracks.storage.common.disk.IPhysicalDrive;
import org.apache.hyracks.storage.common.file.BufferedFileHandle;
import org.apache.hyracks.util.annotations.CriticalPath;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class DiskCacheSweeperThread implements Runnable, IDiskSpaceMaker {
    private static final Logger LOGGER = LogManager.getLogger();
    private final long waitTime;
    private final CloudDiskResourceCacheLockNotifier resourceManager;
    private final IPhysicalDrive physicalDrive;
    private final List<SweepableIndexUnit> indexes;
    private final ICloudIOManager cloudIOManager;
    private final Sweeper sweeper;
    private final long inactiveTimeThreshold;
    private final AtomicBoolean paused;

    public DiskCacheSweeperThread(ExecutorService executorService, long waitTime,
            CloudDiskResourceCacheLockNotifier resourceManager, ICloudIOManager cloudIOManager, int numOfSweepThreads,
            int sweepQueueSize, IPhysicalDrive physicalDrive, BufferCache bufferCache,
            Map<Integer, BufferedFileHandle> fileInfoMap, long inactiveTimeThreshold) {
        this.waitTime = TimeUnit.SECONDS.toMillis(waitTime);
        this.resourceManager = resourceManager;
        this.physicalDrive = physicalDrive;
        this.inactiveTimeThreshold = inactiveTimeThreshold;
        indexes = new ArrayList<>();
        this.cloudIOManager = cloudIOManager;
        paused = new AtomicBoolean();
        sweeper = new Sweeper(paused, executorService, cloudIOManager, bufferCache, fileInfoMap, numOfSweepThreads,
                sweepQueueSize);
    }

    public void pause() {
        LOGGER.info("Pausing Sweeper threads...");
        // Synchronize to ensure the thread is not sweeping
        synchronized (this) {
            // Set paused to ignore all future sweep requests
            paused.set(true);
        }
        sweeper.waitForRunningRequests();
        LOGGER.info("Sweeper threads have been paused successfully");
    }

    public void reportLocalResources(Map<Long, LocalResource> localResources) {
        resourceManager.reportLocalResources(localResources);
    }

    public void resume() {
        paused.set(false);
    }

    @Override
    public void makeSpaceOrThrow(IOException ioException) throws HyracksDataException {
        if (ioException.getMessage().contains("no space")) {
            synchronized (this) {
                // Notify the sweeper thread
                notify();
                try {
                    // Wait for the sweep to finish
                    wait();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
        } else {
            throw HyracksDataException.create(ioException);
        }
    }

    @Override
    public void run() {
        Thread.currentThread().setName(this.getClass().getSimpleName());
        while (true) {
            synchronized (this) {
                try {
                    makeSpace();
                    wait(waitTime);
                } catch (InterruptedException e) {
                    LOGGER.warn("DiskCacheSweeperThread interrupted", e);
                    break;
                } finally {
                    notifyAll();
                }
            }
        }

        Thread.currentThread().interrupt();
    }

    private void makeSpace() {
        if (paused.get()) {
            LOGGER.warn("DiskCacheSweeperThread is paused");
            return;
        }

        if (physicalDrive.computeAndCheckIsPressured()) {
            boolean shouldSweep;
            resourceManager.getEvictionLock().writeLock().lock();
            try {
                shouldSweep = evictInactive();
            } finally {
                resourceManager.getEvictionLock().writeLock().unlock();
            }

            if (shouldSweep) {
                // index eviction didn't help. Sweep!
                sweep();
            }
        }
    }

    private boolean evictInactive() {
        long now = System.nanoTime();
        Collection<LocalResource> inactiveResources = resourceManager.getInactiveResources();
        Collection<UnsweepableIndexUnit> unsweepableIndexes = resourceManager.getUnsweepableIndexes();
        if (inactiveResources.isEmpty() && unsweepableIndexes.isEmpty()) {
            // return true to run sweep as nothing will be evicted
            return true;
        }

        // First evict all resources that were never been registered
        for (LocalResource resource : inactiveResources) {
            try {
                cloudIOManager.evict(resource.getPath());
            } catch (HyracksDataException e) {
                LOGGER.error("Failed to evict resource " + resource.getPath(), e);
            }
        }

        // Next evict all inactive indexes
        for (UnsweepableIndexUnit index : unsweepableIndexes) {
            if (now - index.getLastAccessTime() >= inactiveTimeThreshold) {
                try {
                    cloudIOManager.evict(index.getPath());
                } catch (HyracksDataException e) {
                    LOGGER.error("Failed to evict resource " + index.getPath(), e);
                }
            }
        }

        // If disk is still pressured, proceed with sweep
        return physicalDrive.computeAndCheckIsPressured();
    }

    private void sweep() {
        indexes.clear();
        resourceManager.getSweepableIndexes(indexes);
        sweepIndexes(sweeper, indexes);
    }

    @CriticalPath
    private static void sweepIndexes(Sweeper sweeper, List<SweepableIndexUnit> indexes) {
        for (int i = 0; i < indexes.size(); i++) {
            SweepableIndexUnit index = indexes.get(i);
            if (!index.isSweeping()) {
                try {
                    sweeper.sweep(index);
                } catch (InterruptedException e) {
                    LOGGER.warn("Sweeping thread interrupted", e);
                    Thread.currentThread().interrupt();
                }
            }
        }
    }
}
