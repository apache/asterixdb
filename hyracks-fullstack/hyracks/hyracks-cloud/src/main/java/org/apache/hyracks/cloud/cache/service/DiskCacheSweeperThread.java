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
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.io.IDiskSpaceMaker;
import org.apache.hyracks.cloud.cache.unit.DatasetUnit;
import org.apache.hyracks.cloud.cache.unit.IndexUnit;
import org.apache.hyracks.cloud.io.ICloudIOManager;
import org.apache.hyracks.cloud.sweeper.ISweeper;
import org.apache.hyracks.cloud.sweeper.Sweeper;
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
    private final List<IndexUnit> indexes;
    private final ISweeper sweeper;

    public DiskCacheSweeperThread(ExecutorService executorService, long waitTime,
            CloudDiskResourceCacheLockNotifier resourceManager, ICloudIOManager cloudIOManager, int numOfSweepThreads,
            int sweepQueueSize, IPhysicalDrive physicalDrive, BufferCache bufferCache,
            Map<Integer, BufferedFileHandle> fileInfoMap) {
        this.waitTime = TimeUnit.SECONDS.toMillis(waitTime);
        this.resourceManager = resourceManager;
        this.physicalDrive = physicalDrive;
        indexes = new ArrayList<>();
        sweeper = new Sweeper(executorService, cloudIOManager, bufferCache, fileInfoMap, numOfSweepThreads,
                sweepQueueSize);
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
                    sweep();
                    wait(waitTime);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } finally {
                    notifyAll();
                }
            }
        }
    }

    private void sweep() {
        if (physicalDrive.computeAndCheckIsPressured()) {
            for (DatasetUnit dataset : resourceManager.getDatasets().values()) {
                indexes.clear();
                dataset.getIndexes(indexes);
                sweepIndexes(sweeper, indexes);
            }
        }
    }

    @CriticalPath
    private static void sweepIndexes(ISweeper sweeper, List<IndexUnit> indexes) {
        for (int i = 0; i < indexes.size(); i++) {
            IndexUnit index = indexes.get(i);
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
