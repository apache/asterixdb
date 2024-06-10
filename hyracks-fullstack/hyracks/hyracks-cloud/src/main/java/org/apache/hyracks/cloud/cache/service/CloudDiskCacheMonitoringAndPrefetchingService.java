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

import java.util.Map;
import java.util.concurrent.ExecutorService;

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.storage.common.LocalResource;
import org.apache.hyracks.storage.common.disk.IDiskCacheMonitoringService;
import org.apache.hyracks.storage.common.disk.IPhysicalDrive;
import org.apache.hyracks.storage.common.disk.prefetch.AbstractPrefetchRequest;
import org.apache.hyracks.storage.common.disk.prefetch.IPrefetchHandler;

public final class CloudDiskCacheMonitoringAndPrefetchingService
        implements IDiskCacheMonitoringService, IPrefetchHandler {
    private final ExecutorService executor;
    private final DiskCacheSweeperThread monitorThread;
    private final IPhysicalDrive drive;

    public CloudDiskCacheMonitoringAndPrefetchingService(ExecutorService executor, IPhysicalDrive drive,
            DiskCacheSweeperThread monitorThread) {
        this.executor = executor;
        this.drive = drive;
        this.monitorThread = monitorThread;
    }

    @Override
    public void start() {
        executor.execute(monitorThread);
    }

    @Override
    public void stop() {
        monitorThread.pause();
        executor.shutdown();
    }

    @Override
    public void pause() {
        monitorThread.pause();
    }

    @Override
    public void resume() {
        monitorThread.resume();
    }

    @Override
    public boolean isEnabled() {
        return true;
    }

    @Override
    public void reportLocalResources(Map<Long, LocalResource> localResources) {
        monitorThread.reportLocalResources(localResources);
    }

    @Override
    public IPhysicalDrive getPhysicalDrive() {
        return drive;
    }

    @Override
    public void request(AbstractPrefetchRequest request) throws HyracksDataException {
        // TODO implement
    }
}
