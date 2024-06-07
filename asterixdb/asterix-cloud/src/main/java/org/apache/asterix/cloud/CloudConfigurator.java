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
package org.apache.asterix.cloud;

import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.asterix.cloud.clients.ICloudGuardian;
import org.apache.asterix.common.api.INamespacePathResolver;
import org.apache.asterix.common.cloud.CloudCachePolicy;
import org.apache.asterix.common.cloud.IPartitionBootstrapper;
import org.apache.asterix.common.config.CloudProperties;
import org.apache.asterix.common.utils.StorageConstants;
import org.apache.asterix.common.utils.StoragePathUtil;
import org.apache.hyracks.api.application.INCServiceContext;
import org.apache.hyracks.api.config.IApplicationConfig;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.io.IIOManager;
import org.apache.hyracks.cloud.buffercache.context.DefaultCloudReadContext;
import org.apache.hyracks.cloud.buffercache.page.CloudDiskCachedPageAllocator;
import org.apache.hyracks.cloud.cache.service.CloudDiskCacheMonitoringAndPrefetchingService;
import org.apache.hyracks.cloud.cache.service.CloudDiskResourceCacheLockNotifier;
import org.apache.hyracks.cloud.cache.service.DiskCacheSweeperThread;
import org.apache.hyracks.cloud.cache.service.IEvictableLocalResourceFilter;
import org.apache.hyracks.cloud.filesystem.PhysicalDrive;
import org.apache.hyracks.control.common.controllers.NCConfig;
import org.apache.hyracks.control.nc.io.IOManager;
import org.apache.hyracks.storage.common.buffercache.BufferCache;
import org.apache.hyracks.storage.common.buffercache.DefaultDiskCachedPageAllocator;
import org.apache.hyracks.storage.common.buffercache.IBufferCache;
import org.apache.hyracks.storage.common.buffercache.IDiskCachedPageAllocator;
import org.apache.hyracks.storage.common.buffercache.context.IBufferCacheReadContext;
import org.apache.hyracks.storage.common.buffercache.context.read.DefaultBufferCacheReadContextProvider;
import org.apache.hyracks.storage.common.disk.DummyPhysicalDrive;
import org.apache.hyracks.storage.common.disk.IDiskCacheMonitoringService;
import org.apache.hyracks.storage.common.disk.IDiskResourceCacheLockNotifier;
import org.apache.hyracks.storage.common.disk.IPhysicalDrive;
import org.apache.hyracks.storage.common.disk.NoOpDiskCacheMonitoringService;
import org.apache.hyracks.storage.common.disk.NoOpDiskResourceCacheLockNotifier;
import org.apache.hyracks.storage.common.file.BufferedFileHandle;

public final class CloudConfigurator {
    private static final IEvictableLocalResourceFilter FILTER =
            (x -> StoragePathUtil.getPartitionNumFromRelativePath(x.getPath()) != StorageConstants.METADATA_PARTITION);
    private final CloudProperties cloudProperties;
    private final IOManager localIoManager;
    private final AbstractCloudIOManager cloudIOManager;
    private final IPhysicalDrive physicalDrive;
    private final IDiskResourceCacheLockNotifier lockNotifier;
    private final IDiskCachedPageAllocator pageAllocator;
    private final IBufferCacheReadContext defaultContext;
    private final boolean diskCacheManagerRequired;
    private final long diskCacheMonitoringInterval;

    private CloudConfigurator(CloudProperties cloudProperties, IIOManager ioManager,
            INamespacePathResolver nsPathResolver, ICloudGuardian guardian) throws HyracksDataException {
        this.cloudProperties = cloudProperties;
        localIoManager = (IOManager) ioManager;
        diskCacheManagerRequired = cloudProperties.getCloudCachePolicy() == CloudCachePolicy.SELECTIVE;
        cloudIOManager = createIOManager(ioManager, cloudProperties, nsPathResolver, guardian);
        physicalDrive = createPhysicalDrive(diskCacheManagerRequired, cloudProperties, ioManager);
        lockNotifier = createLockNotifier(diskCacheManagerRequired);
        pageAllocator = createPageAllocator(diskCacheManagerRequired);
        defaultContext = createDefaultBufferCachePageOpContext(diskCacheManagerRequired, physicalDrive);
        diskCacheMonitoringInterval = cloudProperties.getStorageDiskMonitorInterval();
    }

    public IPartitionBootstrapper getPartitionBootstrapper() {
        return cloudIOManager;
    }

    public IIOManager getCloudIoManager() {
        return cloudIOManager;
    }

    public IDiskResourceCacheLockNotifier getLockNotifier() {
        return lockNotifier;
    }

    public IDiskCachedPageAllocator getPageAllocator() {
        return pageAllocator;
    }

    public IBufferCacheReadContext getDefaultContext() {
        return defaultContext;
    }

    public IDiskCacheMonitoringService createDiskCacheMonitoringService(INCServiceContext serviceContext,
            IBufferCache bufferCache, Map<Integer, BufferedFileHandle> fileInfoMap) {
        if (!diskCacheManagerRequired) {
            return NoOpDiskCacheMonitoringService.INSTANCE;
        }

        CloudDiskResourceCacheLockNotifier resourceCacheManager = (CloudDiskResourceCacheLockNotifier) lockNotifier;
        BufferCache diskBufferCache = (BufferCache) bufferCache;
        int numOfIoDevices = localIoManager.getIODevices().size();
        IApplicationConfig appConfig = serviceContext.getAppConfig();
        int ioParallelism = appConfig.getInt(NCConfig.Option.IO_WORKERS_PER_PARTITION);
        int sweepQueueSize = appConfig.getInt(NCConfig.Option.IO_QUEUE_SIZE);
        int numOfSweepThreads = ioParallelism * numOfIoDevices;
        // Ensure at least each sweep thread has one entry in the queue
        int maxSweepQueueSize = Math.max(numOfSweepThreads, sweepQueueSize);
        long inactiveThreshold = cloudProperties.getStorageIndexInactiveDurationThreshold();
        // +1 for the monitorThread
        ExecutorService executor = Executors.newFixedThreadPool(numOfSweepThreads + 1);
        DiskCacheSweeperThread monitorThread = new DiskCacheSweeperThread(executor, diskCacheMonitoringInterval,
                resourceCacheManager, cloudIOManager, numOfSweepThreads, maxSweepQueueSize, physicalDrive,
                diskBufferCache, fileInfoMap, inactiveThreshold);

        IDiskCacheMonitoringService diskCacheService =
                new CloudDiskCacheMonitoringAndPrefetchingService(executor, physicalDrive, monitorThread);
        localIoManager.setSpaceMaker(monitorThread);
        return diskCacheService;
    }

    public static CloudConfigurator of(CloudProperties cloudProperties, IIOManager ioManager,
            INamespacePathResolver nsPathResolver, ICloudGuardian cloudGuardian) throws HyracksDataException {
        return new CloudConfigurator(cloudProperties, ioManager, nsPathResolver, cloudGuardian);
    }

    public static AbstractCloudIOManager createIOManager(IIOManager ioManager, CloudProperties cloudProperties,
            INamespacePathResolver nsPathResolver, ICloudGuardian guardian) throws HyracksDataException {
        IOManager localIoManager = (IOManager) ioManager;
        CloudCachePolicy policy = cloudProperties.getCloudCachePolicy();
        if (policy == CloudCachePolicy.EAGER) {
            return new EagerCloudIOManager(localIoManager, cloudProperties, nsPathResolver, guardian);
        }

        boolean selective = policy == CloudCachePolicy.SELECTIVE;
        return new LazyCloudIOManager(localIoManager, cloudProperties, nsPathResolver, selective, guardian);
    }

    private static IPhysicalDrive createPhysicalDrive(boolean diskCacheManagerRequired, CloudProperties cloudProperties,
            IIOManager ioManager) throws HyracksDataException {
        if (diskCacheManagerRequired) {
            double storagePercentage = cloudProperties.getStorageAllocationPercentage();
            double pressureThreshold = cloudProperties.getStorageSweepThresholdPercentage();
            long pressureDebugSize = cloudProperties.getStorageDebugSweepThresholdSize();
            return new PhysicalDrive(ioManager.getIODevices(), pressureThreshold, storagePercentage, pressureDebugSize);
        }

        return DummyPhysicalDrive.INSTANCE;
    }

    private static IDiskResourceCacheLockNotifier createLockNotifier(boolean diskCacheManagerRequired) {
        if (diskCacheManagerRequired) {
            return new CloudDiskResourceCacheLockNotifier(FILTER);
        }

        return NoOpDiskResourceCacheLockNotifier.INSTANCE;
    }

    private static IDiskCachedPageAllocator createPageAllocator(boolean diskCacheManagerRequired) {
        if (diskCacheManagerRequired) {
            return CloudDiskCachedPageAllocator.INSTANCE;
        }
        return DefaultDiskCachedPageAllocator.INSTANCE;
    }

    private static IBufferCacheReadContext createDefaultBufferCachePageOpContext(boolean diskCacheManagerRequired,
            IPhysicalDrive drive) {
        if (diskCacheManagerRequired) {
            return new DefaultCloudReadContext(drive);
        }

        return DefaultBufferCacheReadContextProvider.DEFAULT;
    }
}
