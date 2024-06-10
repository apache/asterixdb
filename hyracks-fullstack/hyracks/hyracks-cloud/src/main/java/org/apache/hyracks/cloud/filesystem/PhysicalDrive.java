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
package org.apache.hyracks.cloud.filesystem;

import java.io.File;
import java.io.IOException;
import java.nio.file.FileStore;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.io.IODeviceHandle;
import org.apache.hyracks.storage.common.disk.IPhysicalDrive;
import org.apache.hyracks.util.StorageUtil;
import org.apache.hyracks.util.annotations.ThreadSafe;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

@ThreadSafe
public final class PhysicalDrive implements IPhysicalDrive {
    private static final Logger LOGGER = LogManager.getLogger();
    private final List<FileStore> drivePaths;
    private final DiskSpace diskSpace;
    private final AtomicLong usedSpace;

    public PhysicalDrive(List<IODeviceHandle> deviceHandles, double pressureThreshold, double storagePercentage,
            long pressureDebugSize) throws HyracksDataException {
        drivePaths = getDrivePaths(deviceHandles);
        diskSpace = getPressureSize(drivePaths, pressureThreshold, storagePercentage, pressureDebugSize);
        usedSpace = new AtomicLong();
        computeAndCheckIsPressured();
    }

    @Override
    public boolean computeAndCheckIsPressured() {
        long usedSpace = getUsedSpace();
        long pressureCapacity = diskSpace.getPressureCapacity();
        boolean isPressured = usedSpace > pressureCapacity;
        this.usedSpace.set(usedSpace);

        if (isPressured) {
            LOGGER.info("Used space: {}, pressureCapacity: {} (isPressured: {})",
                    StorageUtil.toHumanReadableSize(usedSpace), StorageUtil.toHumanReadableSize(pressureCapacity),
                    true);
        } else {
            LOGGER.debug("Used space: {}, pressureCapacity: {} (isPressured: {})",
                    StorageUtil.toHumanReadableSize(usedSpace), StorageUtil.toHumanReadableSize(pressureCapacity),
                    false);
        }

        return isPressured;
    }

    @Override
    public boolean isUnpressured() {
        return usedSpace.get() <= diskSpace.getPressureCapacity();
    }

    @Override
    public boolean hasSpace() {
        return usedSpace.get() < diskSpace.getPressureCapacity();
    }

    private long getUsedSpace() {
        long totalUsedSpace = 0;
        for (int i = 0; i < drivePaths.size(); i++) {
            FileStore device = drivePaths.get(i);
            try {
                totalUsedSpace += getTotalSpace(device) - getUsableSpace(device);
            } catch (HyracksDataException e) {
                LOGGER.warn("Cannot get used space", e);
            }
        }
        return totalUsedSpace;
    }

    private static DiskSpace getPressureSize(List<FileStore> drivePaths, double pressureThreshold,
            double storagePercentage, long pressureDebugSize) throws HyracksDataException {

        long totalCapacity = 0;
        long totalUsedSpace = 0;
        for (FileStore drive : drivePaths) {
            long totalSpace = getTotalSpace(drive);
            totalCapacity += totalSpace;
            totalUsedSpace += totalSpace - getUsableSpace(drive);
        }

        long allocatedCapacity = (long) (totalCapacity * storagePercentage);
        long pressureCapacity = pressureDebugSize > 0 ? totalUsedSpace + pressureDebugSize
                : (long) (allocatedCapacity * pressureThreshold);

        LOGGER.info(
                "PhysicalDrive configured with diskCapacity: {}, allocatedCapacity: {}, and pressureCapacity: {} (used space: {})",
                StorageUtil.toHumanReadableSize(totalCapacity), StorageUtil.toHumanReadableSize(allocatedCapacity),
                StorageUtil.toHumanReadableSize(pressureCapacity), StorageUtil.toHumanReadableSize(totalUsedSpace));

        return new DiskSpace(allocatedCapacity, pressureCapacity);
    }

    private static List<FileStore> getDrivePaths(List<IODeviceHandle> deviceHandles) throws HyracksDataException {
        Set<String> distinctDrives = new HashSet<>();
        List<FileStore> fileStores = new ArrayList<>();
        for (IODeviceHandle handle : deviceHandles) {
            FileStore fileStore = createFileStore(handle.getMount());
            String driveName = fileStore.name();
            if (!distinctDrives.contains(driveName)) {
                fileStores.add(fileStore);
                distinctDrives.add(driveName);
            }
        }
        return fileStores;
    }

    private static FileStore createFileStore(File root) throws HyracksDataException {
        try {
            return Files.getFileStore(root.toPath());
        } catch (IOException e) {
            throw HyracksDataException.create(e);
        }
    }

    private static long getTotalSpace(FileStore fileStore) throws HyracksDataException {
        try {
            return fileStore.getTotalSpace();
        } catch (IOException e) {
            throw HyracksDataException.create(e);
        }
    }

    private static long getUsableSpace(FileStore fileStore) throws HyracksDataException {
        try {
            return fileStore.getUsableSpace();
        } catch (IOException e) {
            throw HyracksDataException.create(e);
        }
    }

    private static class DiskSpace {
        private final long allocatedCapacity;
        private final long pressureCapacity;

        private DiskSpace(long allocatedCapacity, long pressureCapacity) {
            this.allocatedCapacity = allocatedCapacity;
            this.pressureCapacity = pressureCapacity;
        }

        public long getAllocatedCapacity() {
            return allocatedCapacity;
        }

        public long getPressureCapacity() {
            return pressureCapacity;
        }
    }
}
