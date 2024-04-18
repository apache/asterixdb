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
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.hyracks.api.io.IODeviceHandle;
import org.apache.hyracks.storage.common.disk.IPhysicalDrive;
import org.apache.hyracks.util.StorageUtil;
import org.apache.hyracks.util.annotations.ThreadSafe;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

@ThreadSafe
public final class PhysicalDrive implements IPhysicalDrive {
    private static final Logger LOGGER = LogManager.getLogger();
    private final List<File> drivePaths;
    private final long pressureSize;
    private final AtomicBoolean pressured;

    public PhysicalDrive(List<IODeviceHandle> deviceHandles, double pressureThreshold, double storagePercentage,
            long pressureDebugSize) {
        drivePaths = getDrivePaths(deviceHandles);
        pressureSize = getPressureSize(drivePaths, pressureThreshold, storagePercentage, pressureDebugSize);
        pressured = new AtomicBoolean(getUsedSpace() <= pressureSize);
    }

    @Override
    public boolean computeAndCheckIsPressured() {
        long usedSpace = getUsedSpace();
        boolean isPressured = usedSpace > pressureSize;
        pressured.set(isPressured);
        LOGGER.info("Used space: {}, pressureCapacity: {} (isPressured: {})",
                StorageUtil.toHumanReadableSize(usedSpace), StorageUtil.toHumanReadableSize(pressureSize), isPressured);
        return isPressured;
    }

    @Override
    public boolean hasSpace() {
        return !pressured.get();
    }

    private long getUsedSpace() {
        long totalUsedSpace = 0;
        for (int i = 0; i < drivePaths.size(); i++) {
            File device = drivePaths.get(i);
            totalUsedSpace += device.getTotalSpace() - device.getFreeSpace();
        }
        return totalUsedSpace;
    }

    private static long getPressureSize(List<File> drivePaths, double pressureThreshold, double storagePercentage,
            long pressureDebugSize) {

        long totalCapacity = 0;
        long totalUsedSpace = 0;
        for (File drive : drivePaths) {
            totalCapacity += drive.getTotalSpace();
            totalUsedSpace += drive.getTotalSpace() - drive.getFreeSpace();
        }

        long allocatedCapacity = (long) (totalCapacity * storagePercentage);
        long pressureCapacity = pressureDebugSize > 0 ? totalUsedSpace + pressureDebugSize
                : (long) (allocatedCapacity * pressureThreshold);

        LOGGER.info(
                "PhysicalDrive configured with diskCapacity: {}, allocatedCapacity: {}, and pressureCapacity: {} (used space: {})",
                StorageUtil.toHumanReadableSize(totalCapacity), StorageUtil.toHumanReadableSize(allocatedCapacity),
                StorageUtil.toHumanReadableSize(pressureCapacity), StorageUtil.toHumanReadableSize(totalUsedSpace));

        return pressureCapacity;
    }

    private static List<File> getDrivePaths(List<IODeviceHandle> deviceHandles) {
        File[] roots = File.listRoots();
        Set<File> distinctUsedRoots = new HashSet<>();
        for (IODeviceHandle handle : deviceHandles) {
            File handlePath = handle.getMount();
            for (File root : roots) {
                if (handlePath.getAbsolutePath().startsWith(root.getAbsolutePath())
                        && !distinctUsedRoots.contains(root)) {
                    distinctUsedRoots.add(root);
                    break;
                }
            }
        }
        return new ArrayList<>(distinctUsedRoots);
    }
}
