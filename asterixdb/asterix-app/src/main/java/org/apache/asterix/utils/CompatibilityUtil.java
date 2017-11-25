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
package org.apache.asterix.utils;

import java.util.ArrayList;
import java.util.List;
import java.util.logging.Logger;

import org.apache.asterix.app.nc.task.MigrateStorageResourcesTask;
import org.apache.asterix.common.api.INCLifecycleTask;
import org.apache.asterix.common.utils.StorageConstants;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.control.nc.NodeControllerService;

public class CompatibilityUtil {

    private static final Logger LOGGER = Logger.getLogger(CompatibilityUtil.class.getName());
    private static final int MIN_COMPATIBLE_VERSION = 1;

    private CompatibilityUtil() {
    }

    public static void ensureCompatibility(NodeControllerService ncs, int onDiskVerson) throws HyracksDataException {
        if (onDiskVerson == StorageConstants.VERSION) {
            return;
        }
        ensureUpgradability(onDiskVerson);
        LOGGER.info(() -> "Upgrading from storage version " + onDiskVerson + " to " + StorageConstants.VERSION);
        final List<INCLifecycleTask> upgradeTasks = getUpgradeTasks(onDiskVerson);
        for (INCLifecycleTask task : upgradeTasks) {
            task.perform(ncs);
        }
    }

    private static void ensureUpgradability(int onDiskVerson) {
        if (onDiskVerson < MIN_COMPATIBLE_VERSION) {
            throw new IllegalStateException(String.format(
                    "Storage cannot be upgraded to new version. Current version (%s). On disk version: (%s)",
                    StorageConstants.VERSION, onDiskVerson));
        }
    }

    private static List<INCLifecycleTask> getUpgradeTasks(int fromVersion) {
        List<INCLifecycleTask> upgradeTasks = new ArrayList<>();
        if (fromVersion < StorageConstants.REBALANCE_STORAGE_VERSION) {
            upgradeTasks.add(new MigrateStorageResourcesTask());
        }
        return upgradeTasks;
    }
}