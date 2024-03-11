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

import java.util.List;
import java.util.Set;

import org.apache.asterix.common.cloud.IPartitionBootstrapper;
import org.apache.asterix.common.transactions.IRecoveryManager;
import org.apache.asterix.common.utils.StoragePathUtil;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.io.FileReference;
import org.apache.hyracks.api.io.IIOManager;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class LocalPartitionBootstrapper implements IPartitionBootstrapper {
    private static final Logger LOGGER = LogManager.getLogger();
    private final IIOManager ioManager;

    public LocalPartitionBootstrapper(IIOManager ioManager) {
        this.ioManager = ioManager;
    }

    @Override
    public IRecoveryManager.SystemState getSystemStateOnMissingCheckpoint() {
        //The checkpoint file doesn't exist => Failure happened during NC initialization.
        //Retry to initialize the NC by setting the state to PERMANENT_DATA_LOSS
        LOGGER.info("The checkpoint file doesn't exist: systemState = PERMANENT_DATA_LOSS");
        return IRecoveryManager.SystemState.PERMANENT_DATA_LOSS;
    }

    @Override
    public void bootstrap(Set<Integer> activePartitions, List<FileReference> currentOnDiskPartitions,
            boolean metadataNode, int metadataPartition, boolean cleanup, boolean ensureCompleteBootstrap)
            throws HyracksDataException {
        for (FileReference onDiskPartition : currentOnDiskPartitions) {
            int partitionNum = StoragePathUtil.getPartitionNumFromRelativePath(onDiskPartition.getAbsolutePath());
            if (!activePartitions.contains(partitionNum)) {
                LOGGER.warn("deleting partition {} since it is not on partitions to keep {}", partitionNum,
                        activePartitions);
                ioManager.delete(onDiskPartition);
            }
        }
    }
}
