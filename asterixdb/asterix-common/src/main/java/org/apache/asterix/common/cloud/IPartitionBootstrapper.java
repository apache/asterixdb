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
package org.apache.asterix.common.cloud;

import java.util.List;
import java.util.Set;

import org.apache.asterix.common.transactions.IRecoveryManager;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.io.FileReference;

/**
 * Bootstraps a node's partitions directories
 */
public interface IPartitionBootstrapper {

    /**
     * @return the systems state in case the checkpoint upon calling {@link IRecoveryManager#getSystemState()}
     * is missing the checkpoint file
     */
    IRecoveryManager.SystemState getSystemStateOnMissingCheckpoint();

    /**
     * Bootstraps the node's partitions directory by doing the following:
     * - Deleting the directories in <code>currentOnDiskPartitions</code> that are not in <code>activePartitions</code>
     * - In cloud deployment only, it does the following:
     * This will do the following:
     * - Cleanup all local files (i.e., delete all local files that do not exist in the cloud storage)
     * - Depends on the caching policy:
     * - - Eager: Will download the contents of all active partitions
     * - - Lazy: No file will be downloaded at start, but will be when opened
     *
     * @param activePartitions        the current active storage partitions of the NC
     * @param currentOnDiskPartitions paths to the current local partitions
     * @param metadataNode            whether the node is a metadata node as well
     * @param metadataPartition       metadata partition number
     * @param cleanup                 performs cleanup by deleting all unkept partitions
     * @param ensureCompleteBootstrap ensures the metadata catalog was fully bootstrapped
     */
    void bootstrap(Set<Integer> activePartitions, List<FileReference> currentOnDiskPartitions, boolean metadataNode,
            int metadataPartition, boolean cleanup, boolean ensureCompleteBootstrap) throws HyracksDataException;
}
