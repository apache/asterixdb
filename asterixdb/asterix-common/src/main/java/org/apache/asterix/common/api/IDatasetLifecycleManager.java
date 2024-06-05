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
package org.apache.asterix.common.api;

import java.util.List;
import java.util.Set;
import java.util.function.IntPredicate;
import java.util.function.Predicate;

import org.apache.asterix.common.context.DatasetInfo;
import org.apache.asterix.common.context.IndexInfo;
import org.apache.asterix.common.context.PrimaryIndexOperationTracker;
import org.apache.asterix.common.replication.IReplicationStrategy;
import org.apache.asterix.common.storage.IIndexCheckpointManagerProvider;
import org.apache.asterix.common.storage.StorageIOStats;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMComponentIdGenerator;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIndex;
import org.apache.hyracks.storage.am.lsm.common.api.IVirtualBufferCache;
import org.apache.hyracks.storage.common.IIndex;
import org.apache.hyracks.storage.common.IResourceLifecycleManager;
import org.apache.hyracks.storage.common.buffercache.IRateLimiter;

public interface IDatasetLifecycleManager extends IResourceLifecycleManager<IIndex> {
    /**
     * @param datasetId
     * @param indexId
     * @return The corresponding index, or null if it is not found in the registered indexes.
     * @throws HyracksDataException
     */
    IIndex getIndex(int datasetId, long indexId) throws HyracksDataException;

    /**
     * Indicates if the dataset with id {@code datasetId} is currently registered
     * with this {@link IDatasetLifecycleManager}
     *
     * @param datasetId
     * @return true if the dataset is currently registered. Otherwise false.
     */
    boolean isRegistered(int datasetId);

    /**
     * Flushes all open datasets synchronously.
     *
     * @throws HyracksDataException
     */
    void flushAllDatasets() throws HyracksDataException;

    /**
     * Flushes all open datasets synchronously for partitions {@code partitions}
     *
     * @param partitions
     * @throws HyracksDataException
     */
    void flushAllDatasets(IntPredicate partitions) throws HyracksDataException;

    /**
     * Schedules asynchronous flush on indexes matching the predicate {@code indexPredicate}
     *
     * @param indexPredicate
     * @throws HyracksDataException
     */
    void asyncFlushMatchingIndexes(Predicate<ILSMIndex> indexPredicate) throws HyracksDataException;

    /**
     * creates (if necessary) and returns the dataset info.
     *
     * @param datasetID
     * @return
     */
    DatasetInfo getDatasetInfo(int datasetID);

    /**
     * @param datasetId  the dataset id to be flushed.
     * @param asyncFlush a flag indicating whether to wait for the flush to complete or not.
     * @throws HyracksDataException
     */
    void flushDataset(int datasetId, boolean asyncFlush) throws HyracksDataException;

    /**
     * creates (if necessary) and returns the primary index operation tracker of a dataset.
     *
     * @param datasetId
     * @param partition
     * @param path
     * @return
     */
    PrimaryIndexOperationTracker getOperationTracker(int datasetId, int partition, String path);

    /**
     * creates (if necessary) and returns the component Id generator of a dataset.
     *
     * @param datasetId
     * @param partition
     * @param path
     * @return
     */
    ILSMComponentIdGenerator getComponentIdGenerator(int datasetId, int partition, String path);

    /**
     * creates (if necessary) and returns the rate limiter of a dataset.
     *
     * @param datasetId
     * @param partition
     * @param path
     * @return
     */
    IRateLimiter getRateLimiter(int datasetId, int partition, long writeRateLimit);

    /**
     * creates (if necessary) and returns the dataset virtual buffer caches.
     *
     * @param datasetId
     * @param ioDeviceNum
     * @return
     */
    List<IVirtualBufferCache> getVirtualBufferCaches(int datasetId, int ioDeviceNum);

    /**
     * Attempts to close the datasets in {@code datasetsToClose}
     *
     * @param datasetsToClose
     * @throws HyracksDataException
     */
    void closeDatasets(Set<Integer> datasetsToClose) throws HyracksDataException;

    /**
     * Flushes then closes all open datasets
     */
    void closeAllDatasets() throws HyracksDataException;

    /**
     * @return a list of all indexes that are open at the time of the call.
     */
    List<IndexInfo> getOpenIndexesInfo();

    /**
     * Flushes all opened datasets that are matching {@code replicationStrategy}.
     *
     * @param replicationStrategy
     * @param partitions
     * @throws HyracksDataException
     */
    void flushDataset(IReplicationStrategy replicationStrategy, IntPredicate partitions) throws HyracksDataException;

    /**
     * Waits for all ongoing IO operations on all open datasets that are matching {@code replicationStrategy} and
     * {@code partition}.
     *
     * @param replicationStrategy
     * @param partition
     * @throws HyracksDataException
     */
    void waitForIO(IReplicationStrategy replicationStrategy, int partition) throws HyracksDataException;

    /**
     * Waits for all ongoing IO operations on all open datasets and atomically performs the provided {@code operation}
     * on each opened index before allowing any I/Os to go through.
     *
     * @param replicationStrategy replication strategy
     * @param partition           partition to perform the required operation against
     * @param operation           operation to perform
     */

    void waitForIOAndPerform(IReplicationStrategy replicationStrategy, int partition, IIOBlockingOperation operation)
            throws HyracksDataException;

    /**
     * @return the current datasets io stats
     */
    StorageIOStats getDatasetsIOStats();

    /**
     * Closes {@code resourcePath} if open
     *
     * @param resourcePath
     * @throws HyracksDataException
     */
    void closeIfOpen(String resourcePath) throws HyracksDataException;

    /**
     * Removes all memory references of {@code partition}
     *
     * @param partitionId
     */
    void closePartition(int partitionId);

    IIndexCheckpointManagerProvider getIndexCheckpointManagerProvider();
}
