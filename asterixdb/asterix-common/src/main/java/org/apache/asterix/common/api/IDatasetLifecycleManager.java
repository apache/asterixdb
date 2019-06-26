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
import java.util.function.Predicate;

import org.apache.asterix.common.context.DatasetInfo;
import org.apache.asterix.common.context.IndexInfo;
import org.apache.asterix.common.context.PrimaryIndexOperationTracker;
import org.apache.asterix.common.replication.IReplicationStrategy;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMComponentIdGenerator;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIndex;
import org.apache.hyracks.storage.am.lsm.common.api.IVirtualBufferCache;
import org.apache.hyracks.storage.common.IIndex;
import org.apache.hyracks.storage.common.IResourceLifecycleManager;

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
     * @param datasetId
     *            the dataset id to be flushed.
     * @param asyncFlush
     *            a flag indicating whether to wait for the flush to complete or not.
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
     * creates (if necessary) and returns the dataset virtual buffer caches.
     *
     * @param datasetId
     * @param ioDeviceNum
     * @return
     */
    List<IVirtualBufferCache> getVirtualBufferCaches(int datasetId, int ioDeviceNum);

    /**
     * Flushes then closes all open datasets
     */
    void closeAllDatasets() throws HyracksDataException;

    /**
     * @return a list of all indexes that are open at the time of the call.
     */
    List<IndexInfo> getOpenIndexesInfo();

    /**
     * Flushes and closes all user datasets (non-metadata datasets)
     *
     * @throws HyracksDataException
     */
    void closeUserDatasets() throws HyracksDataException;

    /**
     * Flushes all opened datasets that are matching {@code replicationStrategy}.
     *
     * @param replicationStrategy
     * @throws HyracksDataException
     */
    void flushDataset(IReplicationStrategy replicationStrategy) throws HyracksDataException;

    /**
     * Waits for all ongoing IO operations on all open datasets that are matching {@code replicationStrategy}.
     *
     * @param replicationStrategy
     * @throws HyracksDataException
     */
    void waitForIO(IReplicationStrategy replicationStrategy) throws HyracksDataException;
}
