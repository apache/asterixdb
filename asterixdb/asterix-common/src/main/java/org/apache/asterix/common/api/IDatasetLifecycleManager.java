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

import org.apache.asterix.common.context.DatasetInfo;
import org.apache.asterix.common.context.IndexInfo;
import org.apache.asterix.common.context.PrimaryIndexOperationTracker;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.storage.am.common.api.IIndex;
import org.apache.hyracks.storage.am.common.api.IResourceLifecycleManager;
import org.apache.hyracks.storage.am.lsm.common.api.IVirtualBufferCache;

public interface IDatasetLifecycleManager extends IResourceLifecycleManager<IIndex> {
    /**
     * @param datasetId
     * @param indexId
     * @return The corresponding index, or null if it is not found in the registered indexes.
     * @throws HyracksDataException
     */
    IIndex getIndex(int datasetId, long indexId) throws HyracksDataException;

    /**
     * Flushes all open datasets synchronously.
     *
     * @throws HyracksDataException
     */
    void flushAllDatasets() throws HyracksDataException;

    /**
     * Schedules asynchronous flush on datasets that have memory components with first LSN < nonSharpCheckpointTargetLSN.
     *
     * @param nonSharpCheckpointTargetLSN
     * @throws HyracksDataException
     */
    void scheduleAsyncFlushForLaggingDatasets(long nonSharpCheckpointTargetLSN) throws HyracksDataException;

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
     * @param datasetID
     * @return
     */
    PrimaryIndexOperationTracker getOperationTracker(int datasetID);

    /**
     * creates (if necessary) and returns the dataset virtual buffer caches.
     *
     * @param datasetID
     * @param ioDeviceNum
     * @return
     */
    List<IVirtualBufferCache> getVirtualBufferCaches(int datasetID, int ioDeviceNum);

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
}
