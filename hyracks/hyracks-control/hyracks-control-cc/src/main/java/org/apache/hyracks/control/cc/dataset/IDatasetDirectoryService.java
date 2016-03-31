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
package org.apache.hyracks.control.cc.dataset;

import java.util.List;
import java.util.concurrent.ExecutorService;

import org.apache.hyracks.api.comm.NetworkAddress;
import org.apache.hyracks.api.dataset.DatasetDirectoryRecord;
import org.apache.hyracks.api.dataset.DatasetJobRecord.Status;
import org.apache.hyracks.api.dataset.IDatasetManager;
import org.apache.hyracks.api.dataset.ResultSetId;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.job.IJobLifecycleListener;
import org.apache.hyracks.api.job.JobId;
import org.apache.hyracks.control.common.work.IResultCallback;

public interface IDatasetDirectoryService extends IJobLifecycleListener, IDatasetManager {
    public void init(ExecutorService executor);

    public void registerResultPartitionLocation(JobId jobId, ResultSetId rsId, boolean orderedResult,
            boolean emptyResult, int partition, int nPartitions, NetworkAddress networkAddress);

    public void reportResultPartitionWriteCompletion(JobId jobId, ResultSetId rsId, int partition);

    public void reportResultPartitionFailure(JobId jobId, ResultSetId rsId, int partition);

    public void reportJobFailure(JobId jobId, List<Exception> exceptions);

    public Status getResultStatus(JobId jobId, ResultSetId rsId) throws HyracksDataException;

    public void getResultPartitionLocations(JobId jobId, ResultSetId rsId, DatasetDirectoryRecord[] knownLocations,
            IResultCallback<DatasetDirectoryRecord[]> callback) throws HyracksDataException;
}
