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
package org.apache.hyracks.control.cc.result;

import java.util.List;
import java.util.concurrent.ExecutorService;

import org.apache.hyracks.api.comm.NetworkAddress;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.job.IJobLifecycleListener;
import org.apache.hyracks.api.job.JobId;
import org.apache.hyracks.api.result.IJobResultCallback;
import org.apache.hyracks.api.result.IResultManager;
import org.apache.hyracks.api.result.IResultMetadata;
import org.apache.hyracks.api.result.ResultDirectoryRecord;
import org.apache.hyracks.api.result.ResultJobRecord.Status;
import org.apache.hyracks.api.result.ResultSetId;
import org.apache.hyracks.control.common.work.IResultCallback;

public interface IResultDirectoryService extends IJobLifecycleListener, IResultManager {
    public void init(ExecutorService executor, IJobResultCallback callback);

    public void registerResultPartitionLocation(JobId jobId, ResultSetId rsId, IResultMetadata metadata,
            boolean emptyResult, int partition, int nPartitions, NetworkAddress networkAddress)
            throws HyracksDataException;

    public void reportResultPartitionWriteCompletion(JobId jobId, ResultSetId rsId, int partition)
            throws HyracksDataException;

    public void reportJobFailure(JobId jobId, List<Exception> exceptions);

    public Status getResultStatus(JobId jobId, ResultSetId rsId) throws HyracksDataException;

    public IResultMetadata getResultMetadata(JobId jobId, ResultSetId rsId) throws HyracksDataException;

    public void getResultPartitionLocations(JobId jobId, ResultSetId rsId, ResultDirectoryRecord[] knownLocations,
            IResultCallback<ResultDirectoryRecord[]> callback) throws HyracksDataException;
}
