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
package org.apache.hyracks.api.dataset;

import org.apache.hyracks.api.comm.IFrameWriter;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.exceptions.HyracksException;
import org.apache.hyracks.api.io.IWorkspaceFileFactory;
import org.apache.hyracks.api.job.JobId;

public interface IDatasetPartitionManager extends IDatasetManager {
    public IFrameWriter createDatasetPartitionWriter(IHyracksTaskContext ctx, ResultSetId rsId, boolean orderedResult,
            boolean asyncMode, int partition, int nPartitions) throws HyracksException;

    public void registerResultPartitionLocation(JobId jobId, ResultSetId rsId, int partition, int nPartitions,
            boolean orderedResult, boolean emptyResult) throws HyracksException;

    public void reportPartitionWriteCompletion(JobId jobId, ResultSetId resultSetId, int partition)
            throws HyracksException;

    public void reportPartitionFailure(JobId jobId, ResultSetId resultSetId, int partition) throws HyracksException;

    public void initializeDatasetPartitionReader(JobId jobId, ResultSetId resultSetId, int partition, IFrameWriter noc)
            throws HyracksException;

    public void removePartition(JobId jobId, ResultSetId resultSetId, int partition);

    public void abortReader(JobId jobId);

    public IWorkspaceFileFactory getFileFactory();

    public void close();
}
