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
package org.apache.hyracks.api.result;

import org.apache.hyracks.api.comm.IFrameWriter;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.exceptions.HyracksException;
import org.apache.hyracks.api.job.JobId;

public interface IResultPartitionManager extends IResultManager {
    IFrameWriter createResultPartitionWriter(IHyracksTaskContext ctx, ResultSetId rsId, IResultMetadata metadata,
            boolean asyncMode, int partition, int nPartitions, long maxReads) throws HyracksException;

    void registerResultPartitionLocation(JobId jobId, ResultSetId rsId, int partition, int nPartitions,
            IResultMetadata metadata, boolean emptyResult) throws HyracksException;

    void reportPartitionWriteCompletion(JobId jobId, ResultSetId resultSetId, int partition) throws HyracksException;

    void initializeResultPartitionReader(JobId jobId, ResultSetId resultSetId, int partition, IFrameWriter noc)
            throws HyracksException;

    void removePartition(JobId jobId, ResultSetId resultSetId, int partition);

    void abortReader(JobId jobId);

    void close();

}
