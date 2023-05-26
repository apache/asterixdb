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

package org.apache.asterix.transaction.management.runtime;

import org.apache.asterix.common.api.IJobEventListenerFactory;
import org.apache.hyracks.algebricks.runtime.base.IPushRuntime;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.value.ITuplePartitionerFactory;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.job.IJobletEventListenerFactory;

public class NoOpCommitRuntimeFactory extends CommitRuntimeFactory {

    private static final long serialVersionUID = 2L;

    public NoOpCommitRuntimeFactory(int datasetId, int[] primaryKeyFields, boolean isWriteTransaction,
            int[] datasetPartitions, boolean isSink, ITuplePartitionerFactory partitionerFactory) {
        super(datasetId, primaryKeyFields, isWriteTransaction, datasetPartitions, isSink, partitionerFactory);
    }

    @Override
    public IPushRuntime[] createPushRuntime(IHyracksTaskContext ctx) throws HyracksDataException {
        IJobletEventListenerFactory fact = ctx.getJobletContext().getJobletEventListenerFactory();
        return new IPushRuntime[] { new NoOpCommitRuntime(ctx, ((IJobEventListenerFactory) fact).getTxnId(datasetId),
                datasetId, primaryKeyFields, isWriteTransaction,
                datasetPartitions[ctx.getTaskAttemptId().getTaskId().getPartition()], isSink, partitionerFactory,
                datasetPartitions) };
    }
}
