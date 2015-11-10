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
package org.apache.asterix.runtime.operators.std;

import java.nio.ByteBuffer;

import org.apache.asterix.common.api.IAsterixAppRuntimeContext;
import org.apache.asterix.common.api.IDatasetLifecycleManager;
import org.apache.asterix.common.exceptions.ACIDException;
import org.apache.asterix.common.transactions.DatasetId;
import org.apache.asterix.common.transactions.ILockManager;
import org.apache.asterix.common.transactions.ITransactionContext;
import org.apache.asterix.common.transactions.ITransactionManager;
import org.apache.asterix.common.transactions.JobId;
import org.apache.asterix.transaction.management.service.transaction.TransactionManagementConstants.LockManagerConstants.LockMode;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.IOperatorNodePushable;
import org.apache.hyracks.api.dataflow.value.IRecordDescriptorProvider;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.job.IOperatorDescriptorRegistry;
import org.apache.hyracks.dataflow.std.base.AbstractSingleActivityOperatorDescriptor;
import org.apache.hyracks.dataflow.std.base.AbstractUnaryInputSinkOperatorNodePushable;

public class FlushDatasetOperatorDescriptor extends AbstractSingleActivityOperatorDescriptor {
    private static final long serialVersionUID = 1L;
    private final JobId jobId;
    private final DatasetId datasetId;

    public FlushDatasetOperatorDescriptor(IOperatorDescriptorRegistry spec, JobId jobId, int datasetId) {
        super(spec, 1, 0);
        this.jobId = jobId;
        this.datasetId = new DatasetId(datasetId);
    }

    @Override
    public IOperatorNodePushable createPushRuntime(final IHyracksTaskContext ctx,
            IRecordDescriptorProvider recordDescProvider, int partition, int nPartitions) throws HyracksDataException {
        return new AbstractUnaryInputSinkOperatorNodePushable() {

            @Override
            public void open() throws HyracksDataException {

            }

            @Override
            public void nextFrame(ByteBuffer buffer) throws HyracksDataException {

            }

            @Override
            public void fail() throws HyracksDataException {
                this.close();
            }

            @Override
            public void close() throws HyracksDataException {
                try {
                    IAsterixAppRuntimeContext runtimeCtx = (IAsterixAppRuntimeContext) ctx.getJobletContext()
                            .getApplicationContext().getApplicationObject();
                    IDatasetLifecycleManager datasetLifeCycleManager = runtimeCtx.getDatasetLifecycleManager();
                    ILockManager lockManager = runtimeCtx.getTransactionSubsystem().getLockManager();
                    ITransactionManager txnManager = runtimeCtx.getTransactionSubsystem().getTransactionManager();
                    // get the local transaction
                    ITransactionContext txnCtx = txnManager.getTransactionContext(jobId, false);
                    // lock the dataset granule
                    lockManager.lock(datasetId, -1, LockMode.S, txnCtx);
                    // flush the dataset synchronously
                    datasetLifeCycleManager.flushDataset(datasetId.getId(), false);
                } catch (ACIDException e) {
                    throw new HyracksDataException(e);
                }
            }

        };
    }
}
