/*
 * Copyright 2009-2013 by The Regents of the University of California
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * you may obtain a copy of the License from
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.uci.ics.asterix.runtime.operators.std;

import java.nio.ByteBuffer;

import edu.uci.ics.asterix.common.api.IAsterixAppRuntimeContext;
import edu.uci.ics.asterix.common.context.DatasetLifecycleManager;
import edu.uci.ics.asterix.common.exceptions.ACIDException;
import edu.uci.ics.asterix.common.transactions.DatasetId;
import edu.uci.ics.asterix.common.transactions.ILockManager;
import edu.uci.ics.asterix.common.transactions.ITransactionContext;
import edu.uci.ics.asterix.common.transactions.ITransactionManager;
import edu.uci.ics.asterix.common.transactions.JobId;
import edu.uci.ics.asterix.transaction.management.service.transaction.TransactionManagementConstants.LockManagerConstants.LockMode;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.dataflow.IOperatorNodePushable;
import edu.uci.ics.hyracks.api.dataflow.value.IRecordDescriptorProvider;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.api.job.IOperatorDescriptorRegistry;
import edu.uci.ics.hyracks.dataflow.std.base.AbstractSingleActivityOperatorDescriptor;
import edu.uci.ics.hyracks.dataflow.std.base.AbstractUnaryInputSinkOperatorNodePushable;

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
                    DatasetLifecycleManager datasetLifeCycleManager = (DatasetLifecycleManager) runtimeCtx
                            .getIndexLifecycleManager();
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
