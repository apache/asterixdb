/*
 * Copyright 2009-2012 by The Regents of the University of California
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

package edu.uci.ics.asterix.transaction.management.opcallbacks;

import edu.uci.ics.asterix.transaction.management.exception.ACIDException;
import edu.uci.ics.asterix.transaction.management.resource.TransactionalResourceManagerRepository;
import edu.uci.ics.asterix.transaction.management.service.transaction.ITransactionSubsystemProvider;
import edu.uci.ics.asterix.transaction.management.service.transaction.JobId;
import edu.uci.ics.asterix.transaction.management.service.transaction.TransactionContext;
import edu.uci.ics.asterix.transaction.management.service.transaction.TransactionSubsystem;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryHashFunctionFactory;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.storage.am.common.api.IIndexLifecycleManager;
import edu.uci.ics.hyracks.storage.am.common.api.IModificationOperationCallback;
import edu.uci.ics.hyracks.storage.am.common.api.IModificationOperationCallbackFactory;
import edu.uci.ics.hyracks.storage.am.common.ophelpers.IndexOperation;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMIndex;

/**
 * Assumes LSM-BTrees as primary indexes.
 */
public class PrimaryIndexModificationOperationCallbackFactory extends AbstractOperationCallbackFactory implements
        IModificationOperationCallbackFactory {

    private static final long serialVersionUID = 1L;
    private final IndexOperation indexOp;

    public PrimaryIndexModificationOperationCallbackFactory(JobId jobId, int datasetId, int[] primaryKeyFields,
            IBinaryHashFunctionFactory[] primaryKeyHashFunctionFactories,
            ITransactionSubsystemProvider txnSubsystemProvider, IndexOperation indexOp, byte resourceType) {
        super(jobId, datasetId, primaryKeyFields, primaryKeyHashFunctionFactories, txnSubsystemProvider, resourceType);
        this.indexOp = indexOp;
    }

    @Override
    public IModificationOperationCallback createModificationOperationCallback(long resourceId, Object resource,
            IHyracksTaskContext ctx) throws HyracksDataException {

        TransactionSubsystem txnSubsystem = txnSubsystemProvider.getTransactionSubsystem(ctx);
        IIndexLifecycleManager indexLifeCycleManager = txnSubsystem.getAsterixAppRuntimeContextProvider()
                .getIndexLifecycleManager();
        ILSMIndex index = (ILSMIndex) indexLifeCycleManager.getIndex(resourceId);
        if (index == null) {
            throw new HyracksDataException("Index(id:"+resourceId+") is not registered.");
        }

        try {
            TransactionContext txnCtx = txnSubsystem.getTransactionManager().getTransactionContext(jobId);
            IModificationOperationCallback modCallback = new PrimaryIndexModificationOperationCallback(datasetId,
                    primaryKeyFields, primaryKeyHashFunctionFactories, txnCtx, txnSubsystem.getLockManager(),
                    txnSubsystem, resourceId, resourceType, indexOp);
            txnCtx.registerIndexAndCallback(index, (AbstractOperationCallback) modCallback);
            return modCallback;
        } catch (ACIDException e) {
            throw new HyracksDataException(e);
        }
    }
}
