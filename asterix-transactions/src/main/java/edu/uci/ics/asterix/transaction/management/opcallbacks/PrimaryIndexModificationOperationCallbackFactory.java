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

package edu.uci.ics.asterix.transaction.management.opcallbacks;

import edu.uci.ics.asterix.common.context.ITransactionSubsystemProvider;
import edu.uci.ics.asterix.common.exceptions.ACIDException;
import edu.uci.ics.asterix.common.transactions.AbstractOperationCallback;
import edu.uci.ics.asterix.common.transactions.AbstractOperationCallbackFactory;
import edu.uci.ics.asterix.common.transactions.ITransactionContext;
import edu.uci.ics.asterix.common.transactions.ITransactionSubsystem;
import edu.uci.ics.asterix.common.transactions.JobId;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
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
            ITransactionSubsystemProvider txnSubsystemProvider, IndexOperation indexOp, byte resourceType) {
        super(jobId, datasetId, primaryKeyFields, txnSubsystemProvider, resourceType);
        this.indexOp = indexOp;
    }

    @Override
    public IModificationOperationCallback createModificationOperationCallback(long resourceId, Object resource,
            IHyracksTaskContext ctx) throws HyracksDataException {

        ITransactionSubsystem txnSubsystem = txnSubsystemProvider.getTransactionSubsystem(ctx);
        IIndexLifecycleManager indexLifeCycleManager = txnSubsystem.getAsterixAppRuntimeContextProvider()
                .getIndexLifecycleManager();
        ILSMIndex index = (ILSMIndex) indexLifeCycleManager.getIndex(resourceId);
        if (index == null) {
            throw new HyracksDataException("Index(id:" + resourceId + ") is not registered.");
        }

        try {
            ITransactionContext txnCtx = txnSubsystem.getTransactionManager().getTransactionContext(jobId, false);
            IModificationOperationCallback modCallback = new PrimaryIndexModificationOperationCallback(datasetId,
                    primaryKeyFields, txnCtx, txnSubsystem.getLockManager(), txnSubsystem, resourceId, resourceType,
                    indexOp);
            txnCtx.registerIndexAndCallback(resourceId, index, (AbstractOperationCallback) modCallback, true);
            return modCallback;
        } catch (ACIDException e) {
            throw new HyracksDataException(e);
        }
    }
}
