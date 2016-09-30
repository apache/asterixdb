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

package org.apache.asterix.transaction.management.opcallbacks;

import org.apache.asterix.common.context.ITransactionSubsystemProvider;
import org.apache.asterix.common.exceptions.ACIDException;
import org.apache.asterix.common.transactions.AbstractOperationCallback;
import org.apache.asterix.common.transactions.AbstractOperationCallbackFactory;
import org.apache.asterix.common.transactions.ITransactionContext;
import org.apache.asterix.common.transactions.ITransactionSubsystem;
import org.apache.asterix.common.transactions.JobId;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.IOperatorNodePushable;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.storage.am.common.api.IResourceLifecycleManager;
import org.apache.hyracks.storage.am.common.api.IModificationOperationCallback;
import org.apache.hyracks.storage.am.common.api.IModificationOperationCallbackFactory;
import org.apache.hyracks.storage.am.common.ophelpers.IndexOperation;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIndex;

/**
 * Assumes LSM-BTrees as primary indexes.
 */
public class PrimaryIndexModificationOperationCallbackFactory extends AbstractOperationCallbackFactory
        implements IModificationOperationCallbackFactory {

    private static final long serialVersionUID = 1L;
    private final IndexOperation indexOp;
    private final boolean logBeforeImage;

    public PrimaryIndexModificationOperationCallbackFactory(JobId jobId, int datasetId, int[] primaryKeyFields,
            ITransactionSubsystemProvider txnSubsystemProvider, IndexOperation indexOp, byte resourceType,
            boolean logBeforeImage) {
        super(jobId, datasetId, primaryKeyFields, txnSubsystemProvider, resourceType);
        this.indexOp = indexOp;
        this.logBeforeImage = logBeforeImage;
    }

    @Override
    public IModificationOperationCallback createModificationOperationCallback(String resourcePath, long resourceId,
            int resourcePartition, Object resource, IHyracksTaskContext ctx, IOperatorNodePushable operatorNodePushable)
            throws HyracksDataException {

        ITransactionSubsystem txnSubsystem = txnSubsystemProvider.getTransactionSubsystem(ctx);
        IResourceLifecycleManager indexLifeCycleManager =
                txnSubsystem.getAsterixAppRuntimeContextProvider().getDatasetLifecycleManager();
        ILSMIndex index = (ILSMIndex) indexLifeCycleManager.get(resourcePath);
        if (index == null) {
            throw new HyracksDataException("Index(id:" + resourceId + ") is not registered.");
        }

        try {
            ITransactionContext txnCtx = txnSubsystem.getTransactionManager().getTransactionContext(jobId, false);
            IModificationOperationCallback modCallback = new PrimaryIndexModificationOperationCallback(datasetId,
                    primaryKeyFields, txnCtx, txnSubsystem.getLockManager(), txnSubsystem, resourceId,
                    resourcePartition, resourceType, indexOp, operatorNodePushable, logBeforeImage);
            txnCtx.registerIndexAndCallback(resourceId, index, (AbstractOperationCallback) modCallback, true);
            return modCallback;
        } catch (ACIDException e) {
            throw new HyracksDataException(e);
        }
    }
}
