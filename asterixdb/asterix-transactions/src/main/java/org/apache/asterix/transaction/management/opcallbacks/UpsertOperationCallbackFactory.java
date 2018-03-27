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

import org.apache.asterix.common.api.IJobEventListenerFactory;
import org.apache.asterix.common.context.ITransactionSubsystemProvider;
import org.apache.asterix.common.dataflow.DatasetLocalResource;
import org.apache.asterix.common.exceptions.ACIDException;
import org.apache.asterix.common.transactions.AbstractOperationCallbackFactory;
import org.apache.asterix.common.transactions.DatasetId;
import org.apache.asterix.common.transactions.ITransactionContext;
import org.apache.asterix.common.transactions.ITransactionSubsystem;
import org.apache.asterix.transaction.management.opcallbacks.AbstractIndexModificationOperationCallback.Operation;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.IOperatorNodePushable;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.job.IJobletEventListenerFactory;
import org.apache.hyracks.storage.am.common.api.IModificationOperationCallbackFactory;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIndex;
import org.apache.hyracks.storage.common.IModificationOperationCallback;
import org.apache.hyracks.storage.common.IResourceLifecycleManager;
import org.apache.hyracks.storage.common.LocalResource;

public class UpsertOperationCallbackFactory extends AbstractOperationCallbackFactory
        implements IModificationOperationCallbackFactory {

    private static final long serialVersionUID = 1L;
    protected final Operation indexOp;

    public UpsertOperationCallbackFactory(int datasetId, int[] primaryKeyFields,
            ITransactionSubsystemProvider txnSubsystemProvider, Operation indexOp, byte resourceType) {
        super(datasetId, primaryKeyFields, txnSubsystemProvider, resourceType);
        this.indexOp = indexOp;
    }

    @Override
    public IModificationOperationCallback createModificationOperationCallback(LocalResource resource,
            IHyracksTaskContext ctx, IOperatorNodePushable operatorNodePushable) throws HyracksDataException {
        DatasetLocalResource aResource = (DatasetLocalResource) resource.getResource();
        ITransactionSubsystem txnSubsystem = txnSubsystemProvider.getTransactionSubsystem(ctx);
        IResourceLifecycleManager indexLifeCycleManager =
                txnSubsystem.getApplicationContext().getDatasetLifecycleManager();
        ILSMIndex index = (ILSMIndex) indexLifeCycleManager.get(resource.getPath());
        if (index == null) {
            throw new HyracksDataException("Index(id:" + resource.getId() + ") is not registered.");
        }

        try {
            IJobletEventListenerFactory fact = ctx.getJobletContext().getJobletEventListenerFactory();
            ITransactionContext txnCtx = txnSubsystem.getTransactionManager()
                    .getTransactionContext(((IJobEventListenerFactory) fact).getTxnId(datasetId));
            IModificationOperationCallback modCallback = new UpsertOperationCallback(new DatasetId(datasetId),
                    primaryKeyFields, txnCtx, txnSubsystem.getLockManager(), txnSubsystem, resource.getId(),
                    aResource.getPartition(), resourceType, indexOp);
            txnCtx.register(resource.getId(), aResource.getPartition(), index, modCallback, true);
            return modCallback;
        } catch (ACIDException e) {
            throw HyracksDataException.create(e);
        }
    }
}
