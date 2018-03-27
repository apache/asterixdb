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
import org.apache.asterix.common.exceptions.ACIDException;
import org.apache.asterix.common.transactions.AbstractOperationCallbackFactory;
import org.apache.asterix.common.transactions.DatasetId;
import org.apache.asterix.common.transactions.ITransactionContext;
import org.apache.asterix.common.transactions.ITransactionSubsystem;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.IOperatorNodePushable;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.job.IJobletEventListenerFactory;
import org.apache.hyracks.storage.am.common.api.ISearchOperationCallbackFactory;
import org.apache.hyracks.storage.common.ISearchOperationCallback;

public class PrimaryIndexSearchOperationCallbackFactory extends AbstractOperationCallbackFactory
        implements ISearchOperationCallbackFactory {

    private static final long serialVersionUID = 1L;

    public PrimaryIndexSearchOperationCallbackFactory(int datasetId, int[] entityIdFields,
            ITransactionSubsystemProvider txnSubsystemProvider, byte resourceType) {
        super(datasetId, entityIdFields, txnSubsystemProvider, resourceType);
    }

    @Override
    public ISearchOperationCallback createSearchOperationCallback(long resourceId, IHyracksTaskContext ctx,
            IOperatorNodePushable operatorNodePushable) throws HyracksDataException {
        ITransactionSubsystem txnSubsystem = txnSubsystemProvider.getTransactionSubsystem(ctx);
        try {
            IJobletEventListenerFactory fact = ctx.getJobletContext().getJobletEventListenerFactory();
            ITransactionContext txnCtx = txnSubsystem.getTransactionManager()
                    .getTransactionContext(((IJobEventListenerFactory) fact).getTxnId(datasetId));
            return new PrimaryIndexSearchOperationCallback(new DatasetId(datasetId), resourceId, primaryKeyFields,
                    txnSubsystem.getLockManager(), txnCtx);
        } catch (ACIDException e) {
            throw HyracksDataException.create(e);
        }
    }

}
