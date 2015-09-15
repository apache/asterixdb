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

import org.apache.asterix.common.exceptions.ACIDException;
import org.apache.asterix.common.transactions.ILockManager;
import org.apache.asterix.common.transactions.ITransactionContext;
import org.apache.asterix.common.transactions.ITransactionSubsystem;
import org.apache.asterix.transaction.management.service.transaction.TransactionManagementConstants.LockManagerConstants.LockMode;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;
import org.apache.hyracks.storage.am.common.api.IModificationOperationCallback;
import org.apache.hyracks.storage.am.common.ophelpers.IndexOperation;

/**
 * Assumes LSM-BTrees as primary indexes.
 * Performs locking on primary keys, and also logs before/after images.
 */
public class PrimaryIndexModificationOperationCallback extends AbstractIndexModificationOperationCallback implements
        IModificationOperationCallback {

    public PrimaryIndexModificationOperationCallback(int datasetId, int[] primaryKeyFields, ITransactionContext txnCtx,
            ILockManager lockManager, ITransactionSubsystem txnSubsystem, long resourceId, byte resourceType,
            IndexOperation indexOp) {
        super(datasetId, primaryKeyFields, txnCtx, lockManager, txnSubsystem, resourceId, resourceType, indexOp);
    }

    @Override
    public void before(ITupleReference tuple) throws HyracksDataException {
        int pkHash = computePrimaryKeyHashValue(tuple, primaryKeyFields);
        try {
            lockManager.lock(datasetId, pkHash, LockMode.X, txnCtx);
        } catch (ACIDException e) {
            throw new HyracksDataException(e);
        }
    }

    @Override
    public void found(ITupleReference before, ITupleReference after) throws HyracksDataException {
        try {
            int pkHash = computePrimaryKeyHashValue(after, primaryKeyFields);
            log(pkHash, after);
        } catch (ACIDException e) {
            throw new HyracksDataException(e);
        }
    }
}
