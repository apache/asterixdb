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
import org.apache.asterix.common.transactions.AbstractOperationCallback;
import org.apache.asterix.common.transactions.DatasetId;
import org.apache.asterix.common.transactions.ILockManager;
import org.apache.asterix.common.transactions.ITransactionContext;
import org.apache.asterix.transaction.management.service.transaction.TransactionManagementConstants.LockManagerConstants.LockMode;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;
import org.apache.hyracks.storage.common.ISearchOperationCallback;

/**
 * Assumes LSM-BTrees as primary indexes. Implements try/locking and unlocking on primary keys.
 */
public class PrimaryIndexInstantSearchOperationCallback extends AbstractOperationCallback
        implements ISearchOperationCallback {

    public PrimaryIndexInstantSearchOperationCallback(DatasetId datasetId, long resourceId, int[] entityIdFields,
            ILockManager lockManager, ITransactionContext txnCtx) {
        super(datasetId, resourceId, entityIdFields, txnCtx, lockManager);
    }

    @Override
    public boolean proceed(ITupleReference tuple) throws HyracksDataException {
        int pkHash = computePrimaryKeyHashValue(tuple, primaryKeyFields);
        try {
            return lockManager.instantTryLock(datasetId, pkHash, LockMode.S, txnCtx);
        } catch (ACIDException e) {
            throw HyracksDataException.create(e);
        }
    }

    @Override
    public void reconcile(ITupleReference tuple) throws HyracksDataException {
        int pkHash = computePrimaryKeyHashValue(tuple, primaryKeyFields);
        try {
            lockManager.lock(datasetId, pkHash, LockMode.S, txnCtx);
        } catch (ACIDException e) {
            throw HyracksDataException.create(e);
        }
    }

    /**
     * Cancels the reconcile() operation. Since reconcile() gets a lock, this lock
     * needs to be unlocked to reverse the effect of reconcile().
     */
    @Override
    public void cancel(ITupleReference tuple) throws HyracksDataException {
        int pkHash = computePrimaryKeyHashValue(tuple, primaryKeyFields);
        try {
            lockManager.unlock(datasetId, pkHash, LockMode.S, txnCtx);
        } catch (ACIDException e) {
            throw HyracksDataException.create(e);
        }
    }

    @Override
    public void complete(ITupleReference tuple) throws HyracksDataException {
        int pkHash = computePrimaryKeyHashValue(tuple, primaryKeyFields);
        try {
            lockManager.unlock(datasetId, pkHash, LockMode.S, txnCtx);
        } catch (ACIDException e) {
            throw HyracksDataException.create(e);
        }
    }

    @Override
    public void before(ITupleReference tuple) throws HyracksDataException {
    }
}
