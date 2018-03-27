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
 * This Callback method tries to get a lock on PK (instantTryLock).
 * If it fails, this callback does nothing since its purpose is to attempt to get an instant lock
 * and get the result of it. This operation callback is used in an index-only plan.
 */
public class SecondaryIndexInstantSearchOperationCallback extends AbstractOperationCallback
        implements ISearchOperationCallback {

    public SecondaryIndexInstantSearchOperationCallback(DatasetId datasetId, long resourceId, int[] entityIdFields,
            ILockManager lockManager, ITransactionContext txnCtx) {
        super(datasetId, resourceId, entityIdFields, txnCtx, lockManager);
    }

    @Override
    public void before(ITupleReference tuple) throws HyracksDataException {
        // This will not be used for a modification operation.
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
        // No reconciled required since the purpose is instantTryLock on PK.
    }

    @Override
    public void cancel(ITupleReference tuple) throws HyracksDataException {
        // No cancel required since reconcile operation is empty.
    }

    @Override
    public void complete(ITupleReference tuple) throws HyracksDataException {
        // No cancel required since reconcile operation is empty.
    }

}
