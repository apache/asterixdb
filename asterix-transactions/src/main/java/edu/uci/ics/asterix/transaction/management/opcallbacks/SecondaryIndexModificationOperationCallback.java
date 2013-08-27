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

import edu.uci.ics.asterix.common.exceptions.ACIDException;
import edu.uci.ics.asterix.common.transactions.ILockManager;
import edu.uci.ics.asterix.common.transactions.IRecoveryManager.ResourceType;
import edu.uci.ics.asterix.common.transactions.ITransactionContext;
import edu.uci.ics.asterix.common.transactions.ITransactionSubsystem;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.ITupleReference;
import edu.uci.ics.hyracks.storage.am.common.api.IModificationOperationCallback;
import edu.uci.ics.hyracks.storage.am.common.ophelpers.IndexOperation;
import edu.uci.ics.hyracks.storage.am.lsm.btree.tuples.LSMBTreeTupleReference;

/**
 * Secondary-index modifications do not require any locking.
 * We assume that the modification of the corresponding primary index has already taken an appropriate lock.
 * This callback performs logging of the before and/or after images for secondary indexes.
 */
public class SecondaryIndexModificationOperationCallback extends AbstractIndexModificationOperationCallback implements
        IModificationOperationCallback {

    protected final IndexOperation oldOp;

    public SecondaryIndexModificationOperationCallback(int datasetId, int[] primaryKeyFields,
            ITransactionContext txnCtx, ILockManager lockManager, ITransactionSubsystem txnSubsystem, long resourceId,
            byte resourceType, IndexOperation indexOp) {
        super(datasetId, primaryKeyFields, txnCtx, lockManager, txnSubsystem, resourceId, resourceType, indexOp);
        oldOp = (indexOp == IndexOperation.DELETE) ? IndexOperation.INSERT : IndexOperation.DELETE;
    }

    @Override
    public void before(ITupleReference tuple) throws HyracksDataException {
        // Do nothing.
    }

    @Override
    public void found(ITupleReference before, ITupleReference after) throws HyracksDataException {
        try {
            int pkHash = computePrimaryKeyHashValue(after, primaryKeyFields);
            IndexOperation effectiveOldOp;
            if (resourceType == ResourceType.LSM_BTREE) {
                LSMBTreeTupleReference lsmBTreeTuple = (LSMBTreeTupleReference) before;
                if (before == null) {
                    effectiveOldOp = IndexOperation.NOOP;
                } else if (lsmBTreeTuple != null && lsmBTreeTuple.isAntimatter()) {
                    effectiveOldOp = IndexOperation.DELETE;
                } else {
                    effectiveOldOp = IndexOperation.INSERT;
                }
            } else {
                effectiveOldOp = oldOp;
            }
            this.log(pkHash, after, effectiveOldOp, before);
        } catch (ACIDException e) {
            throw new HyracksDataException(e);
        }
    }
}
