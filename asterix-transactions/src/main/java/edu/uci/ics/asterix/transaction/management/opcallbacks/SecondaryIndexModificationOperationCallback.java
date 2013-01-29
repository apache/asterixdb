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
import edu.uci.ics.asterix.transaction.management.service.locking.ILockManager;
import edu.uci.ics.asterix.transaction.management.service.logging.IndexLogger;
import edu.uci.ics.asterix.transaction.management.service.transaction.TransactionContext;
import edu.uci.ics.asterix.transaction.management.service.transaction.TransactionSubsystem;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryHashFunctionFactory;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.ITupleReference;
import edu.uci.ics.hyracks.storage.am.common.api.IModificationOperationCallback;
import edu.uci.ics.hyracks.storage.am.common.ophelpers.IndexOperation;

/**
 * Secondary-index modifications do not require any locking.
 * We assume that the modification of the corresponding primary index has already taken an appropriate lock.
 * This callback performs logging of the before and/or after images for secondary indexes.
 */
public class SecondaryIndexModificationOperationCallback extends AbstractOperationCallback implements
        IModificationOperationCallback {

    protected final long resourceId;
    protected final byte resourceType;
    protected final IndexOperation indexOp;
    protected final IndexOperation oldOp;
    protected final TransactionSubsystem txnSubsystem;

    public SecondaryIndexModificationOperationCallback(int datasetId, int[] primaryKeyFields,
            IBinaryHashFunctionFactory[] primaryKeyHashFunctionFactories, TransactionContext txnCtx,
            ILockManager lockManager, TransactionSubsystem txnSubsystem, long resourceId, byte resourceType,
            IndexOperation indexOp) {
        super(datasetId, primaryKeyFields, primaryKeyHashFunctionFactories, txnCtx, lockManager);
        this.resourceId = resourceId;
        this.resourceType = resourceType;
        this.indexOp = indexOp;
        oldOp = (indexOp == IndexOperation.DELETE) ? IndexOperation.INSERT : IndexOperation.DELETE;
        this.txnSubsystem = txnSubsystem;
    }

    @Override
    public void before(ITupleReference tuple) throws HyracksDataException {
        // Do nothing.
    }

    @Override
    public void found(ITupleReference before, ITupleReference after) throws HyracksDataException {
        IndexLogger logger = txnSubsystem.getTreeLoggerRepository().getIndexLogger(resourceId, resourceType);
        int pkHash = computePrimaryKeyHashValue(after, primaryKeyFields, primaryKeyHashFunctions);
        try {
            logger.generateLogRecord(txnSubsystem, txnCtx, datasetId.getId(), pkHash, resourceId, indexOp, after,
                    oldOp, before);
        } catch (ACIDException e) {
            throw new HyracksDataException(e);
        }
    }
}
