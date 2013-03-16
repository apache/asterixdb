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

import edu.uci.ics.asterix.transaction.management.service.locking.ILockManager;
import edu.uci.ics.asterix.transaction.management.service.transaction.DatasetId;
import edu.uci.ics.asterix.transaction.management.service.transaction.TransactionContext;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.ITupleReference;
import edu.uci.ics.hyracks.storage.am.bloomfilter.impls.MurmurHash128Bit;

public abstract class AbstractOperationCallback {
    
    private final static long SEED = 0L;
    
    protected final DatasetId datasetId;
    protected final int[] primaryKeyFields;
    protected final ILockManager lockManager;
    protected final TransactionContext txnCtx;
    protected int transactorLocalNumActiveOperations = 0;
    protected final long[] longHashes;

    public AbstractOperationCallback(int datasetId, int[] primaryKeyFields,
            TransactionContext txnCtx, ILockManager lockManager) {
        this.datasetId = new DatasetId(datasetId);
        this.primaryKeyFields = primaryKeyFields;
        this.txnCtx = txnCtx;
        this.lockManager = lockManager;
        this.longHashes= new long[2];
    }

    public int computePrimaryKeyHashValue(ITupleReference tuple, int[] primaryKeyFields) {
        MurmurHash128Bit.hash3_x64_128(tuple, primaryKeyFields, SEED, longHashes);
        return Math.abs((int) longHashes[0]); 
    }

    public TransactionContext getTransactionContext() {
        return txnCtx;
    }

    public int getLocalNumActiveOperations() {
        return transactorLocalNumActiveOperations;
    }

    public void incrementLocalNumActiveOperations() {
        transactorLocalNumActiveOperations++;
    }

    public void decrementLocalNumActiveOperations() {
        transactorLocalNumActiveOperations--;
    }
}
