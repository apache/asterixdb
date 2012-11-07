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
import edu.uci.ics.asterix.transaction.management.service.transaction.FieldsHashValueGenerator;
import edu.uci.ics.asterix.transaction.management.service.transaction.TransactionContext;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryHashFunction;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryHashFunctionFactory;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.ITupleReference;

public abstract class AbstractOperationCallback {
    protected final DatasetId datasetId;
    protected final int[] primaryKeyFields;
    protected final IBinaryHashFunction[] primaryKeyHashFunctions;
    protected final ILockManager lockManager;
    protected final TransactionContext txnCtx;
    protected int transactorLocalNumActiveOperations = 0;

    public AbstractOperationCallback(int datasetId, int[] primaryKeyFields,
            IBinaryHashFunctionFactory[] primaryKeyHashFunctionFactories, TransactionContext txnCtx,
            ILockManager lockManager) {
        this.datasetId = new DatasetId(datasetId);
        this.primaryKeyFields = primaryKeyFields;
        if (primaryKeyHashFunctionFactories != null) {
            this.primaryKeyHashFunctions = new IBinaryHashFunction[primaryKeyHashFunctionFactories.length];
            for (int i = 0; i < primaryKeyHashFunctionFactories.length; ++i) {
                this.primaryKeyHashFunctions[i] = primaryKeyHashFunctionFactories[i].createBinaryHashFunction();
            }
        } else {
            this.primaryKeyHashFunctions = null;
        }
        this.txnCtx = txnCtx;
        this.lockManager = lockManager;
    }

    public int computePrimaryKeyHashValue(ITupleReference tuple, int[] primaryKeyFields,
            IBinaryHashFunction[] primaryKeyHashFunctions) {
        return FieldsHashValueGenerator.computeFieldsHashValue(tuple, primaryKeyFields, primaryKeyHashFunctions);
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
