/*
 * Copyright 2009-2010 by The Regents of the University of California
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
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryHashFunction;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.ITupleReference;

public abstract class AbstractOperationCallback {
    protected final DatasetId datasetId;
    protected final int[] primaryKeyFields;
    protected final IBinaryHashFunction[] primaryKeyHashFunctions;
    protected final ILockManager lockManager;
    protected final TransactionContext txnCtx;

    public AbstractOperationCallback(DatasetId datasetId, int[] primaryKeyFields,
            IBinaryHashFunction[] primaryKeyHashFunctions, TransactionContext txnCtx, ILockManager lockManager) {
        this.datasetId = datasetId;
        this.primaryKeyFields = primaryKeyFields;
        this.primaryKeyHashFunctions = primaryKeyHashFunctions;
        this.txnCtx = txnCtx;
        this.lockManager = lockManager;
    }

    public int computePrimaryKeyHashValue(ITupleReference tuple, int[] primaryKeyFields,
            IBinaryHashFunction[] primaryKeyHashFunctions) {
        int h = 0;
        for (int i = 0; i < primaryKeyFields.length; i++) {
            int entityFieldIdx = primaryKeyFields[i];
            int fh = primaryKeyHashFunctions[i].hash(tuple.getFieldData(entityFieldIdx),
                    tuple.getFieldStart(entityFieldIdx), tuple.getFieldLength(entityFieldIdx));
            h = h * 31 + fh;
        }
        return h;
    }
}
