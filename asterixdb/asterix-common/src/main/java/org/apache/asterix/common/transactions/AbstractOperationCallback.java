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

package org.apache.asterix.common.transactions;

import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;
import org.apache.hyracks.storage.am.bloomfilter.impls.MurmurHash128Bit;

public abstract class AbstractOperationCallback {

    private final static long SEED = 0L;

    protected final DatasetId datasetId;
    protected final int[] primaryKeyFields;
    protected final ITransactionContext txnCtx;
    protected final ILockManager lockManager;
    protected final long[] longHashes;
    protected final long resourceId;

    public AbstractOperationCallback(DatasetId datasetId, long resourceId, int[] primaryKeyFields,
            ITransactionContext txnCtx, ILockManager lockManager) {
        this.datasetId = datasetId;
        this.resourceId = resourceId;
        this.primaryKeyFields = primaryKeyFields;
        this.txnCtx = txnCtx;
        this.lockManager = lockManager;
        longHashes = new long[2];
    }

    public int computePrimaryKeyHashValue(ITupleReference tuple, int[] primaryKeyFields) {
        MurmurHash128Bit.hash3_x64_128(tuple, primaryKeyFields, SEED, longHashes);
        return Math.abs((int) longHashes[0]);
    }

    public void beforeOperation() {
        txnCtx.beforeOperation(resourceId);
    }

    public void afterOperation() {
        txnCtx.afterOperation(resourceId);
    }
}
