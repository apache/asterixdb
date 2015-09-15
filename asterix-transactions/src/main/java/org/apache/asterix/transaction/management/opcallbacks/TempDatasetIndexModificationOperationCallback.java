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

import org.apache.asterix.common.transactions.ILockManager;
import org.apache.asterix.common.transactions.ITransactionContext;
import org.apache.asterix.common.transactions.ITransactionSubsystem;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;
import org.apache.hyracks.storage.am.common.api.IModificationOperationCallback;
import org.apache.hyracks.storage.am.common.ophelpers.IndexOperation;

/**
 * This class is the operation callback for temporary datasets.
 * A temporary dataset does not require any lock and does not generate any write-ahead update and commit log
 * but generates flush log and job commit log.
 * The "before" and "found" method in this callback is empty so that no locking is requested for accessing a temporary
 * dataset and no write-ahead log is written for update operations.
 */
public class TempDatasetIndexModificationOperationCallback extends AbstractIndexModificationOperationCallback implements
        IModificationOperationCallback {

    public TempDatasetIndexModificationOperationCallback(int datasetId, int[] primaryKeyFields,
            ITransactionContext txnCtx, ILockManager lockManager, ITransactionSubsystem txnSubsystem, long resourceId,
            byte resourceType, IndexOperation indexOp) {
        super(datasetId, primaryKeyFields, txnCtx, lockManager, txnSubsystem, resourceId, resourceType, indexOp);
    }

    @Override
    public void before(ITupleReference tuple) throws HyracksDataException {

    }

    @Override
    public void found(ITupleReference before, ITupleReference after) throws HyracksDataException {

    }
}
