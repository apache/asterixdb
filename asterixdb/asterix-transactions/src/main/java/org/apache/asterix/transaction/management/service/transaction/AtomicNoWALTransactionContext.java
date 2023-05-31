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
package org.apache.asterix.transaction.management.service.transaction;

import java.util.ArrayList;
import java.util.List;

import org.apache.asterix.common.context.PrimaryIndexOperationTracker;
import org.apache.asterix.common.dataflow.LSMIndexUtil;
import org.apache.asterix.common.exceptions.ACIDException;
import org.apache.asterix.common.transactions.ITransactionManager;
import org.apache.asterix.common.transactions.LogRecord;
import org.apache.asterix.common.transactions.TxnId;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMOperationTracker;
import org.apache.hyracks.storage.am.lsm.common.impls.FlushOperation;
import org.apache.hyracks.util.annotations.ThreadSafe;

@ThreadSafe
public class AtomicNoWALTransactionContext extends AtomicTransactionContext {

    public AtomicNoWALTransactionContext(TxnId txnId) {
        super(txnId);
    }

    @Override
    public void cleanup() {
        super.cleanup();
        final int txnState = getTxnState();
        switch (txnState) {
            case ITransactionManager.ABORTED:
                deleteUncommittedRecords();
                break;
            case ITransactionManager.COMMITTED:
                ensureDurable();
                break;
            default:
                throw new IllegalStateException("invalid state in txn clean up: " + getTxnState());
        }
    }

    private void deleteUncommittedRecords() {
        for (ILSMOperationTracker opTrackerRef : modifiedIndexes) {
            PrimaryIndexOperationTracker primaryIndexOpTracker = (PrimaryIndexOperationTracker) opTrackerRef;
            try {
                primaryIndexOpTracker.deleteMemoryComponent();
            } catch (HyracksDataException e) {
                throw new ACIDException(e);
            }
        }
    }

    private void ensureDurable() {
        List<FlushOperation> flushes = new ArrayList<>();
        LogRecord dummyLogRecord = new LogRecord();
        try {
            for (ILSMOperationTracker opTrackerRef : modifiedIndexes) {
                PrimaryIndexOperationTracker primaryIndexOpTracker = (PrimaryIndexOperationTracker) opTrackerRef;
                primaryIndexOpTracker.triggerScheduleFlush(dummyLogRecord);
                flushes.addAll(primaryIndexOpTracker.getScheduledFlushes());
            }
            LSMIndexUtil.waitFor(flushes);
        } catch (HyracksDataException e) {
            throw new ACIDException(e);
        }
    }

    @Override
    public boolean hasWAL() {
        return false;
    }
}
