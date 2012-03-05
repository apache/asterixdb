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
package edu.uci.ics.asterix.transaction.management.service.recovery;

import edu.uci.ics.asterix.transaction.management.service.logging.PhysicalLogLocator;
import edu.uci.ics.asterix.transaction.management.service.transaction.ITransactionManager;

/**
 * Represents a bookkeeping data-structure that is populated duing the analysis
 * phase of recovery. It contains for each transaction, the transaction state,
 * the LSN corresponding to the last log record written by the transaction and
 * the log record written by the transaction that needs to be undone.
 */
public class TransactionTableEntry {

    private long transactionId;
    private ITransactionManager.TransactionState transactionState;
    private PhysicalLogLocator lastLSN;
    private PhysicalLogLocator undoNextLSN;

    public TransactionTableEntry(long transactionId, ITransactionManager.TransactionState transactionState,
            PhysicalLogLocator lastLSN, PhysicalLogLocator undoNextLSN) {
        this.transactionId = transactionId;
        this.transactionState = transactionState;
        this.lastLSN = lastLSN;
        this.undoNextLSN = undoNextLSN;
    }

    public long getTransactionId() {
        return transactionId;
    }

    public void setTransactionId(long transactionId) {
        this.transactionId = transactionId;
    }

    public ITransactionManager.TransactionState getTransactionState() {
        return transactionState;
    }

    public void setTransactionState(ITransactionManager.TransactionState transactionState) {
        this.transactionState = transactionState;
    }

    public PhysicalLogLocator getLastLSN() {
        return lastLSN;
    }

    public void setLastLSN(PhysicalLogLocator lastLSN) {
        this.lastLSN = lastLSN;
    }

    public PhysicalLogLocator getUndoNextLSN() {
        return undoNextLSN;
    }

    public void setUndoNextLSN(PhysicalLogLocator undoNextLSN) {
        this.undoNextLSN = undoNextLSN;
    }

}
