package edu.uci.ics.asterix.common.transactions;

import edu.uci.ics.hyracks.dataflow.common.data.accessors.ITupleReference;
import edu.uci.ics.hyracks.storage.am.common.ophelpers.IndexOperation;

/**
 * Represents the state of a transaction thread. The state contains information
 * that includes the tuple being operated, the operation and the location of the
 * log record corresponding to the operation.
 */
public class ReusableLogContentObject {

    private LogicalLogLocator logicalLogLocator;
    private IndexOperation newOperation;
    private ITupleReference newValue;
    private IndexOperation oldOperation;
    private ITupleReference oldValue;

    public ReusableLogContentObject(LogicalLogLocator logicalLogLocator, IndexOperation newOperation,
            ITupleReference newValue, IndexOperation oldOperation, ITupleReference oldValue) {
        this.logicalLogLocator = logicalLogLocator;
        this.newOperation = newOperation;
        this.newValue = newValue;
        this.oldOperation = oldOperation;
        this.oldValue = oldValue;
    }

    public synchronized LogicalLogLocator getLogicalLogLocator() {
        return logicalLogLocator;
    }

    public synchronized void setLogicalLogLocator(LogicalLogLocator logicalLogLocator) {
        this.logicalLogLocator = logicalLogLocator;
    }

    public synchronized void setNewOperation(IndexOperation newOperation) {
        this.newOperation = newOperation;
    }

    public synchronized IndexOperation getNewOperation() {
        return newOperation;
    }

    public synchronized void setNewValue(ITupleReference newValue) {
        this.newValue = newValue;
    }

    public synchronized ITupleReference getNewValue() {
        return newValue;
    }

    public synchronized void setOldOperation(IndexOperation oldOperation) {
        this.oldOperation = oldOperation;
    }

    public synchronized IndexOperation getOldOperation() {
        return oldOperation;
    }

    public synchronized void setOldValue(ITupleReference oldValue) {
        this.oldValue = oldValue;
    }

    public synchronized ITupleReference getOldValue() {
        return oldValue;
    }
}
