/*
 * Copyright 2009-2011 by The Regents of the University of California
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
package edu.uci.ics.asterix.transaction.management.service.logging;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import edu.uci.ics.asterix.transaction.management.exception.ACIDException;
import edu.uci.ics.asterix.transaction.management.resource.ICloseable;
import edu.uci.ics.asterix.transaction.management.service.transaction.TransactionContext;
import edu.uci.ics.asterix.transaction.management.service.transaction.TransactionProvider;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.ITupleReference;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndex;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndexTupleWriter;
import edu.uci.ics.hyracks.storage.am.common.ophelpers.IndexOp;

/**
 * Represents a utility class for generating log records corresponding to
 * operations on a ITreeIndex implementation. A TreeLogger instance is thread
 * safe and can be shared across multiple threads that may belong to same or
 * different transactions.
 */
class TransactionState {

    private final Map<Long, TxnThreadState> transactionThreads = new HashMap<Long, TxnThreadState>();

    public synchronized TxnThreadState getTransactionThreadState(long threadId) {
        return transactionThreads.get(threadId);
    }

    public synchronized void putTransactionThreadState(long threadId, TxnThreadState txnThreadState) {
        this.transactionThreads.put(threadId, txnThreadState);
    }

    public synchronized void remove(long threadId) {
        transactionThreads.remove(threadId);
    }
}

/**
 * Represents the state of a transaction thread. The state contains information
 * that includes the tuple being operated, the operation and the location of the
 * log record corresponding to the operation.
 */
class TxnThreadState {

    private ITupleReference tuple;
    private IndexOp indexOperation;
    private LogicalLogLocator logicalLogLocator;

    public TxnThreadState(LogicalLogLocator logicalLogLocator, IndexOp indexOperation, ITupleReference tupleReference) {
        this.tuple = tupleReference;
        this.indexOperation = indexOperation;
        this.logicalLogLocator = logicalLogLocator;
    }

    public synchronized ITupleReference getTuple() {
        return tuple;
    }

    public synchronized void setTuple(ITupleReference tuple) {
        this.tuple = tuple;
    }

    public synchronized IndexOp getIndexOperation() {
        return indexOperation;
    }

    public synchronized void setIndexOperation(IndexOp indexOperation) {
        this.indexOperation = indexOperation;
    }

    public synchronized LogicalLogLocator getLogicalLogLocator() {
        return logicalLogLocator;
    }

    public synchronized void setLogicalLogLocator(LogicalLogLocator logicalLogLocator) {
        this.logicalLogLocator = logicalLogLocator;
    }

}

public class TreeLogger implements ILogger, ICloseable {

    private static final byte resourceMgrId = TreeResourceManager.ID;
    private final Map<Object, Object> arguments = new ConcurrentHashMap<Object, Object>();

    public static final String TREE_INDEX = "TREE_INDEX";
    public static final String TUPLE_REFERENCE = "TUPLE_REFERENCE";
    public static final String TUPLE_WRITER = "TUPLE_WRITER";
    public static final String INDEX_OPERATION = "INDEX_OPERATION";
    public static final String RESOURCE_ID = "RESOURCE_ID";

    private final ITreeIndex treeIndex;
    private final ITreeIndexTupleWriter treeIndexTupleWriter;
    private final byte[] resourceIdBytes;
    private final byte[] resourceIdLengthBytes;

    public class BTreeOperationCodes {
        public static final byte INSERT = 0;
        public static final byte DELETE = 1;
    }

    public TreeLogger(byte[] resourceIdBytes, ITreeIndex treeIndex) {
        this.resourceIdBytes = resourceIdBytes;
        this.treeIndex = treeIndex;
        treeIndexTupleWriter = treeIndex.getLeafFrameFactory().getTupleWriterFactory().createTupleWriter();
        this.resourceIdLengthBytes = DataUtil.intToByteArray(resourceIdBytes.length);
    }

    public synchronized void close(TransactionContext context) {
        TransactionState txnState = (TransactionState) arguments.get(context.getJobId());
        txnState.remove(Thread.currentThread().getId());
        arguments.remove(context.getJobId());
    }

    public void generateLogRecord(TransactionProvider provider, TransactionContext context, IndexOp operation,
            ITupleReference tuple) throws ACIDException {
        context.addCloseableResource(this); // the close method would be called
        // on this TreeLogger instance at
        // the time of transaction
        // commit/abort.
        if (operation != IndexOp.INSERT && operation != IndexOp.DELETE) {
            throw new ACIDException("Loging for Operation " + operation + " not supported");

        }

        TxnThreadState txnThreadState = null;
        TransactionState txnState;
        txnState = (TransactionState) arguments.get(context.getJobId());
        if (txnState == null) {
            synchronized (context) { // threads belonging to different
                // transaction do not need to
                // synchronize amongst them.
                if (txnState == null) {
                    txnState = new TransactionState();
                    arguments.put(context.getJobId(), txnState);
                }
            }
        }

        txnThreadState = txnState.getTransactionThreadState(Thread.currentThread().getId());
        if (txnThreadState == null) {
            LogicalLogLocator logicalLogLocator = LogUtil.getDummyLogicalLogLocator(provider.getLogManager());
            txnThreadState = new TxnThreadState(logicalLogLocator, operation, tuple);
            txnState.putTransactionThreadState(Thread.currentThread().getId(), txnThreadState);
        }
        txnThreadState.setIndexOperation(operation);
        txnThreadState.setTuple(tuple);
        int tupleSize = treeIndexTupleWriter.bytesRequired(tuple);
        // Below 4 is for the int representing the length of resource id and 1
        // is for
        // the byte representing the operation
        int logContentLength = 4 + resourceIdBytes.length + 1 + tupleSize;
        provider.getLogManager().log(txnThreadState.getLogicalLogLocator(), context, resourceMgrId, 0L, LogType.UPDATE,
                LogActionType.REDO_UNDO, logContentLength, (ILogger) this, arguments);
    }

    @Override
    public void log(TransactionContext context, LogicalLogLocator logicalLogLocator, int logRecordSize,
            Map<Object, Object> loggerArguments) throws ACIDException {
        TransactionState txnState = (TransactionState) loggerArguments.get(context.getJobId());
        TxnThreadState state = (TxnThreadState) txnState.getTransactionThreadState(Thread.currentThread().getId());
        int count = 0;
        byte[] logBuffer = logicalLogLocator.getBuffer().getArray();
        System.arraycopy(resourceIdLengthBytes, 0, logBuffer, logicalLogLocator.getMemoryOffset(), 4);
        count += 4; // count is incremented by 4 because we wrote the length
                    // that is an int and hence 4 bytes
        System.arraycopy(resourceIdBytes, 0, logBuffer, logicalLogLocator.getMemoryOffset() + count,
                resourceIdBytes.length);
        count += resourceIdBytes.length;
        logBuffer[logicalLogLocator.getMemoryOffset() + count] = (byte) state.getIndexOperation().ordinal();
        count += 1; // count is incremented by 1 to account for the byte
                    // written.
        treeIndexTupleWriter.writeTuple(state.getTuple(), logicalLogLocator.getBuffer().getArray(),
                logicalLogLocator.getMemoryOffset() + count);
    }

    @Override
    public void postLog(TransactionContext context, Map<Object, Object> loggerArguments) throws ACIDException {
    }

    @Override
    public void preLog(TransactionContext context, Map<Object, Object> loggerArguments) throws ACIDException {
    }

}
