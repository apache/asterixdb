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
import edu.uci.ics.asterix.transaction.management.service.transaction.IResourceManager.ResourceType;
import edu.uci.ics.asterix.transaction.management.service.transaction.TransactionContext;
import edu.uci.ics.asterix.transaction.management.service.transaction.TransactionSubsystem;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.ITupleReference;
import edu.uci.ics.hyracks.storage.am.common.api.IIndex;
import edu.uci.ics.hyracks.storage.am.common.ophelpers.IndexOperation;
import edu.uci.ics.hyracks.storage.am.common.tuples.SimpleTupleWriter;

public class IndexLogger implements ILogger, ICloseable {

    private final Map<Object, Object> jobId2ReusableLogContentObjectRepositoryMap = new ConcurrentHashMap<Object, Object>();

    public static final String TREE_INDEX = "TREE_INDEX";
    public static final String TUPLE_REFERENCE = "TUPLE_REFERENCE";
    public static final String TUPLE_WRITER = "TUPLE_WRITER";
    public static final String INDEX_OPERATION = "INDEX_OPERATION";
    public static final String RESOURCE_ID = "RESOURCE_ID";

    private final long resourceId;
    private final byte resourceType;
    private final SimpleTupleWriter tupleWriter;

    public class BTreeOperationCodes {
        public static final byte INSERT = 0;
        public static final byte DELETE = 1;
    }

    public IndexLogger(long resourceId, byte resourceType, IIndex index) {
        this.resourceId = resourceId;
        this.resourceType = resourceType;
        this.tupleWriter = new SimpleTupleWriter();
    }

    public synchronized void close(TransactionContext context) {
        ReusableLogContentObjectRepository txnThreadStateRepository = (ReusableLogContentObjectRepository) jobId2ReusableLogContentObjectRepositoryMap
                .get(context.getJobId());
        txnThreadStateRepository.remove(Thread.currentThread().getId());
        jobId2ReusableLogContentObjectRepositoryMap.remove(context.getJobId());
    }

    public void generateLogRecord(TransactionSubsystem txnSubsystem, TransactionContext context, int datasetId,
            int PKHashValue, long resourceId, IndexOperation newOperation, ITupleReference newValue,
            IndexOperation oldOperation, ITupleReference oldValue) throws ACIDException {

        if (this.resourceId != resourceId) {
            throw new ACIDException("IndexLogger mistach");
        }

        context.addCloseableResource(this); // the close method would be called
        // on this TreeLogger instance at
        // the time of transaction
        // commit/abort.
        if (newOperation != IndexOperation.INSERT && newOperation != IndexOperation.DELETE) {
            throw new ACIDException("Loging for Operation " + newOperation + " not supported");
        }

        ReusableLogContentObject reusableLogContentObject = null;
        ReusableLogContentObjectRepository reusableLogContentObjectRepository = null;
        reusableLogContentObjectRepository = (ReusableLogContentObjectRepository) jobId2ReusableLogContentObjectRepositoryMap
                .get(context.getJobId());
        if (reusableLogContentObjectRepository == null) {
            synchronized (context) { // threads belonging to different
                // transaction do not need to
                // synchronize amongst them.
                if (reusableLogContentObjectRepository == null) {
                    reusableLogContentObjectRepository = new ReusableLogContentObjectRepository();
                    jobId2ReusableLogContentObjectRepositoryMap.put(context.getJobId(),
                            reusableLogContentObjectRepository);
                }
            }
        }

        reusableLogContentObject = reusableLogContentObjectRepository.getObject(Thread.currentThread().getId());
        if (reusableLogContentObject == null) {
            LogicalLogLocator logicalLogLocator = LogUtil.getDummyLogicalLogLocator(txnSubsystem.getLogManager());
            reusableLogContentObject = new ReusableLogContentObject(logicalLogLocator, newOperation, newValue,
                    oldOperation, oldValue);
            reusableLogContentObjectRepository.putObject(Thread.currentThread().getId(), reusableLogContentObject);
        } else {
            reusableLogContentObject.setNewOperation(newOperation);
            reusableLogContentObject.setNewValue(newValue);
            reusableLogContentObject.setOldOperation(oldOperation);
            reusableLogContentObject.setOldValue(oldValue);
        }

        int logContentSize = 4/*TupleFieldCount*/+ 1/*NewOperation*/+ 4/*newValueLength*/;
        if (newValue != null) {
            logContentSize += tupleWriter.bytesRequired(newValue);
        }

        logContentSize += 1/*OldOperation*/+ 4/*oldValueLength*/;
        if (oldValue != null) {
            logContentSize += tupleWriter.bytesRequired(oldValue);
        }

        txnSubsystem.getLogManager().log(LogType.UPDATE, context, datasetId, PKHashValue, resourceId, resourceType,
                logContentSize, reusableLogContentObject, this, reusableLogContentObject.getLogicalLogLocator());
    }

    @Override
    public void log(TransactionContext context, LogicalLogLocator logicalLogLocator, int logContentSize,
            ReusableLogContentObject reusableLogContentObject) throws ACIDException {
        int offset = 0;
        int tupleSize = 0;

        //tuple field count
        (logicalLogLocator.getBuffer()).writeInt(logicalLogLocator.getMemoryOffset() + offset, reusableLogContentObject
                .getNewValue().getFieldCount());
        offset += 4;

        //new operation
        (logicalLogLocator.getBuffer()).put(logicalLogLocator.getMemoryOffset() + offset,
                (byte) reusableLogContentObject.getNewOperation().ordinal());
        offset += 1;

        //new tuple size
        if (reusableLogContentObject.getNewValue() != null) {
            tupleSize = tupleWriter.bytesRequired(reusableLogContentObject.getNewValue());
        }
        (logicalLogLocator.getBuffer()).writeInt(logicalLogLocator.getMemoryOffset() + offset, tupleSize);
        offset += 4;

        //new tuple
        if (tupleSize != 0) {
            tupleWriter.writeTuple(reusableLogContentObject.getNewValue(), logicalLogLocator.getBuffer().getArray(),
                    logicalLogLocator.getMemoryOffset() + offset);
            offset += tupleSize;
        }

        if (resourceType == ResourceType.LSM_BTREE) {
            //old operation
            (logicalLogLocator.getBuffer()).put(logicalLogLocator.getMemoryOffset() + offset,
                    (byte) reusableLogContentObject.getOldOperation().ordinal());
            offset += 1;

            if (reusableLogContentObject.getOldOperation() != IndexOperation.NOOP) {
                //old tuple size
                if (reusableLogContentObject.getOldValue() != null) {
                    tupleSize = tupleWriter.bytesRequired(reusableLogContentObject.getOldValue());
                } else {
                    tupleSize = 0;
                }
                (logicalLogLocator.getBuffer()).writeInt(logicalLogLocator.getMemoryOffset() + offset, tupleSize);
                offset += 4;

                if (tupleSize != 0) {
                    //old tuple
                    tupleWriter.writeTuple(reusableLogContentObject.getOldValue(), logicalLogLocator.getBuffer()
                            .getArray(), logicalLogLocator.getMemoryOffset() + offset);
                }
            }
        }
    }

    @Override
    public void postLog(TransactionContext context, ReusableLogContentObject reusableLogContentObject)
            throws ACIDException {
    }

    @Override
    public void preLog(TransactionContext context, ReusableLogContentObject reusableLogContentObject)
            throws ACIDException {
    }

    /**
     * Represents a utility class for generating log records corresponding to
     * operations on a ITreeIndex implementation. A TreeLogger instance is thread
     * safe and can be shared across multiple threads that may belong to same or
     * different transactions.
     */
    public class ReusableLogContentObjectRepository {

        private final Map<Long, ReusableLogContentObject> id2Object = new HashMap<Long, ReusableLogContentObject>();

        public synchronized ReusableLogContentObject getObject(long threadId) {
            return id2Object.get(threadId);
        }

        public synchronized void putObject(long threadId, ReusableLogContentObject reusableLogContentObject) {
            this.id2Object.put(threadId, reusableLogContentObject);
        }

        public synchronized void remove(long threadId) {
            id2Object.remove(threadId);
        }
    }

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

}
