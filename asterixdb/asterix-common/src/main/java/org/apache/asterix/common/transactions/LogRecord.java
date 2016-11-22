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

import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.zip.CRC32;

import org.apache.asterix.common.context.PrimaryIndexOperationTracker;
import org.apache.asterix.common.replication.IReplicationThread;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;
import org.apache.hyracks.storage.am.common.tuples.SimpleTupleReference;
import org.apache.hyracks.storage.am.common.tuples.SimpleTupleWriter;

/**
 * == LogRecordFormat ==
 * ---------------------------
 * [Header1] (6 bytes) : for all log types
 * LogSource(1)
 * LogType(1)
 * JobId(4)
 * ---------------------------
 * [Header2] (16 bytes + PKValueSize) : for entity_commit, upsert_entity_commit, and update log types
 * DatasetId(4) //stored in dataset_dataset in Metadata Node
 * ResourcePartition(4)
 * PKHashValue(4)
 * PKValueSize(4)
 * PKValue(PKValueSize)
 * ---------------------------
 * [Header3] (12 bytes) : only for update log type
 * ResourceId(8) //stored in .metadata of the corresponding index in NC node
 * LogRecordSize(4)
 * ---------------------------
 * [Body] (9 bytes + NewValueSize) : only for update log type
 * FieldCnt(4)
 * NewOp(1)
 * NewValueSize(4)
 * NewValue(NewValueSize)
 * ---------------------------
 * [Tail] (8 bytes) : for all log types
 * Checksum(8)
 * ---------------------------
 * = LogSize =
 * 1) JOB_COMMIT_LOG_SIZE: 14 bytes (Header1(6) + Tail(8))
 * 2) ENTITY_COMMIT || UPSERT_ENTITY_COMMIT: (Header1(6) + Header2(16) + Tail(8)) + PKValueSize
 * --> ENTITY_COMMIT_LOG_BASE_SIZE = 30
 * 3) UPDATE: (Header1(6) + Header2(16) + + Header3(20) + Body(9) + Tail(8)) + PKValueSize + NewValueSize
 * --> UPDATE_LOG_BASE_SIZE = 59
 * 4) FLUSH: 18 bytes (Header1(6) + DatasetId(4) + Tail(8))
 * 5) WAIT_LOG_SIZE: 14 bytes (Header1(6) + Tail(8))
 * --> WAIT_LOG only requires LogType Field, but in order to conform the log reader protocol
 * it also includes LogSource and JobId fields.
 */

public class LogRecord implements ILogRecord {

    // ------------- fields in a log record (begin) ------------//
    private byte logSource;
    private byte logType;
    private int jobId;
    private int datasetId;
    private int PKHashValue;
    private int PKValueSize;
    private ITupleReference PKValue;
    private long resourceId;
    private int resourcePartition;
    private int logSize;
    private int newValueFieldCount;
    private byte newOp;
    private int newValueSize;
    private ITupleReference newValue;
    private int oldValueSize;
    private ITupleReference oldValue;
    private int oldValueFieldCount;
    private long checksum;
    private long prevMarkerLSN;
    private ByteBuffer marker;
    // ------------- fields in a log record (end) --------------//
    private final ILogMarkerCallback callback; // A callback for log mark operations
    private int PKFieldCnt;
    private ITransactionContext txnCtx;
    private long LSN;
    private final AtomicBoolean isFlushed;
    private final PrimaryKeyTupleReference readPKValue;
    private final SimpleTupleReference readNewValue;
    private final SimpleTupleReference readOldValue;
    private final CRC32 checksumGen;
    private int[] PKFields;
    private PrimaryIndexOperationTracker opTracker;
    private IReplicationThread replicationThread;
    /**
     * The fields (numOfFlushedIndexes and nodeId) are used for remote flush logs only
     * to indicate the source of the log and how many indexes were flushed using its LSN.
     */
    private int numOfFlushedIndexes;
    private String nodeId;
    private boolean replicated = false;

    public LogRecord(ILogMarkerCallback callback) {
        this.callback = callback;
        isFlushed = new AtomicBoolean(false);
        readPKValue = new PrimaryKeyTupleReference();
        readNewValue = SimpleTupleWriter.INSTANCE.createTupleReference();
        readOldValue = SimpleTupleWriter.INSTANCE.createTupleReference();
        checksumGen = new CRC32();
        logSource = LogSource.LOCAL;
    }

    public LogRecord() {
        this(null);
    }

    private void doWriteLogRecord(ByteBuffer buffer) {
        buffer.put(logSource);
        buffer.put(logType);
        buffer.putInt(jobId);
        switch (logType) {
            case LogType.ENTITY_COMMIT:
            case LogType.UPSERT_ENTITY_COMMIT:
                writeEntityInfo(buffer);
                break;
            case LogType.UPDATE:
                writeEntityInfo(buffer);
                buffer.putLong(resourceId);
                buffer.putInt(logSize);
                buffer.putInt(newValueFieldCount);
                buffer.put(newOp);
                buffer.putInt(newValueSize);
                writeTuple(buffer, newValue, newValueSize);
                if (oldValueSize > 0) {
                    buffer.putInt(oldValueSize);
                    buffer.putInt(oldValueFieldCount);
                    writeTuple(buffer, oldValue, oldValueSize);
                }
                break;
            case LogType.FLUSH:
                buffer.putInt(datasetId);
                break;
            case LogType.MARKER:
                buffer.putInt(datasetId);
                buffer.putInt(resourcePartition);
                callback.before(buffer);
                buffer.putInt(logSize);
                buffer.put(marker);
                break;
            default:
                // Do nothing
        }
    }

    private void writeEntityInfo(ByteBuffer buffer) {
        buffer.putInt(resourcePartition);
        buffer.putInt(datasetId);
        buffer.putInt(PKHashValue);
        if (PKValueSize <= 0) {
            throw new IllegalStateException("Primary Key Size is less than or equal to 0");
        }
        buffer.putInt(PKValueSize);
        writePKValue(buffer);
    }

    @Override
    public void writeLogRecord(ByteBuffer buffer) {
        int beginOffset = buffer.position();
        doWriteLogRecord(buffer);
        checksum = generateChecksum(buffer, beginOffset, logSize - CHKSUM_LEN);
        buffer.putLong(checksum);
    }

    @Override
    public void writeRemoteLogRecord(ByteBuffer buffer) {
        doWriteLogRecord(buffer);
        if (logType == LogType.FLUSH) {
            buffer.putLong(LSN);
            buffer.putInt(numOfFlushedIndexes);
            buffer.putInt(nodeId.length());
            buffer.put(nodeId.getBytes());
        }
    }

    private void writePKValue(ByteBuffer buffer) {
        if (logSource == LogSource.LOCAL) {
            for (int i = 0; i < PKFieldCnt; i++) {
                buffer.put(PKValue.getFieldData(0), PKValue.getFieldStart(PKFields[i]),
                        PKValue.getFieldLength(PKFields[i]));
            }
        } else {
            // since PKValue is already serialized in remote logs, just put it into buffer
            buffer.put(PKValue.getFieldData(0), 0, PKValueSize);
        }
    }

    private void writeTuple(ByteBuffer buffer, ITupleReference tuple, int size) {
        SimpleTupleWriter.INSTANCE.writeTuple(tuple, buffer.array(), buffer.position());
        buffer.position(buffer.position() + size);
    }

    private long generateChecksum(ByteBuffer buffer, int offset, int len) {
        checksumGen.reset();
        checksumGen.update(buffer.array(), offset, len);
        return checksumGen.getValue();
    }

    @Override
    public RecordReadStatus readLogRecord(ByteBuffer buffer) {
        int beginOffset = buffer.position();

        //read common fields
        RecordReadStatus status = doReadLogRecord(buffer);
        if (status != RecordReadStatus.OK) {
            buffer.position(beginOffset);
            return status;
        }

        // attempt to read checksum
        if (buffer.remaining() < CHKSUM_LEN) {
            buffer.position(beginOffset);
            return RecordReadStatus.TRUNCATED;
        }
        checksum = buffer.getLong();
        if (checksum != generateChecksum(buffer, beginOffset, logSize - CHKSUM_LEN)) {
            return RecordReadStatus.BAD_CHKSUM;
        }

        return RecordReadStatus.OK;
    }

    private RecordReadStatus doReadLogRecord(ByteBuffer buffer) {
        //first we need the logtype and Job ID, if the buffer isn't that big, then no dice.
        if (buffer.remaining() < ALL_RECORD_HEADER_LEN) {
            return RecordReadStatus.TRUNCATED;
        }
        logSource = buffer.get();
        logType = buffer.get();
        jobId = buffer.getInt();
        switch (logType) {
            case LogType.FLUSH:
                if (buffer.remaining() < DatasetId.BYTES) {
                    return RecordReadStatus.TRUNCATED;
                }
                datasetId = buffer.getInt();
                resourceId = 0l;
                // fall throuh
            case LogType.WAIT:
                computeAndSetLogSize();
                break;
            case LogType.JOB_COMMIT:
            case LogType.ABORT:
                datasetId = -1;
                PKHashValue = -1;
                computeAndSetLogSize();
                break;
            case LogType.ENTITY_COMMIT:
            case LogType.UPSERT_ENTITY_COMMIT:
                if (readEntityInfo(buffer)) {
                    computeAndSetLogSize();
                } else {
                    return RecordReadStatus.TRUNCATED;
                }
                break;
            case LogType.UPDATE:
                if (readEntityInfo(buffer)) {
                    if (buffer.remaining() < UPDATE_LSN_HEADER + UPDATE_BODY_HEADER) {
                        return RecordReadStatus.TRUNCATED;
                    }
                    resourceId = buffer.getLong();
                    logSize = buffer.getInt();
                    newValueFieldCount = buffer.getInt();
                    newOp = buffer.get();
                    newValueSize = buffer.getInt();
                    if (buffer.remaining() < newValueSize) {
                        if (logSize > buffer.capacity()) {
                            return RecordReadStatus.LARGE_RECORD;
                        }
                        return RecordReadStatus.TRUNCATED;
                    }
                    newValue = readTuple(buffer, readNewValue, newValueFieldCount, newValueSize);
                    if (logSize > getUpdateLogSizeWithoutOldValue()) {
                        // Prev Image exists
                        if (buffer.remaining() < Integer.BYTES) {
                            return RecordReadStatus.TRUNCATED;
                        }
                        oldValueSize = buffer.getInt();
                        if (buffer.remaining() < Integer.BYTES) {
                            return RecordReadStatus.TRUNCATED;
                        }
                        oldValueFieldCount = buffer.getInt();
                        if (buffer.remaining() < oldValueSize) {
                            return RecordReadStatus.TRUNCATED;
                        }
                        oldValue = readTuple(buffer, readOldValue, oldValueFieldCount, oldValueSize);
                    } else {
                        oldValueSize = 0;
                    }
                } else {
                    return RecordReadStatus.TRUNCATED;
                }
                break;
            case LogType.MARKER:
                if (buffer.remaining() < DS_LEN + RS_PARTITION_LEN + PRVLSN_LEN + LOGRCD_SZ_LEN) {
                    return RecordReadStatus.TRUNCATED;
                }
                datasetId = buffer.getInt();
                resourcePartition = buffer.getInt();
                prevMarkerLSN = buffer.getLong();
                logSize = buffer.getInt();
                int lenRemaining = logSize - MARKER_BASE_LOG_SIZE;
                if (buffer.remaining() < lenRemaining) {
                    return RecordReadStatus.TRUNCATED;
                }

                if (marker == null || marker.capacity() < lenRemaining) {
                    // TODO(amoudi): account for memory allocation
                    marker = ByteBuffer.allocate(lenRemaining + Short.BYTES);
                }
                marker.clear();
                buffer.get(marker.array(), 0, lenRemaining);
                marker.position(lenRemaining);
                marker.flip();
                break;
            default:
                break;
        }
        return RecordReadStatus.OK;
    }

    private boolean readEntityInfo(ByteBuffer buffer) {
        //attempt to read in the resourcePartition, dsid, PK hash and PK length
        if (buffer.remaining() < ENTITYCOMMIT_UPDATE_HEADER_LEN) {
            return false;
        }
        resourcePartition = buffer.getInt();
        datasetId = buffer.getInt();
        PKHashValue = buffer.getInt();
        PKValueSize = buffer.getInt();
        // attempt to read in the PK
        if (buffer.remaining() < PKValueSize) {
            return false;
        }
        if (PKValueSize <= 0) {
            throw new IllegalStateException("Primary Key Size is less than or equal to 0");
        }
        PKValue = readPKValue(buffer);
        return true;
    }

    @Override
    public void readRemoteLog(ByteBuffer buffer) {
        //read common fields
        doReadLogRecord(buffer);

        if (logType == LogType.FLUSH) {
            LSN = buffer.getLong();
            numOfFlushedIndexes = buffer.getInt();
            //read serialized node id
            int nodeIdLength = buffer.getInt();
            byte[] nodeIdBytes = new byte[nodeIdLength];
            buffer.get(nodeIdBytes);
            nodeId = new String(nodeIdBytes);
        }
    }

    private ITupleReference readPKValue(ByteBuffer buffer) {
        if (buffer.position() + PKValueSize > buffer.limit()) {
            throw new BufferUnderflowException();
        }
        readPKValue.reset(buffer.array(), buffer.position(), PKValueSize);
        buffer.position(buffer.position() + PKValueSize);
        return readPKValue;
    }

    private static ITupleReference readTuple(ByteBuffer srcBuffer, SimpleTupleReference destTuple, int fieldCnt,
            int size) {
        if (srcBuffer.position() + size > srcBuffer.limit()) {
            throw new BufferUnderflowException();
        }
        destTuple.setFieldCount(fieldCnt);
        destTuple.resetByTupleOffset(srcBuffer, srcBuffer.position());
        srcBuffer.position(srcBuffer.position() + size);
        return destTuple;
    }

    @Override
    public void computeAndSetPKValueSize() {
        int i;
        PKValueSize = 0;
        for (i = 0; i < PKFieldCnt; i++) {
            PKValueSize += PKValue.getFieldLength(PKFields[i]);
        }
    }

    private void setUpdateLogSize() {
        logSize = getUpdateLogSizeWithoutOldValue();
        if (oldValueSize > 0) {
            logSize += /*size*/Integer.BYTES + /*fieldCount*/Integer.BYTES + /*tuple*/oldValueSize;
        }
    }

    private int getUpdateLogSizeWithoutOldValue() {
        return UPDATE_LOG_BASE_SIZE + PKValueSize + newValueSize;
    }

    @Override
    public void computeAndSetLogSize() {
        switch (logType) {
            case LogType.UPDATE:
                setUpdateLogSize();
                break;
            case LogType.JOB_COMMIT:
            case LogType.ABORT:
                logSize = JOB_TERMINATE_LOG_SIZE;
                break;
            case LogType.ENTITY_COMMIT:
            case LogType.UPSERT_ENTITY_COMMIT:
                logSize = ENTITY_COMMIT_LOG_BASE_SIZE + PKValueSize;
                break;
            case LogType.FLUSH:
                logSize = FLUSH_LOG_SIZE;
                break;
            case LogType.WAIT:
                logSize = WAIT_LOG_SIZE;
                break;
            case LogType.MARKER:
                setMarkerLogSize();
                break;
            default:
                throw new IllegalStateException("Unsupported Log Type");
        }
    }

    private void setMarkerLogSize() {
        logSize = MARKER_BASE_LOG_SIZE + marker.remaining();
    }

    @Override
    public String getLogRecordForDisplay() {
        StringBuilder builder = new StringBuilder();
        builder.append(" Source : ").append(LogSource.toString(logSource));
        builder.append(" LSN : ").append(LSN);
        builder.append(" LogType : ").append(LogType.toString(logType));
        builder.append(" LogSize : ").append(logSize);
        builder.append(" JobId : ").append(jobId);
        if (logType == LogType.ENTITY_COMMIT || logType == LogType.UPSERT_ENTITY_COMMIT || logType == LogType.UPDATE) {
            builder.append(" DatasetId : ").append(datasetId);
            builder.append(" ResourcePartition : ").append(resourcePartition);
            builder.append(" PKHashValue : ").append(PKHashValue);
            builder.append(" PKFieldCnt : ").append(PKFieldCnt);
            builder.append(" PKSize: ").append(PKValueSize);
        }
        if (logType == LogType.UPDATE) {
            builder.append(" ResourceId : ").append(resourceId);
        }
        return builder.toString();
    }

    ////////////////////////////////////////////
    // getter and setter methods
    ////////////////////////////////////////////

    @Override
    public ITransactionContext getTxnCtx() {
        return txnCtx;
    }

    @Override
    public void setTxnCtx(ITransactionContext txnCtx) {
        this.txnCtx = txnCtx;
    }

    @Override
    public boolean isFlushed() {
        return isFlushed.get();
    }

    @Override
    public void isFlushed(boolean isFlushed) {
        this.isFlushed.set(isFlushed);
    }

    @Override
    public byte getLogType() {
        return logType;
    }

    @Override
    public void setLogType(byte logType) {
        this.logType = logType;
    }

    @Override
    public int getJobId() {
        return jobId;
    }

    @Override
    public void setJobId(int jobId) {
        this.jobId = jobId;
    }

    @Override
    public int getDatasetId() {
        return datasetId;
    }

    @Override
    public void setDatasetId(int datasetId) {
        this.datasetId = datasetId;
    }

    @Override
    public int getPKHashValue() {
        return PKHashValue;
    }

    @Override
    public void setPKHashValue(int PKHashValue) {
        this.PKHashValue = PKHashValue;
    }

    @Override
    public long getResourceId() {
        return resourceId;
    }

    @Override
    public void setResourceId(long resourceId) {
        this.resourceId = resourceId;
    }

    @Override
    public int getLogSize() {
        return logSize;
    }

    @Override
    public int getRemoteLogSize() {
        int remoteLogSize = logSize;
        if (logType == LogType.FLUSH) {
            //LSN
            remoteLogSize += Long.BYTES;
            //num of indexes
            remoteLogSize += Integer.BYTES;
            //serialized node id String
            remoteLogSize += Integer.BYTES + nodeId.length();
        }
        remoteLogSize -= CHKSUM_LEN;
        return remoteLogSize;
    }

    @Override
    public void setLogSize(int logSize) {
        this.logSize = logSize;
    }

    @Override
    public byte getNewOp() {
        return newOp;
    }

    @Override
    public void setNewOp(byte newOp) {
        this.newOp = newOp;
    }

    @Override
    public void setNewValueSize(int newValueSize) {
        this.newValueSize = newValueSize;
    }

    @Override
    public ITupleReference getNewValue() {
        return newValue;
    }

    @Override
    public void setNewValue(ITupleReference newValue) {
        this.newValue = newValue;
        this.newValueFieldCount = newValue.getFieldCount();
    }

    @Override
    public long getChecksum() {
        return checksum;
    }

    @Override
    public void setChecksum(long checksum) {
        this.checksum = checksum;
    }

    @Override
    public long getLSN() {
        return LSN;
    }

    @Override
    public void setLSN(long LSN) {
        this.LSN = LSN;
    }

    @Override
    public int getPKValueSize() {
        return PKValueSize;
    }

    @Override
    public ITupleReference getPKValue() {
        return PKValue;
    }

    @Override
    public void setPKFields(int[] primaryKeyFields) {
        PKFields = primaryKeyFields;
        PKFieldCnt = PKFields.length;
    }

    @Override
    public void setPKValue(ITupleReference PKValue) {
        this.PKValue = PKValue;
    }

    public PrimaryIndexOperationTracker getOpTracker() {
        return opTracker;
    }

    @Override
    public String getNodeId() {
        return nodeId;
    }

    @Override
    public void setNodeId(String nodeId) {
        this.nodeId = nodeId;
    }

    public IReplicationThread getReplicationThread() {
        return replicationThread;
    }

    @Override
    public void setReplicationThread(IReplicationThread replicationThread) {
        this.replicationThread = replicationThread;
    }

    @Override
    public void setLogSource(byte logSource) {
        this.logSource = logSource;
    }

    @Override
    public byte getLogSource() {
        return logSource;
    }

    public int getNumOfFlushedIndexes() {
        return numOfFlushedIndexes;
    }

    public void setNumOfFlushedIndexes(int numOfFlushedIndexes) {
        this.numOfFlushedIndexes = numOfFlushedIndexes;
    }

    public void setPKFieldCnt(int pKFieldCnt) {
        PKFieldCnt = pKFieldCnt;
    }

    public void setOpTracker(PrimaryIndexOperationTracker opTracker) {
        this.opTracker = opTracker;
    }

    @Override
    public int getResourcePartition() {
        return resourcePartition;
    }

    @Override
    public void setResourcePartition(int resourcePartition) {
        this.resourcePartition = resourcePartition;
    }

    @Override
    public void setReplicated(boolean replicate) {
        this.replicated = replicate;
    }

    @Override
    public boolean isReplicated() {
        return replicated;
    }

    @Override
    public ITupleReference getOldValue() {
        return oldValue;
    }

    @Override
    public void setOldValue(ITupleReference oldValue) {
        this.oldValue = oldValue;
        this.oldValueFieldCount = oldValue.getFieldCount();
    }

    @Override
    public void setOldValueSize(int oldValueSize) {
        this.oldValueSize = oldValueSize;
    }

    public void setMarker(ByteBuffer marker) {
        this.marker = marker;
    }

    @Override
    public boolean isMarker() {
        return logType == LogType.MARKER;
    }

    @Override
    public void logAppended(long lsn) {
        callback.after(lsn);
    }

    @Override
    public long getPreviousMarkerLSN() {
        return prevMarkerLSN;
    }

    @Override
    public ByteBuffer getMarker() {
        return marker;
    }
}
