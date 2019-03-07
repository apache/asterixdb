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

import static org.apache.asterix.common.transactions.LogConstants.*;

import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.zip.CRC32;

import org.apache.asterix.common.context.PrimaryIndexOperationTracker;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;
import org.apache.hyracks.storage.am.common.api.ITreeIndexTupleReference;
import org.apache.hyracks.storage.am.common.tuples.SimpleTupleReferenceV0;
import org.apache.hyracks.storage.am.common.tuples.SimpleTupleWriter;

/**
 * == LogRecordFormat ==
 * ---------------------------
 * [Header1] (10 bytes) : for all log types
 * LogSourceVersion(1) : high 5 bits are used for log record version; low 3 bits are reserved for LogSource
 * LogType(1)
 * TxnId(8)
 * ---------------------------
 * [Header2] (8 bytes) : for entity_commit, upsert_entity_commit, filter and update log types
 * DatasetId(4) //stored in dataset_dataset in Metadata Node
 * ResourcePartition(4)
 * ---------------------------
 * [Header3] (8 bytes + PKValueSize) : for entity_commit, upsert_entity_commit, and update log types
 * PKHashValue(4)
 * PKValueSize(4)
 * PKValue(PKValueSize)
 * ---------------------------
 * [Header4] (12 bytes) : only for update, filter log type
 * ResourceId(8) //stored in .metadata of the corresponding index in NC node
 * LogRecordSize(4)
 * ---------------------------
 * [Body] (9 bytes + NewValueSize) : only for update, filter log type
 * FieldCnt(4)
 * NewOp(1)
 * NewValueSize(4)
 * NewValue(NewValueSize)
 * ---------------------------
 * [Tail] (8 bytes) : for all log types
 * Checksum(8)
 * ---------------------------
 */
public class LogRecord implements ILogRecord {
    // ------------- fields in a log record (begin) ------------//
    private int version = V_CURRENT;
    private byte logSource = LogSource.LOCAL;
    private byte logType;
    private long txnId;
    private int datasetId;
    private int pKHashValue;
    private int pKValueSize;
    private ITupleReference pKValue;
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
    private long flushingComponentMinId;
    private long flushingComponentMaxId;
    // ------------- fields in a log record (end) --------------//
    private final ILogMarkerCallback callback; // A callback for log mark operations
    private int pKFieldCnt;
    private ITransactionContext txnCtx;
    private volatile long LSN;
    private final AtomicBoolean isFlushed;
    private final PrimaryKeyTupleReference readPKValue;
    private final ITreeIndexTupleReference readNewValue;
    private final ITreeIndexTupleReference readOldValue;
    private ITreeIndexTupleReference readNewValueV0;
    private ITreeIndexTupleReference readOldValueV0;
    private final CRC32 checksumGen;
    private int[] pKFields;
    private PrimaryIndexOperationTracker opTracker;

    /**
     * The fields (numOfFlushedIndexes and nodeId) are used for remote flush logs only
     * to indicate the source of the log and how many indexes were flushed using its LSN.
     */
    private final AtomicBoolean replicated;
    private boolean replicate = false;
    private ILogRequester requester;

    public LogRecord(ILogMarkerCallback callback) {
        this.callback = callback;
        isFlushed = new AtomicBoolean(false);
        replicated = new AtomicBoolean(false);
        readPKValue = new PrimaryKeyTupleReference();
        readNewValue = SimpleTupleWriter.INSTANCE.createTupleReference();
        readOldValue = SimpleTupleWriter.INSTANCE.createTupleReference();
        checksumGen = new CRC32();
    }

    public LogRecord() {
        this(null);
    }

    private void doWriteLogRecord(ByteBuffer buffer) {
        buffer.put((byte) (version << 2 | (logSource & 0xff)));
        buffer.put(logType);
        buffer.putLong(txnId);
        switch (logType) {
            case LogType.ENTITY_COMMIT:
                writeEntityResource(buffer);
                writeEntityValue(buffer);
                break;
            case LogType.UPDATE:
                writeEntityResource(buffer);
                writeEntityValue(buffer);
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
            case LogType.FILTER:
                writeEntityResource(buffer);
                buffer.putLong(resourceId);
                buffer.putInt(logSize);
                buffer.putInt(newValueFieldCount);
                buffer.put(newOp);
                buffer.putInt(newValueSize);
                writeTuple(buffer, newValue, newValueSize);
                break;
            case LogType.FLUSH:
                buffer.putInt(datasetId);
                buffer.putInt(resourcePartition);
                buffer.putLong(flushingComponentMinId);
                buffer.putLong(flushingComponentMaxId);
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

    private void writeEntityValue(ByteBuffer buffer) {
        buffer.putInt(pKHashValue);
        if (pKValueSize <= 0) {
            throw new IllegalStateException("Primary Key Size is less than or equal to 0");
        }
        buffer.putInt(pKValueSize);
        writePKValue(buffer);
    }

    private void writeEntityResource(ByteBuffer buffer) {
        buffer.putInt(resourcePartition);
        buffer.putInt(datasetId);
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
        }
    }

    private void writePKValue(ByteBuffer buffer) {
        if (logSource == LogSource.LOCAL) {
            for (int i = 0; i < pKFieldCnt; i++) {
                buffer.put(pKValue.getFieldData(0), pKValue.getFieldStart(pKFields[i]),
                        pKValue.getFieldLength(pKFields[i]));
            }
        } else {
            // since pKValue is already serialized in remote logs, just put it into buffer
            buffer.put(pKValue.getFieldData(0), 0, pKValueSize);
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
        byte logSourceVersion = buffer.get();
        logSource = (byte) (logSourceVersion & 0x3);
        version = (byte) ((logSourceVersion & 0xff) >> 2);
        ITreeIndexTupleReference readOld;
        ITreeIndexTupleReference readNew;
        if (version == V_0) {
            if (readOldValueV0 == null) {
                readOldValueV0 = new SimpleTupleReferenceV0();
                readNewValueV0 = new SimpleTupleReferenceV0();
            }
            readOld = readOldValueV0;
            readNew = readNewValueV0;
        } else {
            readOld = readOldValue;
            readNew = readNewValue;
        }
        logType = buffer.get();
        txnId = buffer.getLong();
        switch (logType) {
            case LogType.FLUSH:
                if (buffer.remaining() < DS_LEN + RS_PARTITION_LEN + FLUSHING_COMPONENT_MINID_LEN
                        + FLUSHING_COMPONENT_MAXID_LEN) {
                    return RecordReadStatus.TRUNCATED;
                }
                datasetId = buffer.getInt();
                resourcePartition = buffer.getInt();
                flushingComponentMinId = buffer.getLong();
                flushingComponentMaxId = buffer.getLong();
                resourceId = 0l;
                computeAndSetLogSize();
                break;
            case LogType.WAIT:
            case LogType.WAIT_FOR_FLUSHES:
                computeAndSetLogSize();
                break;
            case LogType.JOB_COMMIT:
            case LogType.ABORT:
                datasetId = -1;
                pKHashValue = -1;
                computeAndSetLogSize();
                break;
            case LogType.ENTITY_COMMIT:
                if (readEntityResource(buffer) && readEntityValue(buffer)) {
                    computeAndSetLogSize();
                } else {
                    return RecordReadStatus.TRUNCATED;
                }
                break;
            case LogType.UPDATE:
                if (readEntityResource(buffer) && readEntityValue(buffer)) {
                    return readUpdateInfo(buffer, readNew, readOld);
                } else {
                    return RecordReadStatus.TRUNCATED;
                }
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
            case LogType.FILTER:
                if (readEntityResource(buffer)) {
                    return readUpdateInfo(buffer, readNew, readOld);
                } else {
                    return RecordReadStatus.TRUNCATED;
                }
            default:
                break;
        }
        return RecordReadStatus.OK;
    }

    private boolean readEntityValue(ByteBuffer buffer) {
        //attempt to read in the resourcePartition, dsid, PK hash and PK length
        if (buffer.remaining() < ENTITY_VALUE_HEADER_LEN) {
            return false;
        }
        pKHashValue = buffer.getInt();
        pKValueSize = buffer.getInt();
        // attempt to read in the PK
        if (buffer.remaining() < pKValueSize) {
            return false;
        }
        if (pKValueSize <= 0) {
            throw new IllegalStateException("Primary Key Size is less than or equal to 0");
        }
        pKValue = readPKValue(buffer);
        return true;
    }

    private boolean readEntityResource(ByteBuffer buffer) {
        //attempt to read in the resourcePartition and dsid
        if (buffer.remaining() < ENTITY_RESOURCE_HEADER_LEN) {
            return false;
        }
        resourcePartition = buffer.getInt();
        datasetId = buffer.getInt();
        return true;
    }

    private RecordReadStatus readUpdateInfo(ByteBuffer buffer, ITreeIndexTupleReference newRead,
            ITreeIndexTupleReference oldRead) {
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
        newValue = readTuple(buffer, newRead, newValueFieldCount, newValueSize);
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
            oldValue = readTuple(buffer, oldRead, oldValueFieldCount, oldValueSize);
        } else {
            oldValueSize = 0;
            oldValue = null;
        }
        return RecordReadStatus.OK;
    }

    @Override
    public void readRemoteLog(ByteBuffer buffer) {
        //read common fields
        doReadLogRecord(buffer);

        if (logType == LogType.FLUSH) {
            LSN = buffer.getLong();
        }
    }

    private ITupleReference readPKValue(ByteBuffer buffer) {
        if (buffer.position() + pKValueSize > buffer.limit()) {
            throw new BufferUnderflowException();
        }
        readPKValue.reset(buffer.array(), buffer.position(), pKValueSize);
        buffer.position(buffer.position() + pKValueSize);
        return readPKValue;
    }

    private static ITupleReference readTuple(ByteBuffer srcBuffer, ITreeIndexTupleReference destTuple, int fieldCnt,
            int size) {
        if (srcBuffer.position() + size > srcBuffer.limit()) {
            throw new BufferUnderflowException();
        }
        destTuple.setFieldCount(fieldCnt);
        destTuple.resetByTupleOffset(srcBuffer.array(), srcBuffer.position());
        srcBuffer.position(srcBuffer.position() + size);
        return destTuple;
    }

    @Override
    public void computeAndSetPKValueSize() {
        int i;
        pKValueSize = 0;
        for (i = 0; i < pKFieldCnt; i++) {
            pKValueSize += pKValue.getFieldLength(pKFields[i]);
        }
    }

    private void setUpdateLogSize() {
        logSize = getUpdateLogSizeWithoutOldValue();
        if (oldValueSize > 0) {
            logSize += /*size*/Integer.BYTES + /*fieldCount*/Integer.BYTES + /*tuple*/oldValueSize;
        }
    }

    private int getFilterLogSize() {
        return FILTER_LOG_BASE_SIZE + newValueSize;
    }

    private int getUpdateLogSizeWithoutOldValue() {
        return UPDATE_LOG_BASE_SIZE + pKValueSize + newValueSize;
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
                logSize = ENTITY_COMMIT_LOG_BASE_SIZE + pKValueSize;
                break;
            case LogType.FLUSH:
                logSize = FLUSH_LOG_SIZE;
                break;
            case LogType.WAIT:
            case LogType.WAIT_FOR_FLUSHES:
                logSize = WAIT_LOG_SIZE;
                break;
            case LogType.FILTER:
                logSize = getFilterLogSize();
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
        builder.append(" TxnId : ").append(txnId);
        if (logType == LogType.ENTITY_COMMIT || logType == LogType.UPDATE) {
            builder.append(" DatasetId : ").append(datasetId);
            builder.append(" ResourcePartition : ").append(resourcePartition);
            builder.append(" PKHashValue : ").append(pKHashValue);
            builder.append(" PKFieldCnt : ").append(pKFieldCnt);
            builder.append(" PKSize: ").append(pKValueSize);
        }
        if (logType == LogType.UPDATE) {
            builder.append(" ResourceId : ").append(resourceId);
        }
        builder.append(" Version : ").append(version);
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
    public long getTxnId() {
        return txnId;
    }

    @Override
    public void setTxnId(long txnId) {
        this.txnId = txnId;
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
        return pKHashValue;
    }

    @Override
    public void setPKHashValue(int pKHashValue) {
        this.pKHashValue = pKHashValue;
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
        return pKValueSize;
    }

    @Override
    public ITupleReference getPKValue() {
        return pKValue;
    }

    @Override
    public void setPKFields(int[] primaryKeyFields) {
        pKFields = primaryKeyFields;
        pKFieldCnt = pKFields.length;
    }

    @Override
    public void setPKValue(ITupleReference pKValue) {
        this.pKValue = pKValue;
    }

    public PrimaryIndexOperationTracker getOpTracker() {
        return opTracker;
    }

    @Override
    public void setLogSource(byte logSource) {
        if (logSource < LOG_SOURCE_MIN) {
            throw new IllegalArgumentException("logSource underflow: " + logSource);
        } else if (logSource > LOG_SOURCE_MAX) {
            throw new IllegalArgumentException("logSource overflow: " + logSource);
        }
        this.logSource = logSource;
    }

    @Override
    public byte getLogSource() {
        return logSource;
    }

    public void setPKFieldCnt(int pKFieldCnt) {
        this.pKFieldCnt = pKFieldCnt;
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
    public void setReplicated(boolean replicated) {
        this.replicated.set(replicated);
    }

    @Override
    public boolean isReplicated() {
        return replicated.get();
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

    @Override
    public void setReplicate(boolean replicate) {
        this.replicate = replicate;
    }

    @Override
    public boolean isReplicate() {
        return replicate;
    }

    public ILogRequester getRequester() {
        return requester;
    }

    public void setRequester(ILogRequester requester) {
        this.requester = requester;
    }

    @Override
    public long getFlushingComponentMinId() {
        return flushingComponentMinId;
    }

    @Override
    public void setFlushingComponentMinId(long flushingComponentMinId) {
        this.flushingComponentMinId = flushingComponentMinId;
    }

    @Override
    public long getFlushingComponentMaxId() {
        return flushingComponentMaxId;
    }

    @Override
    public void setFlushingComponentMaxId(long flushingComponentMaxId) {
        this.flushingComponentMaxId = flushingComponentMaxId;
    }

    @Override
    public int getVersion() {
        return version;
    }

    @Override
    public void setVersion(int version) {
        if (version < VERSION_MIN) {
            throw new IllegalArgumentException("version underflow: " + version);
        } else if (version > VERSION_MAX) {
            throw new IllegalArgumentException("version overflow: " + version);
        }
        this.version = (byte) version;
    }
}
