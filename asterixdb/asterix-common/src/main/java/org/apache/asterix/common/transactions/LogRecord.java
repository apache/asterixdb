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

/*
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
 * [Header3] (20 bytes) : only for update log type
 * PrevLSN(8)
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
 *    --> ENTITY_COMMIT_LOG_BASE_SIZE = 30
 * 3) UPDATE: (Header1(6) + Header2(16) + + Header3(20) + Body(9) + Tail(8)) + PKValueSize + NewValueSize
 *    --> UPDATE_LOG_BASE_SIZE = 59
 * 4) FLUSH: 18 bytes (Header1(6) + DatasetId(4) + Tail(8))
 * 5) WAIT_LOG_SIZE: 14 bytes (Header1(6) + Tail(8))
 *    --> WAIT_LOG only requires LogType Field, but in order to conform the log reader protocol
 *        it also includes LogSource and JobId fields.
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
    private int fieldCnt;
    private byte newOp;
    private int newValueSize;
    private ITupleReference newValue;
    private long checksum;
    // ------------- fields in a log record (end) --------------//

    private int PKFieldCnt;
    private ITransactionContext txnCtx;
    private long LSN;
    private final AtomicBoolean isFlushed;
    private final SimpleTupleWriter tupleWriter;
    private final PrimaryKeyTupleReference readPKValue;
    private final SimpleTupleReference readNewValue;
    private final CRC32 checksumGen;
    private int[] PKFields;
    private PrimaryIndexOperationTracker opTracker;
    private IReplicationThread replicationThread;
    private ByteBuffer serializedLog;
    /**
     * The fields (numOfFlushedIndexes and nodeId) are used for serialized flush logs only
     * to indicate the source of the log and how many indexes were flushed using its LSN.
     */
    private int numOfFlushedIndexes;
    private String nodeId;
    private boolean replicated = false;

    public LogRecord() {
        isFlushed = new AtomicBoolean(false);
        tupleWriter = new SimpleTupleWriter();
        readPKValue = new PrimaryKeyTupleReference();
        readNewValue = (SimpleTupleReference) tupleWriter.createTupleReference();
        checksumGen = new CRC32();
        logSource = LogSource.LOCAL;
    }

    private final static int LOG_SOURCE_LEN = Byte.BYTES;
    private final static int TYPE_LEN = Byte.BYTES;
    public final static int PKHASH_LEN = Integer.BYTES;
    public final static int PKSZ_LEN = Integer.BYTES;
    private final static int RS_PARTITION_LEN = Integer.BYTES;
    private final static int RSID_LEN = Long.BYTES;
    private final static int LOGRCD_SZ_LEN = Integer.BYTES;
    private final static int FLDCNT_LEN = Integer.BYTES;
    private final static int NEWOP_LEN = Byte.BYTES;
    private final static int NEWVALSZ_LEN = Integer.BYTES;
    private final static int CHKSUM_LEN = Long.BYTES;

    private final static int ALL_RECORD_HEADER_LEN = LOG_SOURCE_LEN + TYPE_LEN + JobId.BYTES;
    private final static int ENTITYCOMMIT_UPDATE_HEADER_LEN = RS_PARTITION_LEN + DatasetId.BYTES + PKHASH_LEN
            + PKSZ_LEN;
    private final static int UPDATE_LSN_HEADER = RSID_LEN + LOGRCD_SZ_LEN;
    private final static int UPDATE_BODY_HEADER = FLDCNT_LEN + NEWOP_LEN + NEWVALSZ_LEN;
    private final static int REMOTE_FLUSH_LOG_EXTRA_FIELDS_LEN = Long.BYTES + Integer.BYTES + Integer.BYTES;

    private void writeLogRecordCommonFields(ByteBuffer buffer) {
        buffer.put(logSource);
        buffer.put(logType);
        buffer.putInt(jobId);
        if (logType == LogType.UPDATE || logType == LogType.ENTITY_COMMIT || logType == LogType.UPSERT_ENTITY_COMMIT) {
            buffer.putInt(resourcePartition);
            buffer.putInt(datasetId);
            buffer.putInt(PKHashValue);
            if (PKValueSize <= 0) {
                throw new IllegalStateException("Primary Key Size is less than or equal to 0");
            }
            buffer.putInt(PKValueSize);
            writePKValue(buffer);
        }
        if (logType == LogType.UPDATE) {
            buffer.putLong(resourceId);
            buffer.putInt(logSize);
            buffer.putInt(fieldCnt);
            buffer.put(newOp);
            buffer.putInt(newValueSize);
            writeTuple(buffer, newValue, newValueSize);
        }
        if (logType == LogType.FLUSH) {
            buffer.putInt(datasetId);
        }
    }

    @Override
    public void writeLogRecord(ByteBuffer buffer) {
        int beginOffset = buffer.position();
        writeLogRecordCommonFields(buffer);
        checksum = generateChecksum(buffer, beginOffset, logSize - CHKSUM_LEN);
        buffer.putLong(checksum);
    }

    // this method is used when replication is enabled to include the log record LSN in the serialized version
    @Override
    public void writeLogRecord(ByteBuffer buffer, long appendLSN) {
        int beginOffset = buffer.position();
        writeLogRecordCommonFields(buffer);

        if (replicated) {
            //copy the serialized log to send it to replicas
            int serializedLogSize = getSerializedLogSize();
            if (serializedLog == null || serializedLog.capacity() < serializedLogSize) {
                serializedLog = ByteBuffer.allocate(serializedLogSize);
            } else {
                serializedLog.clear();
            }

            int currentPosition = buffer.position();
            int currentLogSize = (currentPosition - beginOffset);

            buffer.position(beginOffset);
            buffer.get(serializedLog.array(), 0, currentLogSize);
            serializedLog.position(currentLogSize);
            if (logType == LogType.FLUSH) {
                serializedLog.putLong(appendLSN);
                serializedLog.putInt(numOfFlushedIndexes);
                serializedLog.putInt(nodeId.length());
                serializedLog.put(nodeId.getBytes());
            }
            serializedLog.flip();
            buffer.position(currentPosition);
        }

        checksum = generateChecksum(buffer, beginOffset, logSize - CHKSUM_LEN);
        buffer.putLong(checksum);
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
        tupleWriter.writeTuple(tuple, buffer.array(), buffer.position());
        // writeTuple() doesn't change the position of the buffer.
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
        RecordReadStatus status = readLogCommonFields(buffer);
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

    private RecordReadStatus readLogCommonFields(ByteBuffer buffer) {
        //first we need the logtype and Job ID, if the buffer isn't that big, then no dice.
        if (buffer.remaining() < ALL_RECORD_HEADER_LEN) {
            return RecordReadStatus.TRUNCATED;
        }
        logSource = buffer.get();
        logType = buffer.get();
        jobId = buffer.getInt();

        if (logType == LogType.FLUSH) {
            if (buffer.remaining() < DatasetId.BYTES) {
                return RecordReadStatus.TRUNCATED;
            }
            datasetId = buffer.getInt();
            resourceId = 0l;
            computeAndSetLogSize();
        } else if (logType == LogType.WAIT) {
            computeAndSetLogSize();
        } else {
            if (logType == LogType.JOB_COMMIT || logType == LogType.ABORT) {
                datasetId = -1;
                PKHashValue = -1;
            } else {
                //attempt to read in the resourcePartition, dsid, PK hash and PK length
                if (buffer.remaining() < ENTITYCOMMIT_UPDATE_HEADER_LEN) {
                    return RecordReadStatus.TRUNCATED;
                }
                resourcePartition = buffer.getInt();
                datasetId = buffer.getInt();
                PKHashValue = buffer.getInt();
                PKValueSize = buffer.getInt();
                // attempt to read in the PK
                if (buffer.remaining() < PKValueSize) {
                    return RecordReadStatus.TRUNCATED;
                }
                if (PKValueSize <= 0) {
                    throw new IllegalStateException("Primary Key Size is less than or equal to 0");
                }
                PKValue = readPKValue(buffer);
            }

            if (logType == LogType.UPDATE) {
                // attempt to read in the previous LSN, log size, new value size, and new record type
                if (buffer.remaining() < UPDATE_LSN_HEADER + UPDATE_BODY_HEADER) {
                    return RecordReadStatus.TRUNCATED;
                }
                resourceId = buffer.getLong();
                logSize = buffer.getInt();
                fieldCnt = buffer.getInt();
                newOp = buffer.get();
                newValueSize = buffer.getInt();
                if (buffer.remaining() < newValueSize) {
                    if (logSize > buffer.capacity()) {
                        return RecordReadStatus.LARGE_RECORD;
                    }
                    return RecordReadStatus.TRUNCATED;
                }
                newValue = readTuple(buffer, readNewValue, fieldCnt, newValueSize);
            } else {
                computeAndSetLogSize();
            }
        }

        return RecordReadStatus.OK;
    }

    @Override
    public RecordReadStatus readRemoteLog(ByteBuffer buffer, boolean remoteRecoveryLog) {
        int beginOffset = buffer.position();

        //read common fields
        RecordReadStatus status = readLogCommonFields(buffer);
        if (status != RecordReadStatus.OK) {
            buffer.position(beginOffset);
            return status;
        }

        if (logType == LogType.FLUSH) {
            if (buffer.remaining() >= REMOTE_FLUSH_LOG_EXTRA_FIELDS_LEN) {
                LSN = buffer.getLong();
                numOfFlushedIndexes = buffer.getInt();
                //read serialized node id
                int nodeIdLength = buffer.getInt();
                if (buffer.remaining() >= nodeIdLength) {
                    byte[] nodeIdBytes = new byte[nodeIdLength];
                    buffer.get(nodeIdBytes);
                    nodeId = new String(nodeIdBytes);
                } else {
                    buffer.position(beginOffset);
                    return RecordReadStatus.TRUNCATED;
                }
            } else {
                buffer.position(beginOffset);
                return RecordReadStatus.TRUNCATED;
            }
        }

        //remote recovery logs need to have the LSN to check which should be replayed
        if (remoteRecoveryLog) {
            if (buffer.remaining() >= Long.BYTES) {
                LSN = buffer.getLong();
            } else {
                buffer.position(beginOffset);
                return RecordReadStatus.TRUNCATED;
            }
        }

        return RecordReadStatus.OK;
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
        logSize = UPDATE_LOG_BASE_SIZE + PKValueSize + newValueSize;
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
            default:
                throw new IllegalStateException("Unsupported Log Type");
        }
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

    @Override
    public int writeRemoteRecoveryLog(ByteBuffer buffer) {
        int bufferBegin = buffer.position();
        writeLogRecordCommonFields(buffer);
        //FLUSH logs should not included in remote recovery
        //LSN must be included in all remote recovery logs
        buffer.putLong(LSN);
        return buffer.position() - bufferBegin;
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
    public int getSerializedLogSize() {
        int serilizedSize = logSize;
        if (logType == LogType.FLUSH) {
            //LSN
            serilizedSize += Long.BYTES;
            //num of indexes
            serilizedSize += Integer.BYTES;
            //serialized node id String
            serilizedSize += Integer.BYTES + nodeId.length();
        }
        serilizedSize -= CHKSUM_LEN;
        return serilizedSize;
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
    public int getNewValueSize() {
        return newValueSize;
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
        this.fieldCnt = newValue.getFieldCount();
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
    public ByteBuffer getSerializedLog() {
        return serializedLog;
    }

    public void setSerializedLog(ByteBuffer serializedLog) {
        this.serializedLog = serializedLog;
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
}
