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
import java.util.HashMap;
import java.util.Map;
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
 * [Header1] (10 bytes + NodeId Length) : for all log types
 * LogSource(1)
 * LogType(1)
 * JobId(4)
 * NodeIdLength(4)
 * NodeId(?)
 * ---------------------------
 * [Header2] (12 bytes + PKValueSize) : for entity_commit and update log types 
 * DatasetId(4) //stored in dataset_dataset in Metadata Node
 * PKHashValue(4)
 * PKValueSize(4)
 * PKValue(PKValueSize)
 * ---------------------------
 * [Header3] (20 bytes) : only for update log type
 * PrevLSN(8)
 * ResourceId(8) //stored in .metadata of the corresponding index in NC node
 * LogRecordSize(4)
 * ---------------------------
 * [Body] (Variable size) : only for update log type
 * FieldCnt(4)
 * NewOp(1)
 * NewValueSize(4)
 * NewValue(NewValueSize)
 * ---------------------------
 * [Tail] (8 bytes) : for all log types
 * Checksum(8)
 * ---------------------------
 * = LogSize =
 * 1) JOB_COMMIT_LOG_SIZE: 13 bytes (5 + 8)
 * 2) ENTITY_COMMIT: 25 + PKSize (5 + 12 + PKSize + 8)
 *    --> ENTITY_COMMIT_LOG_BASE_SIZE = 25
 * 3) UPDATE: 54 + PKValueSize + NewValueSize (5 + 12 + PKValueSize + 20 + 9 + NewValueSize + 8)
 * 4) FLUSH: 5 + 8 + DatasetId(4) (In case of serialize: + (8 bytes for LSN) + (4 bytes for number of flushed indexes)
 */

public class LogRecord implements ILogRecord {

    //------------- fields in a log record (begin) ------------//
    private byte logSource;
    private String nodeId;
    private int nodeIdLength;
    private byte logType;
    private int jobId;
    private int datasetId;
    private int PKHashValue;
    private int PKValueSize;
    private ITupleReference PKValue;
    private long prevLSN;
    private long resourceId;
    private int logSize;
    private int fieldCnt;
    private byte newOp;
    private int newValueSize;
    private ITupleReference newValue;
    private long checksum;
    //------------- fields in a log record (end) --------------//

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
    private final Map<String, byte[]> nodeIdsMap;
    //this field is used for serialized flush logs only to indicate how many indexes were flushed using its LSN.
    private int numOfFlushedIndexes;

    public LogRecord() {
        isFlushed = new AtomicBoolean(false);
        tupleWriter = new SimpleTupleWriter();
        readPKValue = new PrimaryKeyTupleReference();
        readNewValue = (SimpleTupleReference) tupleWriter.createTupleReference();
        checksumGen = new CRC32();
        this.nodeIdsMap = new HashMap<String, byte[]>();
        logSource = LogSource.LOCAL;
    }

    private final static int LOG_SOURCE_LEN = Byte.BYTES;
    private final static int NODE_ID_STRING_LENGTH = Integer.BYTES;
    private final static int TYPE_LEN = Byte.BYTES;
    public final static int PKHASH_LEN = Integer.BYTES;
    public final static int PKSZ_LEN = Integer.BYTES;
    private final static int PRVLSN_LEN = Long.BYTES;
    private final static int RSID_LEN = Long.BYTES;
    private final static int LOGRCD_SZ_LEN = Integer.BYTES;
    private final static int FLDCNT_LEN = Integer.BYTES;
    private final static int NEWOP_LEN = Byte.BYTES;
    private final static int NEWVALSZ_LEN = Integer.BYTES;
    private final static int CHKSUM_LEN = Long.BYTES;

    private final static int ALL_RECORD_HEADER_LEN = LOG_SOURCE_LEN + TYPE_LEN + JobId.BYTES + NODE_ID_STRING_LENGTH;
    private final static int ENTITYCOMMIT_UPDATE_HEADER_LEN = DatasetId.BYTES + PKHASH_LEN + PKSZ_LEN;
    private final static int UPDATE_LSN_HEADER = PRVLSN_LEN + RSID_LEN + LOGRCD_SZ_LEN;
    private final static int UPDATE_BODY_HEADER = FLDCNT_LEN + NEWOP_LEN + NEWVALSZ_LEN;

    private void writeLogRecordCommonFields(ByteBuffer buffer) {
        buffer.put(logSource);
        buffer.put(logType);
        buffer.putInt(jobId);
        if (nodeIdsMap.containsKey(nodeId)) {
            buffer.put(nodeIdsMap.get(nodeId));
        } else {
            //byte array for node id length and string
            byte[] bytes = new byte[(Integer.SIZE / 8) + nodeId.length()];
            buffer.putInt(nodeId.length());
            buffer.put(nodeId.getBytes(java.nio.charset.StandardCharsets.UTF_8));
            buffer.position(buffer.position() - bytes.length);
            buffer.get(bytes, 0, bytes.length);
            nodeIdsMap.put(nodeId, bytes);
        }
        if (logType == LogType.UPDATE || logType == LogType.ENTITY_COMMIT) {
            buffer.putInt(datasetId);
            buffer.putInt(PKHashValue);
            if (PKValueSize <= 0) {
                throw new IllegalStateException("Primary Key Size is less than or equal to 0");
            }
            buffer.putInt(PKValueSize);
            writePKValue(buffer);
        }
        if (logType == LogType.UPDATE) {
            buffer.putLong(prevLSN);
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

    //this method is used when replication is enabled to include the log record LSN in the serialized version
    @Override
    public void writeLogRecord(ByteBuffer buffer, long appendLSN) {
        int beginOffset = buffer.position();
        writeLogRecordCommonFields(buffer);

        if (logSource == LogSource.LOCAL) {
            //copy the serialized log to send it to replicas
            int serializedLogSize = getSerializedLogSize(logType, logSize);

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
            //since PKValue is already serialized in remote logs, just put it into buffer
            buffer.put(PKValue.getFieldData(0), 0, PKValueSize);
        }
    }

    private void writeTuple(ByteBuffer buffer, ITupleReference tuple, int size) {
        tupleWriter.writeTuple(tuple, buffer.array(), buffer.position());
        //writeTuple() doesn't change the position of the buffer. 
        buffer.position(buffer.position() + size);
    }

    private long generateChecksum(ByteBuffer buffer, int offset, int len) {
        checksumGen.reset();
        checksumGen.update(buffer.array(), offset, len);
        return checksumGen.getValue();
    }

    @Override
    public RECORD_STATUS readLogRecord(ByteBuffer buffer) {
        int beginOffset = buffer.position();

        //read header
        RECORD_STATUS status = readLogHeader(buffer);
        if (status != RECORD_STATUS.OK) {
            buffer.position(beginOffset);
            return status;
        }

        //read body
        status = readLogBody(buffer, false);
        if (status != RECORD_STATUS.OK) {
            buffer.position(beginOffset);
            return status;
        }

        //attempt to read checksum
        if (buffer.remaining() < CHKSUM_LEN) {
            buffer.position(beginOffset);
            return RECORD_STATUS.TRUNCATED;
        }
        checksum = buffer.getLong();
        if (checksum != generateChecksum(buffer, beginOffset, logSize - CHKSUM_LEN)) {
            return RECORD_STATUS.BAD_CHKSUM;
        }

        return RECORD_STATUS.OK;
    }

    private RECORD_STATUS readLogHeader(ByteBuffer buffer) {
        //first we need the logtype and Job ID, if the buffer isn't that big, then no dice.
        if (buffer.remaining() < ALL_RECORD_HEADER_LEN) {
            return RECORD_STATUS.TRUNCATED;
        }
        logSource = buffer.get();
        logType = buffer.get();
        jobId = buffer.getInt();
        nodeIdLength = buffer.getInt();
        //attempt to read node id
        if (buffer.remaining() < nodeIdLength) {
            return RECORD_STATUS.TRUNCATED;
        }
        //read node id string
        nodeId = new String(buffer.array(), buffer.position() + buffer.arrayOffset(), nodeIdLength,
                java.nio.charset.StandardCharsets.UTF_8);
        //skip node id string bytes
        buffer.position(buffer.position() + nodeIdLength);

        return RECORD_STATUS.OK;
    }

    private RECORD_STATUS readLogBody(ByteBuffer buffer, boolean allocateTupleBuffer) {
        if (logType != LogType.FLUSH) {
            if (logType == LogType.JOB_COMMIT || logType == LogType.ABORT) {
                datasetId = -1;
                PKHashValue = -1;
            } else {
                //attempt to read in the dsid, PK hash and PK length
                if (buffer.remaining() < ENTITYCOMMIT_UPDATE_HEADER_LEN) {
                    return RECORD_STATUS.TRUNCATED;
                }
                datasetId = buffer.getInt();
                PKHashValue = buffer.getInt();
                PKValueSize = buffer.getInt();
                //attempt to read in the PK
                if (buffer.remaining() < PKValueSize) {
                    return RECORD_STATUS.TRUNCATED;
                }
                if (PKValueSize <= 0) {
                    throw new IllegalStateException("Primary Key Size is less than or equal to 0");
                }
                PKValue = readPKValue(buffer);
            }

            if (logType == LogType.UPDATE) {
                //attempt to read in the previous LSN, log size, new value size, and new record type
                if (buffer.remaining() < UPDATE_LSN_HEADER + UPDATE_BODY_HEADER) {
                    return RECORD_STATUS.TRUNCATED;
                }
                prevLSN = buffer.getLong();
                resourceId = buffer.getLong();
                logSize = buffer.getInt();
                fieldCnt = buffer.getInt();
                newOp = buffer.get();
                newValueSize = buffer.getInt();
                if (buffer.remaining() < newValueSize) {
                    return RECORD_STATUS.TRUNCATED;
                }
                if (!allocateTupleBuffer) {
                    newValue = readTuple(buffer, readNewValue, fieldCnt, newValueSize);
                } else {
                    ByteBuffer tupleBuffer = ByteBuffer.allocate(newValueSize);
                    tupleBuffer.put(buffer.array(), buffer.position(), newValueSize);
                    tupleBuffer.flip();
                    newValue = readTuple(tupleBuffer, readNewValue, fieldCnt, newValueSize);
                    //skip tuple bytes
                    buffer.position(buffer.position() + newValueSize);
                }
            } else {
                computeAndSetLogSize();
            }
        } else {
            computeAndSetLogSize();
            if (buffer.remaining() < DatasetId.BYTES) {
                return RECORD_STATUS.TRUNCATED;
            }
            datasetId = buffer.getInt();
            resourceId = 0l;
            computeAndSetLogSize();
        }
        return RECORD_STATUS.OK;
    }

    @Override
    public void deserialize(ByteBuffer buffer, boolean remoteRecoveryLog, String localNodeId) {
        readLogHeader(buffer);
        if (!remoteRecoveryLog || !nodeId.equals(localNodeId)) {
            readLogBody(buffer, false);
        } else {
            //need to allocate buffer for tuple since the logs will be kept in memory to use during remote recovery
            //TODO when this is redesigned to spill remote recovery logs to disk, this will not be needed
            readLogBody(buffer, true);
        }

        if (logType == LogType.FLUSH) {
            LSN = buffer.getLong();
            numOfFlushedIndexes = buffer.getInt();
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
    public void formJobTerminateLogRecord(ITransactionContext txnCtx, boolean isCommit) {
        this.txnCtx = txnCtx;
        formJobTerminateLogRecord(txnCtx.getJobId().getId(), isCommit, nodeId);
    }

    @Override
    public void formJobTerminateLogRecord(int jobId, boolean isCommit, String nodeId) {
        this.logType = isCommit ? LogType.JOB_COMMIT : LogType.ABORT;
        this.jobId = jobId;
        this.datasetId = -1;
        this.PKHashValue = -1;
        setNodeId(nodeId);
        computeAndSetLogSize();
    }

    public void formFlushLogRecord(int datasetId, PrimaryIndexOperationTracker opTracker, int numOfFlushedIndexes) {
        formFlushLogRecord(datasetId, opTracker, null, numOfFlushedIndexes);
    }

    public void formFlushLogRecord(int datasetId, PrimaryIndexOperationTracker opTracker, String nodeId,
            int numberOfIndexes) {
        this.logType = LogType.FLUSH;
        this.jobId = -1;
        this.datasetId = datasetId;
        this.opTracker = opTracker;
        this.numOfFlushedIndexes = numberOfIndexes;
        if (nodeId != null) {
            setNodeId(nodeId);
        }
        computeAndSetLogSize();
    }

    @Override
    public void formEntityCommitLogRecord(ITransactionContext txnCtx, int datasetId, int PKHashValue,
            ITupleReference PKValue, int[] PKFields) {
        this.txnCtx = txnCtx;
        this.logType = LogType.ENTITY_COMMIT;
        this.jobId = txnCtx.getJobId().getId();
        this.datasetId = datasetId;
        this.PKHashValue = PKHashValue;
        this.PKFieldCnt = PKFields.length;
        this.PKValue = PKValue;
        this.PKFields = PKFields;
        computeAndSetPKValueSize();
        computeAndSetLogSize();
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
                logSize = ENTITY_COMMIT_LOG_BASE_SIZE + PKValueSize;
                break;
            case LogType.FLUSH:
                logSize = FLUSH_LOG_SIZE;
                break;
            default:
                throw new IllegalStateException("Unsupported Log Type");
        }

        logSize += nodeIdLength;
    }

    @Override
    public String getLogRecordForDisplay() {
        StringBuilder builder = new StringBuilder();
        builder.append(" Source : ").append(LogSource.toString(logSource));
        builder.append(" NodeID : ").append(nodeId);
        builder.append(" LSN : ").append(LSN);
        builder.append(" LogType : ").append(LogType.toString(logType));
        builder.append(" LogSize : ").append(logSize);
        builder.append(" JobId : ").append(jobId);
        if (logType == LogType.ENTITY_COMMIT || logType == LogType.UPDATE) {
            builder.append(" DatasetId : ").append(datasetId);
            builder.append(" PKHashValue : ").append(PKHashValue);
            builder.append(" PKFieldCnt : ").append(PKFieldCnt);
            builder.append(" PKSize: ").append(PKValueSize);
        }
        if (logType == LogType.UPDATE) {
            builder.append(" PrevLSN : ").append(prevLSN);
            builder.append(" ResourceId : ").append(resourceId);
        }
        return builder.toString();
    }

    @Override
    public int serialize(ByteBuffer buffer) {
        int bufferBegin = buffer.position();
        writeLogRecordCommonFields(buffer);

        if (logType == LogType.FLUSH) {
            buffer.putLong(LSN);
            buffer.putInt(numOfFlushedIndexes);
        }

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
    public long getPrevLSN() {
        return prevLSN;
    }

    @Override
    public void setPrevLSN(long prevLSN) {
        this.prevLSN = prevLSN;
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
        return getSerializedLogSize(logType, logSize);
    }

    private static int getSerializedLogSize(Byte logType, int logSize) {
        if (logType == LogType.FLUSH) {
            //LSN
            logSize += (Long.SIZE / 8);
            //num of indexes 
            logSize += (Integer.SIZE / 8);
        }

        //checksum not included in serialized version
        logSize -= CHKSUM_LEN;

        return logSize;
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
        this.nodeIdLength = nodeId.length();
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

}
