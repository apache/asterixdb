/*
 * Copyright 2009-2013 by The Regents of the University of California
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

import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.zip.CRC32;

import edu.uci.ics.asterix.common.transactions.ILogRecord;
import edu.uci.ics.asterix.common.transactions.IRecoveryManager.ResourceType;
import edu.uci.ics.asterix.common.transactions.ITransactionContext;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.ITupleReference;
import edu.uci.ics.hyracks.storage.am.common.ophelpers.IndexOperation;
import edu.uci.ics.hyracks.storage.am.common.tuples.SimpleTupleReference;
import edu.uci.ics.hyracks.storage.am.common.tuples.SimpleTupleWriter;

/*
 * == LogRecordFormat ==
 * ---------------------------
 * [Header1] (5 bytes) : for all log types
 * LogType(1)
 * JobId(4)
 * ---------------------------
 * [Header2] (12 bytes + PKValueSize) : for entity_commit and update log types 
 * DatasetId(4) //stored in dataset_dataset in Metadata Node
 * PKHashValue(4)
 * PKValueSize(4)
 * PKValue(PKValueSize)
 * ---------------------------
 * [Header3] (21 bytes) : only for update log type
 * PrevLSN(8)
 * ResourceId(8) //stored in .metadata of the corresponding index in NC node
 * ResourceType(1)
 * LogRecordSize(4)
 * ---------------------------
 * [Body] (Variable size) : only for update log type
 * FieldCnt(4)
 * NewOp(1)
 * NewValueSize(4)
 * NewValue(NewValueSize)
 * OldOp(1)
 * OldValueSize(4)
 * OldValue(OldValueSize)
 * ---------------------------
 * [Tail] (8 bytes) : for all log types
 * Checksum(8)
 * ---------------------------
 * = LogSize =
 * 1) JOB_COMMIT_LOG_SIZE: 13 bytes (5 + 8)
 * 2) ENTITY_COMMIT: 25 + PKSize (5 + 12 + PKSize + 8)
 *    --> ENTITY_COMMIT_LOG_BASE_SIZE = 25
 * 3) UPDATE: 64 + PKSize + New/OldValueSize (5 + 12 + PKSize + 21 + 14 + New/OldValueSize + 8)
 *    --> UPDATE_LOG_BASE_SIZE = 60
 */
public class LogRecord implements ILogRecord {

    //------------- fields in a log record (begin) ------------//
    private byte logType;
    private int jobId;
    private int datasetId;
    private int PKHashValue;
    private int PKValueSize;
    private ITupleReference PKValue;
    private long prevLSN;
    private long resourceId;
    private byte resourceType;
    private int logSize;
    private int fieldCnt;
    private byte newOp;
    private int newValueSize;
    private ITupleReference newValue;
    private byte oldOp;
    private int oldValueSize;
    private ITupleReference oldValue;
    private long checksum;
    //------------- fields in a log record (end) --------------//

    private int PKFieldCnt;
    private static final int CHECKSUM_SIZE = 8;
    private ITransactionContext txnCtx;
    private long LSN;
    private final AtomicBoolean isFlushed;
    private final SimpleTupleWriter tupleWriter;
    private final PrimaryKeyTupleReference readPKValue;
    private final SimpleTupleReference readNewValue;
    private final SimpleTupleReference readOldValue;
    private final CRC32 checksumGen;
    private int[] PKFields;

    public LogRecord() {
        isFlushed = new AtomicBoolean(false);
        tupleWriter = new SimpleTupleWriter();
        readPKValue = new PrimaryKeyTupleReference();
        readNewValue = (SimpleTupleReference) tupleWriter.createTupleReference();
        readOldValue = (SimpleTupleReference) tupleWriter.createTupleReference();
        checksumGen = new CRC32();
    }

    @Override
    public void writeLogRecord(ByteBuffer buffer) {
        int beginOffset = buffer.position();
        buffer.put(logType);
        buffer.putInt(jobId);
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
            buffer.put(resourceType);
            buffer.putInt(logSize);
            buffer.putInt(fieldCnt);
            buffer.put(newOp);
            buffer.putInt(newValueSize);
            writeTuple(buffer, newValue, newValueSize);
            if (resourceType == ResourceType.LSM_BTREE) {
                buffer.put(oldOp);
                if (oldOp != (byte) (IndexOperation.NOOP.ordinal())) {
                    buffer.putInt(oldValueSize);
                    if (oldValueSize > 0) {
                        writeTuple(buffer, oldValue, oldValueSize);
                    }
                }
            }
        }
        checksum = generateChecksum(buffer, beginOffset, logSize - CHECKSUM_SIZE);
        buffer.putLong(checksum);
    }

    private void writePKValue(ByteBuffer buffer) {
        int i;
        for (i = 0; i < PKFieldCnt; i++) {
            buffer.put(PKValue.getFieldData(0), PKValue.getFieldStart(PKFields[i]), PKValue.getFieldLength(PKFields[i]));
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
    public boolean readLogRecord(ByteBuffer buffer) {
        int beginOffset = buffer.position();
        try {
            logType = buffer.get();
            jobId = buffer.getInt();
            if (logType == LogType.JOB_COMMIT || logType == LogType.ABORT) {
                datasetId = -1;
                PKHashValue = -1;
            } else {
                datasetId = buffer.getInt();
                PKHashValue = buffer.getInt();
                PKValueSize = buffer.getInt();
                if (PKValueSize <= 0) {
                    throw new IllegalStateException("Primary Key Size is less than or equal to 0");
                }
                PKValue = readPKValue(buffer);
            }
            if (logType == LogType.UPDATE) {
                prevLSN = buffer.getLong();
                resourceId = buffer.getLong();
                resourceType = buffer.get();
                logSize = buffer.getInt();
                fieldCnt = buffer.getInt();
                newOp = buffer.get();
                newValueSize = buffer.getInt();
                newValue = readTuple(buffer, readNewValue, fieldCnt, newValueSize);
                if (resourceType == ResourceType.LSM_BTREE) {
                    oldOp = buffer.get();
                    if (oldOp != (byte) (IndexOperation.NOOP.ordinal())) {
                        oldValueSize = buffer.getInt();
                        if (oldValueSize > 0) {
                            oldValue = readTuple(buffer, readOldValue, fieldCnt, oldValueSize);
                        }
                    }
                }
            } else {
                computeAndSetLogSize();
            }
            checksum = buffer.getLong();
            if (checksum != generateChecksum(buffer, beginOffset, logSize - CHECKSUM_SIZE)) {
                throw new IllegalStateException();
            }
        } catch (BufferUnderflowException e) {
            buffer.position(beginOffset);
            return false;
        }
        return true;
    }

    private ITupleReference readPKValue(ByteBuffer buffer) {
        if (buffer.position() + PKValueSize > buffer.limit()) {
            throw new BufferUnderflowException();
        }
        readPKValue.reset(buffer.array(), buffer.position(), PKValueSize);
        buffer.position(buffer.position() + PKValueSize);
        return readPKValue;
    }

    private ITupleReference readTuple(ByteBuffer srcBuffer, SimpleTupleReference destTuple, int fieldCnt, int size) {
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
        this.logType = isCommit ? LogType.JOB_COMMIT : LogType.ABORT;
        this.jobId = txnCtx.getJobId().getId();
        this.datasetId = -1;
        this.PKHashValue = -1;
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
        logSize = UPDATE_LOG_BASE_SIZE + PKValueSize + newValueSize + oldValueSize;
        if (resourceType != ResourceType.LSM_BTREE) {
            logSize -= 5; //oldOp(byte: 1) + oldValueLength(int: 4)
        } else {
            if (oldOp == (byte) (IndexOperation.NOOP.ordinal())) {
                logSize -= 4; //oldValueLength(int: 4)
            }
        }
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
            default:
                throw new IllegalStateException("Unsupported Log Type");
        }
    }

    @Override
    public String getLogRecordForDisplay() {
        StringBuilder builder = new StringBuilder();
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
            builder.append(" ResourceType : ").append(resourceType);
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
    public byte getResourceType() {
        return resourceType;
    }

    @Override
    public void setResourceType(byte resourceType) {
        this.resourceType = resourceType;
    }

    @Override
    public int getLogSize() {
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
    public byte getOldOp() {
        return oldOp;
    }

    @Override
    public void setOldOp(byte oldOp) {
        this.oldOp = oldOp;
    }

    @Override
    public int getOldValueSize() {
        return oldValueSize;
    }

    @Override
    public void setOldValueSize(int oldValueSize) {
        this.oldValueSize = oldValueSize;
    }

    @Override
    public ITupleReference getOldValue() {
        return oldValue;
    }

    @Override
    public void setOldValue(ITupleReference oldValue) {
        this.oldValue = oldValue;
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
}
