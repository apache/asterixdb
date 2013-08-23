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
 * [Header1] (13 bytes) : for all log types
 * LogType(1)
 * JobId(4)
 * DatasetId(4) //stored in dataset_dataset in Metadata Node
 * PKHashValue(4)
 * ---------------------------
 * [Header2] (21 bytes) : only for update log type
 * PrevLSN(8)
 * ResourceId(8) //stored in .metadata of the corresponding index in NC node
 * ResourceType(1)
 * LogRecordSize(4)
 * ---------------------------
 * [Body] (Variable size) : only for update log type
 * FieldCnt(4)
 * NewOp(1)
 * NewValueLength(4)
 * NewValue(NewValueLength)
 * OldOp(1)
 * OldValueLength(4)
 * OldValue(OldValueLength)
 * ---------------------------
 * [Tail] (8 bytes) : for all log types
 * Checksum(8)
 * ---------------------------
 * = LogSize =
 * 1) JOB_COMMIT and ENTITY_COMMIT: 21 bytes
 * 2) UPDATE: 56 + old and new value size (13 + 21 + 14 + old and newValueSize + 8)
 */
public class LogRecord implements ILogRecord {

    //------------- fields in a log record (begin) ------------//
    private byte logType;
    private int jobId;
    private int datasetId;
    private int PKHashValue;
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

    private static final int CHECKSUM_SIZE = 8;
    private ITransactionContext txnCtx;
    private long LSN;
    private final AtomicBoolean isFlushed;
    private final SimpleTupleWriter tupleWriter;
    private final SimpleTupleReference newTuple;
    private final CRC32 checksumGen;

    public LogRecord() {
        isFlushed = new AtomicBoolean(false);
        tupleWriter = new SimpleTupleWriter();
        newTuple = (SimpleTupleReference) tupleWriter.createTupleReference();
        checksumGen = new CRC32();
    }

    @Override
    public void writeLogRecord(ByteBuffer buffer) {
        int beginOffset = buffer.position();
        buffer.put(logType);
        buffer.putInt(jobId);
        buffer.putInt(datasetId);
        buffer.putInt(PKHashValue);
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

    private void writeTuple(ByteBuffer buffer, ITupleReference tuple, int size) {
        tupleWriter.writeTuple(tuple, buffer.array(), buffer.position());
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
            datasetId = buffer.getInt();
            PKHashValue = buffer.getInt();
            if (logType == LogType.UPDATE) {
                prevLSN = buffer.getLong();
                resourceId = buffer.getLong();
                resourceType = buffer.get();
                logSize = buffer.getInt();
                fieldCnt = buffer.getInt();
                newOp = buffer.get();
                newValueSize = buffer.getInt();
                newValue = readTuple(buffer, newValueSize);
                if (resourceType == ResourceType.LSM_BTREE) {
                    oldOp = buffer.get();
                    if (oldOp != (byte) (IndexOperation.NOOP.ordinal())) {
                        oldValueSize = buffer.getInt();
                        if (oldValueSize > 0) {
                            oldValue = readTuple(buffer, oldValueSize);
                        }
                    }
                }
            } else {
                logSize = COMMIT_LOG_SIZE;
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

    private ITupleReference readTuple(ByteBuffer buffer, int size) {
        newTuple.setFieldCount(fieldCnt);
        newTuple.resetByTupleOffset(buffer, buffer.position());
        buffer.position(buffer.position() + size);
        return newTuple;
    }

    @Override
    public void formCommitLogRecord(ITransactionContext txnCtx, byte logType, int jobId, int datasetId, int PKHashValue) {
        this.txnCtx = txnCtx;
        this.logType = logType;
        this.jobId = jobId;
        this.datasetId = datasetId;
        this.PKHashValue = PKHashValue;
        this.logSize = COMMIT_LOG_SIZE;
    }

    @Override
    public void setUpdateLogSize() {
        logSize = UPDATE_LOG_BASE_SIZE + newValueSize + oldValueSize;
        if (resourceType != ResourceType.LSM_BTREE) {
            logSize -= 5; //oldOp(byte: 1) + oldValueLength(int: 4)
        } else {
            if (oldOp == (byte) (IndexOperation.NOOP.ordinal())) {
                logSize -= 4; //oldValueLength(int: 4)
            }
        }
    }

    @Override
    public String getLogRecordForDisplay() {
        StringBuilder builder = new StringBuilder();
        builder.append(" LSN : ").append(LSN);
        builder.append(" LogType : ").append(LogType.toString(logType));
        builder.append(" JobId : ").append(jobId);
        builder.append(" DatasetId : ").append(datasetId);
        builder.append(" PKHashValue : ").append(PKHashValue);
        if (logType == LogType.UPDATE) {
            builder.append(" PrevLSN : ").append(prevLSN);
            builder.append(" ResourceId : ").append(resourceId);
            builder.append(" ResourceType : ").append(resourceType);
            builder.append(" LogSize : ").append(logSize);
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
}
