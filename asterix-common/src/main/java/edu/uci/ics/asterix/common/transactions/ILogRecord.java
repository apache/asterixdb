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
package edu.uci.ics.asterix.common.transactions;

import java.nio.ByteBuffer;

import edu.uci.ics.hyracks.dataflow.common.data.accessors.ITupleReference;

public interface ILogRecord {

    public static final int COMMIT_LOG_SIZE = 21;
    public static final int UPDATE_LOG_BASE_SIZE = 56;

    public boolean readLogRecord(ByteBuffer buffer);

    public void writeLogRecord(ByteBuffer buffer);
    
    public void formCommitLogRecord(ITransactionContext txnCtx, byte logType, int jobId, int datasetId, int PKHashValue);

    public void setUpdateLogSize();

    public ITransactionContext getTxnCtx();

    public void setTxnCtx(ITransactionContext txnCtx);

    public boolean isFlushed();

    public void isFlushed(boolean isFlushed);

    public byte getLogType();

    public void setLogType(byte logType);

    public int getJobId();

    public void setJobId(int jobId);

    public int getDatasetId();

    public void setDatasetId(int datasetId);

    public int getPKHashValue();

    public void setPKHashValue(int PKHashValue);

    public long getPrevLSN();

    public void setPrevLSN(long prevLsn);

    public long getResourceId();

    public void setResourceId(long resourceId);

    public byte getResourceType();

    public void setResourceType(byte resourceType);

    public int getLogSize();

    public void setLogSize(int logSize);

    public byte getNewOp();

    public void setNewOp(byte newOp);

    public int getNewValueSize();

    public void setNewValueSize(int newValueSize);

    public ITupleReference getNewValue();

    public void setNewValue(ITupleReference newValue);

    public byte getOldOp();

    public void setOldOp(byte oldOp);

    public int getOldValueSize();

    public void setOldValueSize(int oldValueSize);

    public ITupleReference getOldValue();

    public void setOldValue(ITupleReference oldValue);

    public long getChecksum();

    public void setChecksum(long checksum);
    
    public long getLSN();

    public void setLSN(long LSN);

    public String getLogRecordForDisplay();

}
