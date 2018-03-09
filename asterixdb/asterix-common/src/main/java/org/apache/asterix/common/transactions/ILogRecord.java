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

import java.nio.ByteBuffer;

import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;

public interface ILogRecord {

    enum RecordReadStatus {
        TRUNCATED,
        BAD_CHKSUM,
        OK,
        LARGE_RECORD
    }

    int CHKSUM_LEN = Long.BYTES;
    int FLDCNT_LEN = Integer.BYTES;
    int DS_LEN = Integer.BYTES;
    int LOG_SOURCE_LEN = Byte.BYTES;
    int LOGRCD_SZ_LEN = Integer.BYTES;
    int NEWOP_LEN = Byte.BYTES;
    int NEWVALSZ_LEN = Integer.BYTES;
    int PKHASH_LEN = Integer.BYTES;
    int PKSZ_LEN = Integer.BYTES;
    int PRVLSN_LEN = Long.BYTES;
    int RS_PARTITION_LEN = Integer.BYTES;
    int RSID_LEN = Long.BYTES;
    int SEQ_NUM_LEN = Long.BYTES;
    int TYPE_LEN = Byte.BYTES;
    int UUID_LEN = Long.BYTES;
    int FLUSHING_COMPONENT_MINID_LEN = Long.BYTES;
    int FLUSHING_COMPONENT_MAXID_LEN = Long.BYTES;

    int ALL_RECORD_HEADER_LEN = LOG_SOURCE_LEN + TYPE_LEN + TxnId.BYTES;
    int ENTITY_RESOURCE_HEADER_LEN = RS_PARTITION_LEN + DatasetId.BYTES;
    int ENTITY_VALUE_HEADER_LEN = PKHASH_LEN + PKSZ_LEN;
    int UPDATE_LSN_HEADER = RSID_LEN + LOGRCD_SZ_LEN;
    int UPDATE_BODY_HEADER = FLDCNT_LEN + NEWOP_LEN + NEWVALSZ_LEN;

    int JOB_TERMINATE_LOG_SIZE = ALL_RECORD_HEADER_LEN + CHKSUM_LEN;
    int ENTITY_COMMIT_LOG_BASE_SIZE =
            ALL_RECORD_HEADER_LEN + ENTITY_RESOURCE_HEADER_LEN + ENTITY_VALUE_HEADER_LEN + CHKSUM_LEN;
    int UPDATE_LOG_BASE_SIZE = ENTITY_COMMIT_LOG_BASE_SIZE + UPDATE_LSN_HEADER + UPDATE_BODY_HEADER;
    int FILTER_LOG_BASE_SIZE =
            ALL_RECORD_HEADER_LEN + ENTITY_RESOURCE_HEADER_LEN + UPDATE_BODY_HEADER + UPDATE_LSN_HEADER + CHKSUM_LEN;
    int FLUSH_LOG_SIZE = ALL_RECORD_HEADER_LEN + DS_LEN + RS_PARTITION_LEN + FLUSHING_COMPONENT_MINID_LEN
            + FLUSHING_COMPONENT_MAXID_LEN + CHKSUM_LEN;
    int WAIT_LOG_SIZE = ALL_RECORD_HEADER_LEN + CHKSUM_LEN;
    int MARKER_BASE_LOG_SIZE =
            ALL_RECORD_HEADER_LEN + CHKSUM_LEN + DS_LEN + RS_PARTITION_LEN + PRVLSN_LEN + LOGRCD_SZ_LEN;

    RecordReadStatus readLogRecord(ByteBuffer buffer);

    void writeLogRecord(ByteBuffer buffer);

    ITransactionContext getTxnCtx();

    void setTxnCtx(ITransactionContext txnCtx);

    boolean isFlushed();

    void isFlushed(boolean isFlushed);

    byte getLogType();

    void setLogType(byte logType);

    long getTxnId();

    void setTxnId(long jobId);

    int getDatasetId();

    void setDatasetId(int datasetId);

    int getPKHashValue();

    void setPKHashValue(int PKHashValue);

    long getResourceId();

    void setResourceId(long resourceId);

    int getLogSize();

    void setLogSize(int logSize);

    byte getNewOp();

    void setNewOp(byte newOp);

    void setNewValueSize(int newValueSize);

    ITupleReference getNewValue();

    void setNewValue(ITupleReference newValue);

    long getChecksum();

    void setChecksum(long checksum);

    long getLSN();

    void setLSN(long LSN);

    String getLogRecordForDisplay();

    void computeAndSetLogSize();

    int getPKValueSize();

    ITupleReference getPKValue();

    void setPKFields(int[] primaryKeyFields);

    void computeAndSetPKValueSize();

    void setPKValue(ITupleReference PKValue);

    void readRemoteLog(ByteBuffer buffer);

    void setLogSource(byte logSource);

    byte getLogSource();

    int getRemoteLogSize();

    int getResourcePartition();

    void setResourcePartition(int resourcePartition);

    void setReplicated(boolean replicated);

    /**
     * @return a flag indicating whether the log was replicated
     */
    boolean isReplicated();

    void writeRemoteLogRecord(ByteBuffer buffer);

    ITupleReference getOldValue();

    void setOldValue(ITupleReference tupleBefore);

    void setOldValueSize(int beforeSize);

    boolean isMarker();

    ByteBuffer getMarker();

    void logAppended(long lsn);

    long getPreviousMarkerLSN();

    /**
     * Sets flag indicating if this log should be replicated or not
     *
     * @param replicate
     */
    void setReplicate(boolean replicate);

    /**
     * Gets a flag indicating if this log should be replicated or not
     *
     * @return the flag
     */
    boolean isReplicate();

    long getFlushingComponentMinId();

    void setFlushingComponentMinId(long flushingComponentMinId);

    long getFlushingComponentMaxId();

    void setFlushingComponentMaxId(long flushingComponentMaxId);
}
