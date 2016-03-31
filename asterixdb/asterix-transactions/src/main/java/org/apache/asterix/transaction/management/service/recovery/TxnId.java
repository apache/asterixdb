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
package org.apache.asterix.transaction.management.service.recovery;

import java.nio.ByteBuffer;

import org.apache.asterix.common.transactions.DatasetId;
import org.apache.asterix.common.transactions.JobId;
import org.apache.asterix.common.transactions.LogRecord;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;

public class TxnId {
    public boolean isByteArrayPKValue;
    public int jobId;
    public int datasetId;
    public int pkHashValue;
    public int pkSize;
    public byte[] byteArrayPKValue;
    public ITupleReference tupleReferencePKValue;

    public TxnId(int jobId, int datasetId, int pkHashValue, ITupleReference pkValue, int pkSize,
            boolean isByteArrayPKValue) {
        this.jobId = jobId;
        this.datasetId = datasetId;
        this.pkHashValue = pkHashValue;
        this.pkSize = pkSize;
        this.isByteArrayPKValue = isByteArrayPKValue;
        if (isByteArrayPKValue) {
            this.byteArrayPKValue = new byte[pkSize];
            readPKValueIntoByteArray(pkValue, pkSize, byteArrayPKValue);
        } else {
            this.tupleReferencePKValue = pkValue;
        }
    }

    public TxnId() {
    }

    private static void readPKValueIntoByteArray(ITupleReference pkValue, int pkSize, byte[] byteArrayPKValue) {
        int readOffset = pkValue.getFieldStart(0);
        byte[] readBuffer = pkValue.getFieldData(0);
        for (int i = 0; i < pkSize; i++) {
            byteArrayPKValue[i] = readBuffer[readOffset + i];
        }
    }

    public void setTxnId(int jobId, int datasetId, int pkHashValue, ITupleReference pkValue, int pkSize) {
        this.jobId = jobId;
        this.datasetId = datasetId;
        this.pkHashValue = pkHashValue;
        this.tupleReferencePKValue = pkValue;
        this.pkSize = pkSize;
        this.isByteArrayPKValue = false;
    }

    @Override
    public String toString() {
        return "[" + jobId + "," + datasetId + "," + pkHashValue + "," + pkSize + "]";
    }

    @Override
    public int hashCode() {
        return pkHashValue;
    }

    @Override
    public boolean equals(Object o) {
        if (o == this) {
            return true;
        }
        if (!(o instanceof TxnId)) {
            return false;
        }
        TxnId txnId = (TxnId) o;
        return (txnId.pkHashValue == pkHashValue && txnId.datasetId == datasetId && txnId.jobId == jobId
                && pkSize == txnId.pkSize && isEqualTo(txnId));
    }

    private boolean isEqualTo(TxnId txnId) {
        if (isByteArrayPKValue && txnId.isByteArrayPKValue) {
            return isEqual(byteArrayPKValue, txnId.byteArrayPKValue, pkSize);
        } else if (isByteArrayPKValue && (!txnId.isByteArrayPKValue)) {
            return isEqual(byteArrayPKValue, txnId.tupleReferencePKValue, pkSize);
        } else if ((!isByteArrayPKValue) && txnId.isByteArrayPKValue) {
            return isEqual(txnId.byteArrayPKValue, tupleReferencePKValue, pkSize);
        } else {
            return isEqual(tupleReferencePKValue, txnId.tupleReferencePKValue, pkSize);
        }
    }

    private static boolean isEqual(byte[] a, byte[] b, int size) {
        for (int i = 0; i < size; i++) {
            if (a[i] != b[i]) {
                return false;
            }
        }
        return true;
    }

    private static boolean isEqual(byte[] a, ITupleReference b, int size) {
        int readOffset = b.getFieldStart(0);
        byte[] readBuffer = b.getFieldData(0);
        for (int i = 0; i < size; i++) {
            if (a[i] != readBuffer[readOffset + i]) {
                return false;
            }
        }
        return true;
    }

    private static boolean isEqual(ITupleReference a, ITupleReference b, int size) {
        int aOffset = a.getFieldStart(0);
        byte[] aBuffer = a.getFieldData(0);
        int bOffset = b.getFieldStart(0);
        byte[] bBuffer = b.getFieldData(0);
        for (int i = 0; i < size; i++) {
            if (aBuffer[aOffset + i] != bBuffer[bOffset + i]) {
                return false;
            }
        }
        return true;
    }

    public void serialize(ByteBuffer buffer) {
        buffer.putInt(jobId);
        buffer.putInt(datasetId);
        buffer.putInt(pkHashValue);
        buffer.putInt(pkSize);
        buffer.put((byte) (isByteArrayPKValue ? 1 : 0));
        if (isByteArrayPKValue) {
            buffer.put(byteArrayPKValue);
        }
    }

    public static TxnId deserialize(ByteBuffer buffer) {
        TxnId txnId = new TxnId();
        txnId.jobId = buffer.getInt();
        txnId.datasetId = buffer.getInt();
        txnId.pkHashValue = buffer.getInt();
        txnId.pkSize = buffer.getInt();
        txnId.isByteArrayPKValue = (buffer.get() == 1);
        if (txnId.isByteArrayPKValue) {
            byte[] byteArrayPKValue = new byte[txnId.pkSize];
            buffer.get(byteArrayPKValue);
            txnId.byteArrayPKValue = byteArrayPKValue;
        }
        return txnId;
    }

    public int getCurrentSize() {
        //job id, dataset id, pkHashValue, arraySize, isByteArrayPKValue
        int size = JobId.BYTES + DatasetId.BYTES + LogRecord.PKHASH_LEN + LogRecord.PKSZ_LEN + Byte.BYTES;
        //byte arraySize
        if (isByteArrayPKValue && byteArrayPKValue != null) {
            size += byteArrayPKValue.length;
        }
        return size;
    }
}
