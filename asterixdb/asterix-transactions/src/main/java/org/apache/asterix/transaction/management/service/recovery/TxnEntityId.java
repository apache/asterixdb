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

import static org.apache.asterix.common.transactions.LogConstants.*;

import java.nio.ByteBuffer;

import org.apache.asterix.common.transactions.TxnId;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;

public class TxnEntityId {
    public boolean isByteArrayPKValue;
    public long txnId;
    public int datasetId;
    public int pkHashValue;
    public int pkSize;
    public byte[] byteArrayPKValue;
    public ITupleReference tupleReferencePKValue;

    public TxnEntityId(long txnId, int datasetId, int pkHashValue, ITupleReference pkValue, int pkSize,
            boolean isByteArrayPKValue) {
        this.txnId = txnId;
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

    public TxnEntityId() {
    }

    private static void readPKValueIntoByteArray(ITupleReference pkValue, int pkSize, byte[] byteArrayPKValue) {
        int readOffset = pkValue.getFieldStart(0);
        byte[] readBuffer = pkValue.getFieldData(0);
        for (int i = 0; i < pkSize; i++) {
            byteArrayPKValue[i] = readBuffer[readOffset + i];
        }
    }

    public void setTxnId(long txnId, int datasetId, int pkHashValue, ITupleReference pkValue, int pkSize) {
        this.txnId = txnId;
        this.datasetId = datasetId;
        this.pkHashValue = pkHashValue;
        this.tupleReferencePKValue = pkValue;
        this.pkSize = pkSize;
        this.isByteArrayPKValue = false;
    }

    @Override
    public String toString() {
        return "[" + txnId + "," + datasetId + "," + pkHashValue + "," + pkSize + "]";
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
        if (!(o instanceof TxnEntityId)) {
            return false;
        }
        TxnEntityId txnEntityId = (TxnEntityId) o;
        return (txnEntityId.pkHashValue == pkHashValue && txnEntityId.datasetId == datasetId
                && txnEntityId.txnId == txnId && pkSize == txnEntityId.pkSize && isEqualTo(txnEntityId));
    }

    private boolean isEqualTo(TxnEntityId txnEntityId) {
        if (isByteArrayPKValue && txnEntityId.isByteArrayPKValue) {
            return isEqual(byteArrayPKValue, txnEntityId.byteArrayPKValue, pkSize);
        } else if (isByteArrayPKValue && (!txnEntityId.isByteArrayPKValue)) {
            return isEqual(byteArrayPKValue, txnEntityId.tupleReferencePKValue, pkSize);
        } else if ((!isByteArrayPKValue) && txnEntityId.isByteArrayPKValue) {
            return isEqual(txnEntityId.byteArrayPKValue, tupleReferencePKValue, pkSize);
        } else {
            return isEqual(tupleReferencePKValue, txnEntityId.tupleReferencePKValue, pkSize);
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
        buffer.putLong(txnId);
        buffer.putInt(datasetId);
        buffer.putInt(pkHashValue);
        buffer.putInt(pkSize);
        buffer.put((byte) (isByteArrayPKValue ? 1 : 0));
        if (isByteArrayPKValue) {
            buffer.put(byteArrayPKValue);
        }
    }

    public static TxnEntityId deserialize(ByteBuffer buffer) {
        TxnEntityId txnEntityId = new TxnEntityId();
        txnEntityId.txnId = buffer.getLong();
        txnEntityId.datasetId = buffer.getInt();
        txnEntityId.pkHashValue = buffer.getInt();
        txnEntityId.pkSize = buffer.getInt();
        txnEntityId.isByteArrayPKValue = (buffer.get() == 1);
        if (txnEntityId.isByteArrayPKValue) {
            byte[] byteArrayPKValue = new byte[txnEntityId.pkSize];
            buffer.get(byteArrayPKValue);
            txnEntityId.byteArrayPKValue = byteArrayPKValue;
        }
        return txnEntityId;
    }

    public int getCurrentSize() {
        //txn id, dataset id, pkHashValue, arraySize, isByteArrayPKValue
        int size = TxnId.BYTES + DS_LEN + PKHASH_LEN + PKSZ_LEN + Byte.BYTES;
        //byte arraySize
        if (isByteArrayPKValue && byteArrayPKValue != null) {
            size += byteArrayPKValue.length;
        }
        return size;
    }
}
