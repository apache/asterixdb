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

package org.apache.hyracks.storage.am.lsm.rtree.tuples;

import static org.apache.hyracks.storage.am.lsm.common.api.ILSMTreeTupleReference.ANTIMATTER_BIT_OFFSET;

import org.apache.hyracks.api.dataflow.value.ITypeTraits;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;
import org.apache.hyracks.storage.am.common.util.BitOperationUtils;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMTreeTupleReference;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMTreeTupleWriter;
import org.apache.hyracks.storage.am.rtree.tuples.RTreeTypeAwareTupleWriter;
import org.apache.hyracks.util.encoding.VarLenIntEncoderDecoder;

/**
 * This class writes a point mbr as many double values as the number of dimension of the point.
 * For instance, 2d point mbr is stored as two double values (instead of four double values).
 * We assume that a rtree index tuple logically consists of consecutive secondary key fields and then value fields.
 * For instance, conceptually, if a tuple to be written by this class instance consists of
 * 1 mbr field (which is 4 double key fields) and 1 value field as
 * [0.4, 0.3, 0.4, 0.3, 1]
 * the tuple is stored as
 * [0.4, 0.3, 1]
 * where, inputKeyFieldCount, storedKeyFieldCount, valueFieldCount member variables are set to 4, 2, and 1, respectively.
 * Similarly, the associated class RTreeTypeAwareTupleRefereceForPointMBR instance reads
 * the stored point MBR [0.4, 0.3, 1] and generates a tuple reference which is externally shown as [0.4, 0.3, 0.4, 0.3, 1].
 */

public class LSMRTreeTupleWriterForPointMBR extends RTreeTypeAwareTupleWriter implements ILSMTreeTupleWriter {
    private final int inputKeyFieldCount; //double field count for mbr secondary key of an input tuple
    private final int valueFieldCount; //value(or payload or primary key) field count (same for an input tuple and a stored tuple)
    private final int inputTotalFieldCount; //total field count (key + value fields) of an input tuple.
    private final int storedKeyFieldCount; //double field count to be stored for the mbr secondary key
    private final int storedTotalFieldCount; //total field count (key + value fields) of a stored tuple.
    private final boolean antimatterAware;
    private boolean isAntimatter;

    public LSMRTreeTupleWriterForPointMBR(ITypeTraits[] typeTraits, int keyFieldCount, int valueFieldCount,
            boolean antimatterAware, boolean isAntimatter) {
        super(typeTraits);
        this.inputKeyFieldCount = keyFieldCount;
        this.valueFieldCount = valueFieldCount;
        this.inputTotalFieldCount = keyFieldCount + valueFieldCount;
        this.storedKeyFieldCount = keyFieldCount / 2;
        this.storedTotalFieldCount = storedKeyFieldCount + valueFieldCount;
        this.antimatterAware = antimatterAware;
        this.isAntimatter = isAntimatter;
    }

    @Override
    public int bytesRequired(ITupleReference tuple) {
        int bytes = getNullFlagsBytes(tuple) + getFieldSlotsBytes(tuple);
        //key field
        for (int i = 0; i < storedKeyFieldCount; i++) {
            bytes += tuple.getFieldLength(i);
        }
        //value field
        for (int i = inputKeyFieldCount; i < inputTotalFieldCount; i++) {
            bytes += tuple.getFieldLength(i);
        }
        return bytes;
    }

    @Override
    public LSMRTreeTupleReferenceForPointMBR createTupleReference() {
        return new LSMRTreeTupleReferenceForPointMBR(typeTraits, inputKeyFieldCount, valueFieldCount, antimatterAware);
    }

    @Override
    public int writeTuple(ITupleReference tuple, byte[] targetBuf, int targetOff) {
        int runner = targetOff;
        int nullFlagsBytes = getNullFlagsBytes(tuple);
        // write null indicator bits
        for (int i = 0; i < nullFlagsBytes; i++) {
            targetBuf[runner++] = (byte) 0;
        }

        // write field slots for variable length fields which applies only to value fields in RTree
        for (int i = inputKeyFieldCount; i < inputTotalFieldCount; i++) {
            if (!typeTraits[i].isFixedLength()) {
                runner += VarLenIntEncoderDecoder.encode(tuple.getFieldLength(i), targetBuf, runner);
            }
        }

        // write key fields
        for (int i = 0; i < storedKeyFieldCount; i++) {
            System.arraycopy(tuple.getFieldData(i), tuple.getFieldStart(i), targetBuf, runner, tuple.getFieldLength(i));
            runner += tuple.getFieldLength(i);
        }
        // write value fields
        for (int i = inputKeyFieldCount; i < inputTotalFieldCount; i++) {
            System.arraycopy(tuple.getFieldData(i), tuple.getFieldStart(i), targetBuf, runner, tuple.getFieldLength(i));
            runner += tuple.getFieldLength(i);
        }

        //set antimatter bit if necessary
        //this is used when we flush an in-memory rtree into disk
        //and insert anti-matter tuples from in-memory buddy btree into disk rtree
        if (antimatterAware) {
            if (tuple instanceof ILSMTreeTupleReference && ((ILSMTreeTupleReference) tuple).isAntimatter()) {
                setAntimatterBit(targetBuf, targetOff);
            }
        }

        //this is used during creating secondary index operation, where we explicitly insert some antimatter tuple
        if (isAntimatter) {
            setAntimatterBit(targetBuf, targetOff);
        }

        return runner - targetOff;
    }

    @Override
    public int writeTupleFields(ITupleReference tuple, int startField, int numFields, byte[] targetBuf, int targetOff) {
        //*interior frame tuple writer method*
        //this method is used to write only key fields of an tuple for interior frames
        throw new UnsupportedOperationException(
                "writeTupleFields(ITupleReference, int, int, byte[], int) not implemented for RTreeTypeAwareTupleWriterForPointMBR class.");
    }

    @Override
    protected int getNullFlagsBytes(ITupleReference tuple) {
        return BitOperationUtils.getFlagBytes(storedTotalFieldCount + (antimatterAware ? 1 : 0));
    }

    @Override
    protected int getFieldSlotsBytes(ITupleReference tuple) {
        int fieldSlotBytes = 0;
        for (int i = inputKeyFieldCount; i < inputTotalFieldCount; i++) {
            if (!typeTraits[i].isFixedLength()) {
                fieldSlotBytes += VarLenIntEncoderDecoder.getBytesRequired(tuple.getFieldLength(i));
            }
        }
        return fieldSlotBytes;
    }

    @Override
    public int getCopySpaceRequired(ITupleReference tuple) {
        return bytesRequired(tuple);
    }

    protected void setAntimatterBit(byte[] targetBuf, int targetOff) {
        // Set antimatter bit to 1.
        BitOperationUtils.setBit(targetBuf, targetOff, ANTIMATTER_BIT_OFFSET);
    }

    @Override
    public void setAntimatter(boolean isAntimatter) {
        this.isAntimatter = isAntimatter;
    }
}
