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

package org.apache.hyracks.storage.am.lsm.common.frames;

import org.apache.hyracks.data.std.api.IValueReference;
import org.apache.hyracks.data.std.primitive.BooleanPointable;
import org.apache.hyracks.data.std.primitive.IntegerPointable;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;
import org.apache.hyracks.storage.am.common.api.ITreeIndexTupleReference;
import org.apache.hyracks.storage.am.common.api.ITreeIndexTupleWriter;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMComponentFilterReference;

/**
 * When written to disk:
 * [min set?][max set?][min length][min][max length][max]
 */
public class LSMComponentFilterReference implements ILSMComponentFilterReference {

    private static final int MIN_SET_INDICATOR_OFFSET = 0;
    private static final int MAX_SET_INDICATOR_OFFSET = 1;

    private final ArrayBackedValueStorage min;
    private final ArrayBackedValueStorage max;
    private ArrayBackedValueStorage binaryFilter;
    private final ITreeIndexTupleWriter tupleWriter;
    private ITreeIndexTupleReference minTuple;
    private ITreeIndexTupleReference maxTuple;

    public LSMComponentFilterReference(ITreeIndexTupleWriter tupleWriter) {
        this.tupleWriter = tupleWriter;
        min = new ArrayBackedValueStorage();
        max = new ArrayBackedValueStorage();
        binaryFilter = new ArrayBackedValueStorage();
        minTuple = tupleWriter.createTupleReference();
        maxTuple = tupleWriter.createTupleReference();
    }

    @Override
    public void writeMinTuple(ITupleReference tuple) {
        binaryFilter.reset();
        min.reset();
        min.setSize(tupleWriter.bytesRequired(tuple));
        tupleWriter.writeTuple(tuple, min.getByteArray(), 0);
    }

    @Override
    public void writeMaxTuple(ITupleReference tuple) {
        binaryFilter.reset();
        max.reset();
        max.setSize(tupleWriter.bytesRequired(tuple));
        tupleWriter.writeTuple(tuple, max.getByteArray(), 0);
    }

    @Override
    public boolean isMinTupleSet() {
        return min.getLength() > 0;
    }

    @Override
    public boolean isMaxTupleSet() {
        return max.getLength() > 0;
    }

    @Override
    public ITupleReference getMinTuple() {
        minTuple.resetByTupleOffset(min.getByteArray(), 0);
        return minTuple;
    }

    @Override
    public ITupleReference getMaxTuple() {
        maxTuple.resetByTupleOffset(max.getByteArray(), 0);
        return maxTuple;
    }

    @Override
    public byte[] getByteArray() {
        if (binaryFilter.getLength() == 0) {
            int binarySize = 2;
            if (min.getLength() > 0) {
                binarySize += Integer.BYTES + min.getLength();
            }
            if (max.getLength() > 0) {
                binarySize += Integer.BYTES + max.getLength();
            }
            binaryFilter.setSize(binarySize);
            byte[] buf = binaryFilter.getByteArray();
            BooleanPointable.setBoolean(buf, MIN_SET_INDICATOR_OFFSET, min.getLength() > 0);
            BooleanPointable.setBoolean(buf, MAX_SET_INDICATOR_OFFSET, max.getLength() > 0);
            int offset = 2;
            if (min.getLength() > 0) {
                IntegerPointable.setInteger(buf, offset, min.getLength());
                offset += Integer.BYTES;
                System.arraycopy(min.getByteArray(), 0, buf, offset, min.getLength());
                offset += min.getLength();
            }
            if (max.getLength() > 0) {
                IntegerPointable.setInteger(buf, offset, max.getLength());
                offset += Integer.BYTES;
                System.arraycopy(max.getByteArray(), 0, buf, offset, max.getLength());
            }
        }
        return binaryFilter.getByteArray();
    }

    @Override
    public int getStartOffset() {
        return 0;
    }

    @Override
    public int getLength() {
        if (binaryFilter.getLength() <= 0 && (isMinTupleSet() || isMaxTupleSet())) {
            getByteArray();
        }
        return binaryFilter.getLength();
    }

    @Override
    public void reset() {
        min.reset();
        max.reset();
        binaryFilter.reset();
    }

    @Override
    public void set(byte[] bytes, int start, int length) {
        boolean isMinSet = BooleanPointable.getBoolean(bytes, MIN_SET_INDICATOR_OFFSET + start);
        boolean isMaxSet = BooleanPointable.getBoolean(bytes, MAX_SET_INDICATOR_OFFSET + start);
        int srcOffset = start + 2;
        if (isMinSet) {
            min.setSize(IntegerPointable.getInteger(bytes, srcOffset));
            srcOffset += Integer.BYTES;
            System.arraycopy(bytes, srcOffset, min.getByteArray(), 0, min.getLength());
            srcOffset += min.getLength();
        }
        if (isMaxSet) {
            max.setSize(IntegerPointable.getInteger(bytes, srcOffset));
            srcOffset += Integer.BYTES;
            System.arraycopy(bytes, srcOffset, max.getByteArray(), 0, max.getLength());
        }
        binaryFilter.reset();
    }

    @Override
    public void set(IValueReference pointer) {
        set(pointer.getByteArray(), pointer.getStartOffset(), pointer.getLength());
    }
}
