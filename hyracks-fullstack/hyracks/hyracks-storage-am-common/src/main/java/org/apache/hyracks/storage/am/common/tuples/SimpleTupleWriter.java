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

package org.apache.hyracks.storage.am.common.tuples;

import java.nio.ByteBuffer;

import org.apache.hyracks.data.std.primitive.IntegerPointable;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;
import org.apache.hyracks.storage.am.common.api.ITreeIndexTupleWriter;
import org.apache.hyracks.storage.am.common.util.BitOperationUtils;

/*
 * This class should be replaced by a Util class
 */
public class SimpleTupleWriter implements ITreeIndexTupleWriter {

    public static final SimpleTupleWriter INSTANCE = new SimpleTupleWriter();

    private SimpleTupleWriter() {
    }

    @Override
    public int bytesRequired(ITupleReference tuple) {
        int bytes = getNullFlagsBytes(tuple) + getFieldSlotsBytes(tuple);
        for (int i = 0; i < tuple.getFieldCount(); i++) {
            bytes += tuple.getFieldLength(i);
        }
        return bytes;
    }

    @Override
    public int bytesRequired(ITupleReference tuple, int startField, int numFields) {
        int bytes = getNullFlagsBytes(tuple, startField, numFields) + getFieldSlotsBytes(tuple, startField, numFields);
        for (int i = startField; i < startField + numFields; i++) {
            bytes += tuple.getFieldLength(i);
        }
        return bytes;
    }

    @Override
    public SimpleTupleReference createTupleReference() {
        return new SimpleTupleReference();
    }

    @Override
    public int writeTuple(ITupleReference tuple, ByteBuffer targetBuf, int targetOff) {
        return writeTuple(tuple, targetBuf.array(), targetOff);
    }

    @Override
    public int writeTuple(ITupleReference tuple, byte[] targetBuf, int targetOff) {
        int runner = targetOff;
        int nullFlagsBytes = getNullFlagsBytes(tuple);
        int fieldSlotsBytes = getFieldSlotsBytes(tuple);
        for (int i = 0; i < nullFlagsBytes; i++) {
            targetBuf[runner++] = (byte) 0;
        }
        runner += fieldSlotsBytes;
        int fieldEndOff = 0;
        for (int i = 0; i < tuple.getFieldCount(); i++) {
            System.arraycopy(tuple.getFieldData(i), tuple.getFieldStart(i), targetBuf, runner, tuple.getFieldLength(i));
            fieldEndOff += tuple.getFieldLength(i);
            runner += tuple.getFieldLength(i);
            IntegerPointable.setInteger(targetBuf, targetOff + nullFlagsBytes + i * Integer.BYTES, fieldEndOff);
        }
        return runner - targetOff;
    }

    @Override
    public int writeTupleFields(ITupleReference tuple, int startField, int numFields, byte[] targetBuf, int targetOff) {
        int runner = targetOff;
        int nullFlagsBytes = getNullFlagsBytes(tuple, startField, numFields);
        for (int i = 0; i < nullFlagsBytes; i++) {
            targetBuf[runner++] = (byte) 0;
        }
        runner += getFieldSlotsBytes(tuple, startField, numFields);

        int fieldEndOff = 0;
        int fieldCounter = 0;
        for (int i = startField; i < startField + numFields; i++) {
            System.arraycopy(tuple.getFieldData(i), tuple.getFieldStart(i), targetBuf, runner, tuple.getFieldLength(i));
            fieldEndOff += tuple.getFieldLength(i);
            runner += tuple.getFieldLength(i);
            IntegerPointable.setInteger(targetBuf, targetOff + nullFlagsBytes + fieldCounter * Integer.BYTES,
                    fieldEndOff);
            fieldCounter++;
        }

        return runner - targetOff;
    }

    protected int getNullFlagsBytes(ITupleReference tuple) {
        return BitOperationUtils.getFlagBytes(tuple.getFieldCount());
    }

    protected int getFieldSlotsBytes(ITupleReference tuple) {
        return tuple.getFieldCount() * Integer.BYTES;
    }

    protected int getNullFlagsBytes(ITupleReference tuple, int startField, int numFields) {
        return BitOperationUtils.getFlagBytes(numFields);
    }

    protected int getFieldSlotsBytes(ITupleReference tuple, int startField, int numFields) {
        return numFields * Integer.BYTES;
    }

    @Override
    public int getCopySpaceRequired(ITupleReference tuple) {
        return bytesRequired(tuple);
    }
}
