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

package edu.uci.ics.hyracks.storage.am.common.tuples;

import java.nio.ByteBuffer;

import edu.uci.ics.hyracks.dataflow.common.data.accessors.ITupleReference;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndexTupleReference;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndexTupleWriter;

public class SimpleTupleWriter implements ITreeIndexTupleWriter {

    // Write short in little endian to target byte array at given offset.
    private static void writeShortL(short s, byte[] buf, int targetOff) {
        buf[targetOff] = (byte) (s >> 8);
        buf[targetOff + 1] = (byte) (s >> 0);
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
    public ITreeIndexTupleReference createTupleReference() {
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
            writeShortL((short) fieldEndOff, targetBuf, targetOff + nullFlagsBytes + i * 2);
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
            writeShortL((short) fieldEndOff, targetBuf, targetOff + nullFlagsBytes + fieldCounter * 2);
            fieldCounter++;
        }

        return runner - targetOff;
    }

    protected int getNullFlagsBytes(ITupleReference tuple) {
        return (int) Math.ceil((double) tuple.getFieldCount() / 8.0);
    }

    protected int getFieldSlotsBytes(ITupleReference tuple) {
        return tuple.getFieldCount() * 2;
    }

    protected int getNullFlagsBytes(ITupleReference tuple, int startField, int numFields) {
        return (int) Math.ceil((double) numFields / 8.0);
    }

    protected int getFieldSlotsBytes(ITupleReference tuple, int startField, int numFields) {
        return numFields * 2;
    }

    @Override
    public int getCopySpaceRequired(ITupleReference tuple) {
        return bytesRequired(tuple);
    }
}
