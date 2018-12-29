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

package org.apache.hyracks.dataflow.common.utils;

import java.io.ByteArrayInputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;

import org.apache.hyracks.api.dataflow.value.ISerializerDeserializer;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import org.apache.hyracks.dataflow.common.comm.io.ArrayTupleReference;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;
import org.apache.hyracks.dataflow.common.data.marshalling.DoubleSerializerDeserializer;
import org.apache.hyracks.dataflow.common.data.marshalling.IntegerSerializerDeserializer;

@SuppressWarnings("rawtypes")
public class TupleUtils {
    @SuppressWarnings("unchecked")
    public static void createTuple(ArrayTupleBuilder tupleBuilder, ArrayTupleReference tuple,
            ISerializerDeserializer[] fieldSerdes, boolean filtered, final Object... fields)
            throws HyracksDataException {
        DataOutput dos = tupleBuilder.getDataOutput();
        tupleBuilder.reset();
        int numFields = Math.min(tupleBuilder.getFieldEndOffsets().length, fields.length);
        for (int i = 0; i < numFields; i++) {
            fieldSerdes[i].serialize(fields[i], dos);
            tupleBuilder.addFieldEndOffset();
        }
        if (filtered) {
            fieldSerdes[0].serialize(fields[0], dos);
            tupleBuilder.addFieldEndOffset();
        }
        tuple.reset(tupleBuilder.getFieldEndOffsets(), tupleBuilder.getByteArray());
    }

    public static void createTuple(ArrayTupleBuilder tupleBuilder, ArrayTupleReference tuple,
            ISerializerDeserializer[] fieldSerdes, final Object... fields) throws HyracksDataException {
        createTuple(tupleBuilder, tuple, fieldSerdes, false, fields);

    }

    public static ITupleReference createTuple(ISerializerDeserializer[] fieldSerdes, final Object... fields)
            throws HyracksDataException {
        ArrayTupleBuilder tupleBuilder = new ArrayTupleBuilder(fields.length);
        ArrayTupleReference tuple = new ArrayTupleReference();
        createTuple(tupleBuilder, tuple, fieldSerdes, fields);
        return tuple;
    }

    public static void createIntegerTuple(ArrayTupleBuilder tupleBuilder, ArrayTupleReference tuple, boolean filtered,
            final int... fields) throws HyracksDataException {
        DataOutput dos = tupleBuilder.getDataOutput();
        tupleBuilder.reset();
        for (final int i : fields) {
            IntegerSerializerDeserializer.INSTANCE.serialize(i, dos);
            tupleBuilder.addFieldEndOffset();
        }
        if (filtered) {
            IntegerSerializerDeserializer.INSTANCE.serialize(fields[0], dos);
            tupleBuilder.addFieldEndOffset();
        }
        tuple.reset(tupleBuilder.getFieldEndOffsets(), tupleBuilder.getByteArray());
    }

    public static void createIntegerTuple(ArrayTupleBuilder tupleBuilder, ArrayTupleReference tuple,
            final int... fields) throws HyracksDataException {
        createIntegerTuple(tupleBuilder, tuple, false, fields);
    }

    public static ITupleReference createIntegerTuple(boolean filtered, final int... fields)
            throws HyracksDataException {
        ArrayTupleBuilder tupleBuilder =
                filtered ? new ArrayTupleBuilder(fields.length + 1) : new ArrayTupleBuilder(fields.length);
        ArrayTupleReference tuple = new ArrayTupleReference();
        createIntegerTuple(tupleBuilder, tuple, fields);
        return tuple;
    }

    public static ITupleReference createIntegerTuple(final int... fields) throws HyracksDataException {
        return createIntegerTuple(false, fields);
    }

    public static void createDoubleTuple(ArrayTupleBuilder tupleBuilder, ArrayTupleReference tuple,
            final double... fields) throws HyracksDataException {
        DataOutput dos = tupleBuilder.getDataOutput();
        tupleBuilder.reset();
        for (final double i : fields) {
            DoubleSerializerDeserializer.INSTANCE.serialize(i, dos);
            tupleBuilder.addFieldEndOffset();
        }
        tuple.reset(tupleBuilder.getFieldEndOffsets(), tupleBuilder.getByteArray());
    }

    public static ITupleReference createDoubleTuple(final double... fields) throws HyracksDataException {
        ArrayTupleBuilder tupleBuilder = new ArrayTupleBuilder(fields.length);
        ArrayTupleReference tuple = new ArrayTupleReference();
        createDoubleTuple(tupleBuilder, tuple, fields);
        return tuple;
    }

    public static String printTuple(ITupleReference tuple, ISerializerDeserializer[] fields)
            throws HyracksDataException {
        StringBuilder strBuilder = new StringBuilder();
        int numPrintFields = Math.min(tuple.getFieldCount(), fields.length);
        for (int i = 0; i < numPrintFields; i++) {
            ByteArrayInputStream inStream =
                    new ByteArrayInputStream(tuple.getFieldData(i), tuple.getFieldStart(i), tuple.getFieldLength(i));
            DataInput dataIn = new DataInputStream(inStream);
            Object o = fields[i].deserialize(dataIn);
            strBuilder.append(o.toString());
            if (i != fields.length - 1) {
                strBuilder.append(" ");
            }
        }
        return strBuilder.toString();
    }

    public static Object[] deserializeTuple(ITupleReference tuple, ISerializerDeserializer[] fields)
            throws HyracksDataException {
        int numFields = Math.min(tuple.getFieldCount(), fields.length);
        Object[] objs = new Object[numFields];
        for (int i = 0; i < numFields; i++) {
            ByteArrayInputStream inStream =
                    new ByteArrayInputStream(tuple.getFieldData(i), tuple.getFieldStart(i), tuple.getFieldLength(i));
            DataInput dataIn = new DataInputStream(inStream);
            objs[i] = fields[i].deserialize(dataIn);
        }
        return objs;
    }

    public static ITupleReference copyTuple(ITupleReference tuple) throws HyracksDataException {
        ArrayTupleBuilder tupleBuilder = new ArrayTupleBuilder(tuple.getFieldCount());
        for (int i = 0; i < tuple.getFieldCount(); i++) {
            tupleBuilder.addField(tuple.getFieldData(i), tuple.getFieldStart(i), tuple.getFieldLength(i));
        }
        ArrayTupleReference tupleCopy = new ArrayTupleReference();
        tupleCopy.reset(tupleBuilder.getFieldEndOffsets(), tupleBuilder.getByteArray());
        return tupleCopy;
    }

    public static void copyTuple(ArrayTupleBuilder tupleBuilder, ITupleReference tuple, int numFields)
            throws HyracksDataException {
        tupleBuilder.reset();
        for (int i = 0; i < numFields; i++) {
            tupleBuilder.addField(tuple.getFieldData(i), tuple.getFieldStart(i), tuple.getFieldLength(i));
        }
    }

    public static boolean equalTuples(ITupleReference tuple1, ITupleReference tuple2, int numCmpFields) {
        for (int i = 0; i < numCmpFields; i++) {
            if (!equalFields(tuple1, tuple2, i)) {
                return false;
            }
        }
        return true;
    }

    public static boolean equalFields(ITupleReference tuple1, ITupleReference tuple2, int fIdx) {
        return equalFields(tuple1.getFieldData(fIdx), tuple1.getFieldStart(fIdx), tuple1.getFieldLength(fIdx),
                tuple2.getFieldData(fIdx), tuple2.getFieldStart(fIdx), tuple2.getFieldLength(fIdx));
    }

    public static boolean equalFields(byte[] a, int aOffset, int aLength, byte[] b, int bOffset, int bLength) {
        if (aLength != bLength) {
            return false;
        }
        for (int i = 0; i < aLength; i++) {
            if (a[aOffset + i] != b[bOffset + i]) {
                return false;
            }
        }
        return true;
    }

    public static void addFields(ArrayTupleBuilder sourceBuilder, ArrayTupleBuilder targetBuilder)
            throws HyracksDataException {
        byte[] data = sourceBuilder.getByteArray();
        int[] fieldEnds = sourceBuilder.getFieldEndOffsets();
        int start = 0;
        int offset;
        for (int i = 0; i < fieldEnds.length; i++) {
            if (i > 0) {
                start = fieldEnds[i - 1];
            }
            offset = fieldEnds[i] - start;
            targetBuilder.addField(data, start, offset);
        }
    }
}
