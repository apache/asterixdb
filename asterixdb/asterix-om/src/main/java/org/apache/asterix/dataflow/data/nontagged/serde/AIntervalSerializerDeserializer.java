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
package org.apache.asterix.dataflow.data.nontagged.serde;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.asterix.om.base.AInterval;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.hyracks.api.dataflow.value.ISerializerDeserializer;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.primitive.BytePointable;
import org.apache.hyracks.data.std.primitive.IntegerPointable;
import org.apache.hyracks.data.std.primitive.LongPointable;

/**
 * This class serializes and de-serializes the binary data representation of an interval.
 *
 * Interval {
 *   byte type;
 *   T start;
 *   T end;
 * }
 *
 * T can be of type date, time or datetime.
 */
public class AIntervalSerializerDeserializer implements ISerializerDeserializer<AInterval> {

    private static final long serialVersionUID = 1L;

    public static final AIntervalSerializerDeserializer INSTANCE = new AIntervalSerializerDeserializer();

    private AIntervalSerializerDeserializer() {
    }

    @Override
    public AInterval deserialize(DataInput in) throws HyracksDataException {
        try {
            byte tag = in.readByte();
            long start, end;
            if (tag == ATypeTag.DATETIME.serialize()) {
                start = in.readLong();
                end = in.readLong();
            } else {
                start = in.readInt();
                end = in.readInt();
            }
            return new AInterval(start, end, tag);
        } catch (IOException e) {
            throw HyracksDataException.create(e);
        }

    }

    @Override
    public void serialize(AInterval instance, DataOutput out) throws HyracksDataException {
        try {
            out.writeByte(instance.getIntervalType());
            if (instance.getIntervalType() == ATypeTag.DATETIME.serialize()) {
                out.writeLong(instance.getIntervalStart());
                out.writeLong(instance.getIntervalEnd());
            } else {
                out.writeInt((int) instance.getIntervalStart());
                out.writeInt((int) instance.getIntervalEnd());
            }
        } catch (IOException e) {
            throw HyracksDataException.create(e);
        }
    }

    public static byte getIntervalTimeType(byte[] data, int start) {
        return BytePointable.getByte(data, start);
    }

    private static int getTypeSize() {
        return Byte.BYTES;
    }

    public static long getIntervalStart(byte[] data, int start) {
        if (getIntervalTimeType(data, start) == ATypeTag.DATETIME.serialize()) {
            return LongPointable.getLong(data, getIntervalStartOffset(start));
        } else {
            return IntegerPointable.getInteger(data, getIntervalStartOffset(start));
        }
    }

    public static int getIntervalStartOffset(int start) {
        return start + getTypeSize();
    }

    public static int getStartSize(byte[] data, int start) {
        if (getIntervalTimeType(data, start) == ATypeTag.DATETIME.serialize()) {
            return Long.BYTES;
        } else {
            return Integer.BYTES;
        }
    }

    public static long getIntervalEnd(byte[] data, int start) {
        if (getIntervalTimeType(data, start) == ATypeTag.DATETIME.serialize()) {
            return LongPointable.getLong(data, getIntervalEndOffset(data, start));
        } else {
            return IntegerPointable.getInteger(data, getIntervalEndOffset(data, start));
        }
    }

    public static int getIntervalEndOffset(byte[] data, int start) {
        return getIntervalStartOffset(start) + getStartSize(data, start);
    }

    public static int getEndSize(byte[] data, int start) {
        if (getIntervalTimeType(data, start) == ATypeTag.DATETIME.serialize()) {
            return Long.BYTES;
        } else {
            return Integer.BYTES;
        }
    }

    public static int getIntervalLength(byte[] data, int start) {
        return getTypeSize() + getStartSize(data, start) + getEndSize(data, start);
    }
}
