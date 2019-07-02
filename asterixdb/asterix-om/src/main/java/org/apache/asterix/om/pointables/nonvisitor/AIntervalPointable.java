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

package org.apache.asterix.om.pointables.nonvisitor;

import java.io.DataOutput;
import java.io.IOException;

import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.EnumDeserializer;
import org.apache.hyracks.api.dataflow.value.ITypeTraits;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.io.IJsonSerializable;
import org.apache.hyracks.api.io.IPersistedResourceRegistry;
import org.apache.hyracks.data.std.api.AbstractPointable;
import org.apache.hyracks.data.std.api.IPointable;
import org.apache.hyracks.data.std.api.IPointableFactory;
import org.apache.hyracks.data.std.primitive.BytePointable;
import org.apache.hyracks.data.std.primitive.IntegerPointable;
import org.apache.hyracks.data.std.primitive.LongPointable;
import org.apache.hyracks.data.std.primitive.VarLengthTypeTrait;

import com.fasterxml.jackson.databind.JsonNode;

/**
 * This class interprets the binary data representation of an interval.
 *
 * Interval {
 *   byte type;
 *   T start;
 *   T end;
 * }
 *
 * T can be of type date, time or datetime.
 */
public class AIntervalPointable extends AbstractPointable {

    public static final AIntervalPointableFactory FACTORY = new AIntervalPointableFactory();

    public static final class AIntervalPointableFactory implements IPointableFactory {
        private static final long serialVersionUID = 1L;

        @Override
        public IPointable createPointable() {
            return new AIntervalPointable();
        }

        @Override
        public ITypeTraits getTypeTraits() {
            return VarLengthTypeTrait.INSTANCE;
        }

        @Override
        public JsonNode toJson(IPersistedResourceRegistry registry) throws HyracksDataException {
            return registry.getClassIdentifier(getClass(), serialVersionUID);
        }

        @SuppressWarnings("squid:S1172") // unused parameter
        public static IJsonSerializable fromJson(IPersistedResourceRegistry registry, JsonNode json) {
            return FACTORY;
        }
    }

    private static final int TAG_SIZE = 1;

    public byte getType() {
        return BytePointable.getByte(bytes, start);
    }

    public ATypeTag getTypeTag() {
        return EnumDeserializer.ATYPETAGDESERIALIZER.deserialize(getType());
    }

    public void getStart(IPointable start) throws HyracksDataException {
        start.set(bytes, getIntervalStartOffset(), getStartEndSize());
    }

    public void getTaggedStart(DataOutput output) throws HyracksDataException {
        try {
            output.writeByte(getType());
            output.write(bytes, getIntervalStartOffset(), getStartEndSize());
        } catch (IOException e) {
            throw HyracksDataException.create(e);
        }
    }

    private int getIntervalStartOffset() {
        return start + TAG_SIZE;
    }

    public long getStartValue() throws HyracksDataException {
        switch (getTypeTag()) {
            case DATE:
            case TIME:
                return IntegerPointable.getInteger(bytes, getIntervalStartOffset());
            case DATETIME:
                return LongPointable.getLong(bytes, getIntervalStartOffset());
            default:
                throw new HyracksDataException("Unsupported interval type: " + getTypeTag() + ".");
        }
    }

    public void getEnd(IPointable end) throws HyracksDataException {
        end.set(bytes, getEndOffset(), getStartEndSize());
    }

    public void getTaggedEnd(DataOutput output) throws HyracksDataException {
        try {
            output.writeByte(getType());
            output.write(bytes, getEndOffset(), getStartEndSize());
        } catch (IOException e) {
            throw HyracksDataException.create(e);
        }
    }

    private int getEndOffset() throws HyracksDataException {
        return getIntervalStartOffset() + getStartEndSize();
    }

    public long getEndValue() throws HyracksDataException {
        switch (getTypeTag()) {
            case DATE:
            case TIME:
                return IntegerPointable.getInteger(bytes, getEndOffset());
            case DATETIME:
                return LongPointable.getLong(bytes, getEndOffset());
            default:
                throw new HyracksDataException("Unsupported interval type: " + getTypeTag() + ".");
        }
    }

    private int getStartEndSize() throws HyracksDataException {
        switch (getTypeTag()) {
            case DATE:
            case TIME:
                return Integer.BYTES;
            case DATETIME:
                return Long.BYTES;
            default:
                throw new HyracksDataException("Unsupported interval type: " + getTypeTag() + ".");
        }
    }
}
