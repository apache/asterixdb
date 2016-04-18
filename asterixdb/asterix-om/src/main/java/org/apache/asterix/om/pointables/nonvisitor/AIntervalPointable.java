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

import org.apache.asterix.common.exceptions.AsterixException;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.EnumDeserializer;
import org.apache.asterix.om.util.container.IObjectFactory;
import org.apache.hyracks.api.dataflow.value.ITypeTraits;
import org.apache.hyracks.data.std.api.AbstractPointable;
import org.apache.hyracks.data.std.api.IPointable;
import org.apache.hyracks.data.std.api.IPointableFactory;
import org.apache.hyracks.data.std.primitive.BytePointable;
import org.apache.hyracks.data.std.primitive.IntegerPointable;
import org.apache.hyracks.data.std.primitive.LongPointable;

/*
 * This class interprets the binary data representation of an interval.
 *
 * Interval {
 *   byte type;
 *   IPointable start;
 *   IPointable end;
 * }
 */
public class AIntervalPointable extends AbstractPointable {

    public static final ITypeTraits TYPE_TRAITS = new ITypeTraits() {
        private static final long serialVersionUID = 1L;

        @Override
        public boolean isFixedLength() {
            return false;
        }

        @Override
        public int getFixedLength() {
            return 0;
        }
    };

    public static final IPointableFactory FACTORY = new IPointableFactory() {
        private static final long serialVersionUID = 1L;

        @Override
        public IPointable createPointable() {
            return new AIntervalPointable();
        }

        @Override
        public ITypeTraits getTypeTraits() {
            return TYPE_TRAITS;
        }
    };

    public static final IObjectFactory<IPointable, ATypeTag> ALLOCATOR = new IObjectFactory<IPointable, ATypeTag>() {
        @Override
        public IPointable create(ATypeTag type) {
            return new AIntervalPointable();
        }
    };

    private static final int TAG_SIZE = 1;

    public byte getType() {
        return BytePointable.getByte(bytes, getTypeOffset());
    }

    public ATypeTag getTypeTag() {
        return EnumDeserializer.ATYPETAGDESERIALIZER.deserialize(getType());
    }

    public int getTypeOffset() {
        return start;
    }

    public int getTypeSize() {
        return TAG_SIZE;
    }

    public void getStart(IPointable start) throws AsterixException {
        start.set(bytes, getStartOffset(), getStartSize());
    }

    public int getStartOffset() {
        return getTypeOffset() + getTypeSize();
    }

    public int getStartSize() throws AsterixException {
        switch (getTypeTag()) {
            case DATE:
            case TIME:
                return Integer.BYTES;
            case DATETIME:
                return Long.BYTES;
            default:
                throw new AsterixException("Unsupported interval type: " + getTypeTag() + ".");
        }
    }

    public long getStartValue() throws AsterixException {
        switch (getTypeTag()) {
            case DATE:
            case TIME:
                return IntegerPointable.getInteger(bytes, getStartOffset());
            case DATETIME:
                return LongPointable.getLong(bytes, getStartOffset());
            default:
                throw new AsterixException("Unsupported interval type: " + getTypeTag() + ".");
        }
    }

    public void getEnd(IPointable start) throws AsterixException {
        start.set(bytes, getEndOffset(), getEndSize());
    }

    public int getEndOffset() throws AsterixException {
        return getStartOffset() + getStartSize();
    }

    public int getEndSize() throws AsterixException {
        switch (getTypeTag()) {
            case DATE:
            case TIME:
                return Integer.BYTES;
            case DATETIME:
                return Long.BYTES;
            default:
                throw new AsterixException("Unsupported interval type: " + getTypeTag() + ".");
        }
    }

    public long getEndValue() throws AsterixException {
        switch (getTypeTag()) {
            case DATE:
            case TIME:
                return IntegerPointable.getInteger(bytes, getEndOffset());
            case DATETIME:
                return LongPointable.getLong(bytes, getEndOffset());
            default:
                throw new AsterixException("Unsupported interval type: " + getTypeTag() + ".");
        }
    }

}
