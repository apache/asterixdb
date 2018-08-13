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

import org.apache.asterix.om.types.AOrderedListType;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.AbstractCollectionType;
import org.apache.asterix.om.util.container.IObjectFactory;
import org.apache.asterix.om.utils.NonTaggedFormatUtil;
import org.apache.hyracks.api.dataflow.value.ITypeTraits;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.io.IJsonSerializable;
import org.apache.hyracks.api.io.IPersistedResourceRegistry;
import org.apache.hyracks.data.std.api.AbstractPointable;
import org.apache.hyracks.data.std.api.IPointable;
import org.apache.hyracks.data.std.api.IPointableFactory;
import org.apache.hyracks.data.std.primitive.BytePointable;
import org.apache.hyracks.data.std.primitive.IntegerPointable;
import org.apache.hyracks.data.std.primitive.VarLengthTypeTrait;

import com.fasterxml.jackson.databind.JsonNode;

/*
 * This class interprets the binary data representation of a list.
 *
 * List {
 *   byte tag;
 *   byte type;
 *   int length;
 *   int numberOfItems;
 *   int[numberOfItems] itemOffset;
 *   IPointable[numberOfItems] item;
 */
public class AListPointable extends AbstractPointable {

    public static final AListPointableFactory FACTORY = new AListPointableFactory();

    public static final class AListPointableFactory implements IPointableFactory {
        private static final long serialVersionUID = 1L;

        private AListPointableFactory() {
        }

        @Override
        public AListPointable createPointable() {
            return new AListPointable();
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

    public static final IObjectFactory<IPointable, ATypeTag> ALLOCATOR = new IObjectFactory<IPointable, ATypeTag>() {
        @Override
        public IPointable create(ATypeTag type) {
            return new AListPointable();
        }
    };

    private static final int TAG_SIZE = 1;
    private static final int TYPE_SIZE = 1;
    private static final int LENGTH_SIZE = 4;
    private static final int ITEM_COUNT_SIZE = 4;
    private static final int ITEM_OFFSET_SIZE = 4;

    public byte getTag() {
        return BytePointable.getByte(bytes, getTagOffset());
    }

    public int getTagOffset() {
        return start;
    }

    public int getTagSize() {
        return TAG_SIZE;
    }

    public byte getType() {
        return BytePointable.getByte(bytes, getTypeOffset());
    }

    public int getTypeOffset() {
        return getTagOffset() + getTagSize();
    }

    public int getTypeSize() {
        return TYPE_SIZE;
    }

    public boolean isTyped() {
        if (getType() != ATypeTag.ANY.serialize()) {
            return true;
        }
        return false;
    }

    public boolean isFixedType(AbstractCollectionType inputType) {
        return NonTaggedFormatUtil.isFixedSizedCollection(inputType.getItemType());
    }

    public int getFixedLength(AbstractCollectionType inputType) throws HyracksDataException {
        return NonTaggedFormatUtil.getFieldValueLength(bytes, 0, inputType.getItemType().getTypeTag(), false);
    }

    public boolean isOrdered(AbstractCollectionType inputType) {
        if (inputType instanceof AOrderedListType) {
            return true;
        }
        return false;
    }

    @Override
    public int getLength() {
        return IntegerPointable.getInteger(bytes, getLengthOffset());
    }

    public int getLengthOffset() {
        return getTypeOffset() + getTypeSize();
    }

    public int getLengthSize() {
        return LENGTH_SIZE;
    }

    public int getItemCount() {
        return IntegerPointable.getInteger(bytes, getItemCountOffset());
    }

    public int getItemCountOffset() {
        return getLengthOffset() + getLengthSize();
    }

    public int getItemCountSize() {
        return ITEM_COUNT_SIZE;
    }

    // ----------------------
    // Item accessors
    // ----------------------

    public int getItemOffset(AbstractCollectionType inputType, int index) throws HyracksDataException {
        if (isFixedType(inputType)) {
            return getItemCountOffset() + getItemCountSize() + index * getFixedLength(inputType);
        } else {
            int offset = getItemCountOffset() + getItemCountSize() + index * ITEM_OFFSET_SIZE;
            return start + IntegerPointable.getInteger(bytes, offset);
        }
    }

    public byte getItemTag(AbstractCollectionType inputType, int index) throws HyracksDataException {
        if (getType() != ATypeTag.ANY.serialize()) {
            return getType();
        }
        return bytes[getItemOffset(inputType, index)];
    }

    public int getItemSize(AbstractCollectionType inputType, int index) throws HyracksDataException {
        if (isFixedType(inputType)) {
            return getFixedLength(inputType);
        } else {
            int nextOffset = (getItemCount() > index + 1) ? getItemOffset(inputType, index + 1) : getLength();
            return nextOffset - getItemOffset(inputType, index);
        }
    }

    public void getItemValue(AbstractCollectionType inputType, int index, DataOutput dOut) throws IOException {
        if (getType() != ATypeTag.ANY.serialize()) {
            dOut.writeByte(getType());
        }
        dOut.write(bytes, getItemOffset(inputType, index), getItemSize(inputType, index));
    }
}
