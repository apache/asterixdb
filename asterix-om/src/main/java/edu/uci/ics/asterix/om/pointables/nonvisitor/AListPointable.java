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

package edu.uci.ics.asterix.om.pointables.nonvisitor;

import java.io.DataOutput;
import java.io.IOException;

import edu.uci.ics.asterix.common.exceptions.AsterixException;
import edu.uci.ics.asterix.om.types.AOrderedListType;
import edu.uci.ics.asterix.om.types.ATypeTag;
import edu.uci.ics.asterix.om.types.AbstractCollectionType;
import edu.uci.ics.asterix.om.util.NonTaggedFormatUtil;
import edu.uci.ics.asterix.om.util.container.IObjectFactory;
import edu.uci.ics.hyracks.api.dataflow.value.ITypeTraits;
import edu.uci.ics.hyracks.data.std.api.AbstractPointable;
import edu.uci.ics.hyracks.data.std.api.IPointable;
import edu.uci.ics.hyracks.data.std.api.IPointableFactory;
import edu.uci.ics.hyracks.data.std.primitive.BytePointable;
import edu.uci.ics.hyracks.data.std.primitive.IntegerPointable;

/*
 * This class interprets the binary data representation of a list.
 * 
 * List {
 *   byte type;
 *   int length;
 *   int numberOfItems;
 *   int[numberOfItems] itemOffset;
 *   IPointable[numberOfItems] item;
 */
public class AListPointable extends AbstractPointable {

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
            return new AListPointable();
        }

        @Override
        public ITypeTraits getTypeTraits() {
            return TYPE_TRAITS;
        }
    };

    public static final IObjectFactory<IPointable, String> ALLOCATOR = new IObjectFactory<IPointable, String>() {
        public IPointable create(String id) {
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

    public int getFixedLength(AbstractCollectionType inputType) throws AsterixException {
        return NonTaggedFormatUtil.getFieldValueLength(bytes, 0, inputType.getItemType().getTypeTag(), false);
    }

    public boolean isOrdered(AbstractCollectionType inputType) {
        if (inputType instanceof AOrderedListType) {
            return true;
        }
        return false;
    }

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

    public int getItemOffset(AbstractCollectionType inputType, int index) throws AsterixException {
        if (isFixedType(inputType)) {
            return getItemCountOffset() + getItemCountSize() + index * getFixedLength(inputType);
        } else {
            int offset = getItemCountOffset() + getItemCountSize() + index * ITEM_OFFSET_SIZE;
            return IntegerPointable.getInteger(bytes, offset);
        }
    }

    public byte getItemTag(AbstractCollectionType inputType, int index) throws AsterixException {
        if (getType() != ATypeTag.ANY.serialize()) {
            return getType();
        }
        return bytes[getItemOffset(inputType, index)];
    }

    public int getItemSize(AbstractCollectionType inputType, int index) throws AsterixException {
        if (isFixedType(inputType)) {
            return getFixedLength(inputType);
        } else {
            int nextOffset = (getItemCount() > index + 1) ? getItemOffset(inputType, index + 1) : getLength();
            return nextOffset - getItemOffset(inputType, index);
        }
    }

    public void getItemValue(AbstractCollectionType inputType, int index, DataOutput dOut) throws IOException,
            AsterixException {
        if (getType() != ATypeTag.ANY.serialize()) {
            dOut.writeByte(getType());
        }
        dOut.write(bytes, getItemOffset(inputType, index), getItemSize(inputType, index));
    }

}
