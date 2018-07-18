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

package org.apache.asterix.runtime.evaluators.common;

import java.io.DataOutput;
import java.io.IOException;

import org.apache.asterix.dataflow.data.nontagged.serde.AOrderedListSerializerDeserializer;
import org.apache.asterix.dataflow.data.nontagged.serde.AUnorderedListSerializerDeserializer;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.EnumDeserializer;
import org.apache.asterix.om.utils.NonTaggedFormatUtil;
import org.apache.asterix.runtime.exceptions.TypeMismatchException;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.api.IPointable;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;

/**
 * Utility class for accessing serialized unordered and ordered lists.
 */
public class ListAccessor {

    protected byte[] listBytes;
    protected int start;
    protected ATypeTag listType;
    protected ATypeTag itemType;
    protected int size;

    public ATypeTag getListType() {
        return listType;
    }

    public ATypeTag getItemType() {
        return itemType;
    }

    public boolean itemsAreSelfDescribing() {
        return itemType == ATypeTag.ANY;
    }

    public void reset(byte[] listBytes, int start) throws HyracksDataException {
        this.listBytes = listBytes;
        this.start = start;
        byte typeTag = listBytes[start];
        if (typeTag != ATypeTag.SERIALIZED_UNORDEREDLIST_TYPE_TAG
                && typeTag != ATypeTag.SERIALIZED_ORDEREDLIST_TYPE_TAG) {
            throw new TypeMismatchException("list-accessor", 0, listBytes[start],
                    ATypeTag.SERIALIZED_UNORDEREDLIST_TYPE_TAG, ATypeTag.SERIALIZED_ORDEREDLIST_TYPE_TAG);
        }
        listType = EnumDeserializer.ATYPETAGDESERIALIZER.deserialize(listBytes[start]);
        itemType = EnumDeserializer.ATYPETAGDESERIALIZER.deserialize(listBytes[start + 1]);
        if (listBytes[start] == ATypeTag.SERIALIZED_UNORDEREDLIST_TYPE_TAG) {
            size = AUnorderedListSerializerDeserializer.getNumberOfItems(listBytes, start);
        } else {
            size = AOrderedListSerializerDeserializer.getNumberOfItems(listBytes, start);
        }
    }

    public int size() {
        return size;
    }

    public int getItemOffset(int itemIndex) throws HyracksDataException {
        if (listType == ATypeTag.MULTISET) {
            return AUnorderedListSerializerDeserializer.getItemOffset(listBytes, start, itemIndex);
        } else {
            return AOrderedListSerializerDeserializer.getItemOffset(listBytes, start, itemIndex);
        }
    }

    public int getItemLength(int itemOffset) throws HyracksDataException {
        ATypeTag itemType = getItemType(itemOffset);
        return NonTaggedFormatUtil.getFieldValueLength(listBytes, itemOffset, itemType, itemsAreSelfDescribing());
    }

    public ATypeTag getItemType(int itemOffset) {
        if (itemType == ATypeTag.ANY) {
            return EnumDeserializer.ATYPETAGDESERIALIZER.deserialize(listBytes[itemOffset]);
        } else {
            return itemType;
        }
    }

    public void writeItem(int itemIndex, DataOutput dos) throws IOException {
        int itemOffset = getItemOffset(itemIndex);
        int itemLength = getItemLength(itemOffset);
        if (itemsAreSelfDescribing()) {
            ++itemLength;
        } else {
            dos.writeByte(itemType.serialize());
        }
        dos.write(listBytes, itemOffset, itemLength);
    }

    /**
     * @param itemIndex the index of the item requested
     * @param pointable a pointable that will be set to point to the item requested
     * @param storage if list is strongly typed, the item tag will be written followed by the item value to this storage
     * @return true when the item requested has been written to the storage. false when a pointer to the item was set
     * @throws IOException
     */
    public boolean getOrWriteItem(int itemIndex, IPointable pointable, ArrayBackedValueStorage storage)
            throws IOException {
        int itemOffset = getItemOffset(itemIndex);
        int itemLength = getItemLength(itemOffset);
        if (itemsAreSelfDescribing()) {
            // +1 to account for the already included tag
            pointable.set(listBytes, itemOffset, itemLength + 1);
            return false;
        } else {
            storage.reset();
            storage.getDataOutput().writeByte(itemType.serialize());
            storage.getDataOutput().write(listBytes, itemOffset, itemLength);
            pointable.set(storage);
            return true;
        }
    }

    public byte[] getByteArray() {
        return listBytes;
    }

    public int getStart() {
        return start;
    }
}
