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

package org.apache.asterix.dataflow.data.common;

import static org.apache.asterix.om.types.ATypeTag.VALUE_TYPE_MAPPING;

import java.io.IOException;

import org.apache.asterix.dataflow.data.nontagged.serde.AOrderedListSerializerDeserializer;
import org.apache.asterix.dataflow.data.nontagged.serde.AUnorderedListSerializerDeserializer;
import org.apache.asterix.dataflow.data.nontagged.serde.SerializerDeserializerUtil;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.EnumDeserializer;
import org.apache.asterix.om.utils.NonTaggedFormatUtil;
import org.apache.hyracks.data.std.api.IPointable;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;

/**
 * Utility class for accessing serialized unordered and ordered lists.
 */
public class ListAccessorUtil {

    // TODO(ali): refactor ListAccessor/AOrderedListSerializerDeserializer/AUnorderedListSerializerDeserializer methods
    // TODO(ali): AListPointable
    private ListAccessorUtil() {
    }

    /**
     * @param listBytes bytes of an array or multiset
     * @param start offset to the beginning of the array or multiset
     * @param itemIndex the index of the item requested
     * @param listTag the type of list passed
     * @param listItemTag the item type declared by the list. If the item type is ANY, each item includes its own tag
     * @param pointable a pointable that will be set to point to the item requested
     * @param storage if list is strongly typed, the item tag will be written followed by the item value to this storage
     * @return true when the item requested has been written to the storage. false when a pointer to the item was set
     * @throws IOException
     */
    public static boolean getItem(byte[] listBytes, int start, int itemIndex, ATypeTag listTag, ATypeTag listItemTag,
            IPointable pointable, ArrayBackedValueStorage storage) throws IOException {
        // TODO(ali): this method should be removed if hashing is fixed to avoid copying the tag
        int itemOffset;
        if (listTag == ATypeTag.MULTISET) {
            itemOffset = AUnorderedListSerializerDeserializer.getItemOffset(listBytes, start, itemIndex);
        } else if (listTag == ATypeTag.ARRAY) {
            itemOffset = AOrderedListSerializerDeserializer.getItemOffset(listBytes, start, itemIndex);
        } else {
            throw new IllegalStateException();
        }

        boolean itemIncludesTag = listItemTag == ATypeTag.ANY;
        ATypeTag itemTag = itemIncludesTag ? EnumDeserializer.ATYPETAGDESERIALIZER.deserialize(listBytes[itemOffset])
                : listItemTag;
        int itemValueLength = NonTaggedFormatUtil.getFieldValueLength(listBytes, itemOffset, itemTag, itemIncludesTag);
        if (itemIncludesTag) {
            // +1 to account for the already included tag
            pointable.set(listBytes, itemOffset, itemValueLength + 1);
            return false;
        } else {
            storage.reset();
            storage.getDataOutput().writeByte(listItemTag.serialize());
            storage.getDataOutput().write(listBytes, itemOffset, itemValueLength);
            pointable.set(storage);
            return true;
        }
    }

    public static void getItemFromList(TaggedValueReference listValue, int itemIndex, TaggedValueReference item,
            ATypeTag arrayItemTag, boolean itemHasTag) throws IOException {
        int itemOffset = SerializerDeserializerUtil.getItemOffsetNonTagged(listValue, itemIndex);
        getItem(listValue, itemOffset, item, arrayItemTag, itemHasTag);
    }

    private static void getItem(TaggedValueReference listValue, int itemOffset, TaggedValueReference item,
            ATypeTag listItemTag, boolean itemHasTag) throws IOException {
        byte[] listBytes = listValue.getByteArray();
        ATypeTag itemTag;
        int itemValueOffset = itemOffset;
        if (itemHasTag) {
            itemTag = VALUE_TYPE_MAPPING[listBytes[itemOffset]];
            itemValueOffset = itemOffset + 1;
        } else {
            itemTag = listItemTag;
        }
        int itemValueLength = NonTaggedFormatUtil.getFieldValueLength(listBytes, itemOffset, itemTag, itemHasTag);
        item.set(listBytes, itemValueOffset, itemValueLength, itemTag);
    }
}
