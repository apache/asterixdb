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
import java.util.ArrayList;

import org.apache.asterix.builders.UnorderedListBuilder;
import org.apache.asterix.common.exceptions.AsterixException;
import org.apache.asterix.formats.nontagged.AqlSerializerDeserializerProvider;
import org.apache.asterix.om.base.AUnorderedList;
import org.apache.asterix.om.base.IACursor;
import org.apache.asterix.om.base.IAObject;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.AUnorderedListType;
import org.apache.asterix.om.types.EnumDeserializer;
import org.apache.asterix.om.types.IAType;
import org.apache.asterix.om.util.NonTaggedFormatUtil;
import org.apache.hyracks.api.dataflow.value.ISerializerDeserializer;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;

public class AUnorderedListSerializerDeserializer implements ISerializerDeserializer<AUnorderedList> {

    private static final long serialVersionUID = 1L;

    public static final AUnorderedListSerializerDeserializer SCHEMALESS_INSTANCE = new AUnorderedListSerializerDeserializer();

    private IAType itemType;
    private AUnorderedListType unorderedlistType;
    @SuppressWarnings("rawtypes")
    private ISerializerDeserializer nontaggedSerDes;
    @SuppressWarnings("rawtypes")
    private ISerializerDeserializer taggedSerDes;

    private AUnorderedListSerializerDeserializer() {
        this.itemType = null;
        this.unorderedlistType = null;
    }

    public AUnorderedListSerializerDeserializer(AUnorderedListType unorderedlistType) {
        this.itemType = unorderedlistType.getItemType();
        this.unorderedlistType = unorderedlistType;
        nontaggedSerDes = itemType.getTypeTag() == ATypeTag.ANY
                ? AqlSerializerDeserializerProvider.INSTANCE.getSerializerDeserializer(itemType)
                : AqlSerializerDeserializerProvider.INSTANCE.getNonTaggedSerializerDeserializer(itemType);
        taggedSerDes = AqlSerializerDeserializerProvider.INSTANCE.getSerializerDeserializer(itemType);
    }

    @Override
    public AUnorderedList deserialize(DataInput in) throws HyracksDataException {
        // TODO: schemaless unordered list deserializer
        try {
            boolean fixedSize = false;
            ATypeTag typeTag = EnumDeserializer.ATYPETAGDESERIALIZER.deserialize(in.readByte());
            switch (typeTag) {
                case STRING:
                case BINARY:
                case RECORD:
                case ORDEREDLIST:
                case UNORDEREDLIST:
                case ANY:
                    fixedSize = false;
                    break;
                default:
                    fixedSize = true;
                    break;
            }
            in.readInt(); // list size
            int numberOfitems;
            numberOfitems = in.readInt();
            ArrayList<IAObject> items = new ArrayList<IAObject>();
            if (numberOfitems > 0) {
                if (!fixedSize) {
                    for (int i = 0; i < numberOfitems; i++)
                        in.readInt();
                }
                for (int i = 0; i < numberOfitems; i++) {
                    items.add((IAObject) nontaggedSerDes.deserialize(in));
                }
            }
            AUnorderedListType type = new AUnorderedListType(itemType, "orderedlist");
            return new AUnorderedList(type, items);

        } catch (IOException e) {
            throw new HyracksDataException(e);
        }
    }

    @SuppressWarnings("unchecked")
    @Override
    public void serialize(AUnorderedList instance, DataOutput out) throws HyracksDataException {
        // TODO: schemaless ordered list serializer
        UnorderedListBuilder listBuilder = new UnorderedListBuilder();
        ArrayBackedValueStorage itemValue = new ArrayBackedValueStorage();
        listBuilder.reset(unorderedlistType);
        IACursor cursor = instance.getCursor();
        while (cursor.next()) {
            itemValue.reset();
            taggedSerDes.serialize(cursor.get(), itemValue.getDataOutput());
            listBuilder.addItem(itemValue);
        }
        listBuilder.write(out, false);
    }

    public static final int getUnorderedListLength(byte[] serOrderedList, int offset) {
        return AInt32SerializerDeserializer.getInt(serOrderedList, offset + 1);
    }

    public static int getNumberOfItems(byte[] serOrderedList) {
        return getNumberOfItems(serOrderedList, 0);
    }

    public static int getNumberOfItems(byte[] serOrderedList, int offset) {
        if (serOrderedList[offset] == ATypeTag.UNORDEREDLIST.serialize())
            // 6 = tag (1) + itemTag (1) + list size (4)
            return AInt32SerializerDeserializer.getInt(serOrderedList, offset + 6);
        else
            return -1;
    }

    public static int getItemOffset(byte[] serOrderedList, int offset, int itemIndex) throws AsterixException {
        if (serOrderedList[offset] == ATypeTag.UNORDEREDLIST.serialize()) {
            ATypeTag typeTag = EnumDeserializer.ATYPETAGDESERIALIZER.deserialize(serOrderedList[offset + 1]);
            switch (typeTag) {
                case STRING:
                case BINARY:
                case RECORD:
                case ORDEREDLIST:
                case UNORDEREDLIST:
                case ANY:
                    return offset + AInt32SerializerDeserializer.getInt(serOrderedList, offset + 10 + (4 * itemIndex));
                default:
                    int length = NonTaggedFormatUtil.getFieldValueLength(serOrderedList, offset + 1, typeTag, true);
                    return offset + 10 + (length * itemIndex);
            }
            // 10 = tag (1) + itemTag (1) + list size (4) + number of items (4)
        } else
            return -1;
    }

    public static int getItemOffset(byte[] serOrderedList, int itemIndex) throws AsterixException {
        return getItemOffset(serOrderedList, 0, itemIndex);
    }

}
