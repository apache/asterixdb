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
import java.util.ArrayList;
import java.util.List;

import org.apache.asterix.builders.OrderedListBuilder;
import org.apache.asterix.common.exceptions.AsterixException;
import org.apache.asterix.formats.nontagged.AqlSerializerDeserializerProvider;
import org.apache.asterix.om.base.AOrderedList;
import org.apache.asterix.om.base.IAObject;
import org.apache.asterix.om.types.AOrderedListType;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.asterix.om.types.EnumDeserializer;
import org.apache.asterix.om.types.IAType;
import org.apache.asterix.om.types.TypeTagUtil;
import org.apache.asterix.om.util.NonTaggedFormatUtil;
import org.apache.hyracks.api.dataflow.value.ISerializerDeserializer;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;

public class AOrderedListSerializerDeserializer implements ISerializerDeserializer<AOrderedList> {

    private static final long serialVersionUID = 1L;
    public static final AOrderedListSerializerDeserializer SCHEMALESS_INSTANCE = new AOrderedListSerializerDeserializer();

    private final IAType itemType;
    @SuppressWarnings("rawtypes")
    private final ISerializerDeserializer serializer;
    @SuppressWarnings("rawtypes")
    private final ISerializerDeserializer deserializer;
    private final AOrderedListType orderedlistType;

    private AOrderedListSerializerDeserializer() {
        this(new AOrderedListType(BuiltinType.ANY, "orderedlist"));
    }

    public AOrderedListSerializerDeserializer(AOrderedListType orderedlistType) {
        this.orderedlistType = orderedlistType;
        this.itemType = orderedlistType.getItemType();
        serializer = AqlSerializerDeserializerProvider.INSTANCE.getSerializerDeserializer(itemType);
        deserializer = itemType.getTypeTag() == ATypeTag.ANY
                ? AqlSerializerDeserializerProvider.INSTANCE.getSerializerDeserializer(itemType)
                : AqlSerializerDeserializerProvider.INSTANCE.getNonTaggedSerializerDeserializer(itemType);
    }

    @Override
    public AOrderedList deserialize(DataInput in) throws HyracksDataException {
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

            IAType currentItemType = itemType;
            @SuppressWarnings("rawtypes")
            ISerializerDeserializer currentDeserializer = deserializer;
            if (itemType.getTypeTag() == ATypeTag.ANY && typeTag != ATypeTag.ANY) {
                currentItemType = TypeTagUtil.getBuiltinTypeByTag(typeTag);
                currentDeserializer = AqlSerializerDeserializerProvider.INSTANCE
                        .getNonTaggedSerializerDeserializer(currentItemType);
            }

            List<IAObject> items = new ArrayList<IAObject>();
            in.readInt(); // list size
            int numberOfitems;
            numberOfitems = in.readInt();
            if (numberOfitems > 0) {
                if (!fixedSize) {
                    for (int i = 0; i < numberOfitems; i++)
                        in.readInt();
                }
                for (int i = 0; i < numberOfitems; i++) {
                    IAObject v = (IAObject) currentDeserializer.deserialize(in);
                    items.add(v);
                }
            }
            AOrderedListType type = new AOrderedListType(currentItemType, "orderedlist");
            return new AOrderedList(type, items);
        } catch (Exception e) {
            throw new HyracksDataException(e);
        }
    }

    @SuppressWarnings("unchecked")
    @Override
    public void serialize(AOrderedList instance, DataOutput out) throws HyracksDataException {
        // TODO: schemaless ordered list serializer
        OrderedListBuilder listBuilder = new OrderedListBuilder();
        listBuilder.reset(orderedlistType);
        ArrayBackedValueStorage itemValue = new ArrayBackedValueStorage();
        for (int i = 0; i < instance.size(); i++) {
            itemValue.reset();
            serializer.serialize(instance.getItem(i), itemValue.getDataOutput());
            listBuilder.addItem(itemValue);
        }
        listBuilder.write(out, false);
    }

    public static final int getOrderedListLength(byte[] serOrderedList, int offset) {
        return AInt32SerializerDeserializer.getInt(serOrderedList, offset + 1);
    }

    public static int getNumberOfItems(byte[] serOrderedList) {
        return getNumberOfItems(serOrderedList, 0);
    }

    public static int getNumberOfItems(byte[] serOrderedList, int offset) {
        if (serOrderedList[offset] == ATypeTag.ORDEREDLIST.serialize())
            // 6 = tag (1) + itemTag (1) + list size (4)
            return AInt32SerializerDeserializer.getInt(serOrderedList, offset + 6);
        else
            return -1;
    }

    public static int getItemOffset(byte[] serOrderedList, int offset, int itemIndex) throws AsterixException {
        if (serOrderedList[offset] == ATypeTag.ORDEREDLIST.serialize()) {
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
