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
package edu.uci.ics.asterix.dataflow.data.nontagged.serde;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;

import edu.uci.ics.asterix.builders.OrderedListBuilder;
import edu.uci.ics.asterix.common.exceptions.AsterixException;
import edu.uci.ics.asterix.formats.nontagged.AqlSerializerDeserializerProvider;
import edu.uci.ics.asterix.om.base.AOrderedList;
import edu.uci.ics.asterix.om.base.IAObject;
import edu.uci.ics.asterix.om.types.AOrderedListType;
import edu.uci.ics.asterix.om.types.ATypeTag;
import edu.uci.ics.asterix.om.types.EnumDeserializer;
import edu.uci.ics.asterix.om.types.IAType;
import edu.uci.ics.asterix.om.util.NonTaggedFormatUtil;
import edu.uci.ics.hyracks.api.dataflow.value.ISerializerDeserializer;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.data.std.util.ArrayBackedValueStorage;

public class AOrderedListSerializerDeserializer implements ISerializerDeserializer<AOrderedList> {

    private static final long serialVersionUID = 1L;
    public static final AOrderedListSerializerDeserializer SCHEMALESS_INSTANCE = new AOrderedListSerializerDeserializer();

    private IAType itemType;
    @SuppressWarnings("rawtypes")
    private ISerializerDeserializer serializer;
    @SuppressWarnings("rawtypes")
    private ISerializerDeserializer deserializer;
    private AOrderedListType orderedlistType;

    private AOrderedListSerializerDeserializer() {
        this.itemType = null;
        this.orderedlistType = null;
    }

    public AOrderedListSerializerDeserializer(AOrderedListType orderedlistType) {
        this.itemType = orderedlistType.getItemType();
        this.orderedlistType = orderedlistType;
        serializer = AqlSerializerDeserializerProvider.INSTANCE.getSerializerDeserializer(itemType);
        deserializer = itemType.getTypeTag() == ATypeTag.ANY ? AqlSerializerDeserializerProvider.INSTANCE
                .getSerializerDeserializer(itemType) : AqlSerializerDeserializerProvider.INSTANCE
                .getNonTaggedSerializerDeserializer(itemType);
    }

    @Override
    public AOrderedList deserialize(DataInput in) throws HyracksDataException {
        // TODO: schemaless ordered list deserializer
        try {
            boolean fixedSize = false;
            ATypeTag typeTag = EnumDeserializer.ATYPETAGDESERIALIZER.deserialize(in.readByte());
            switch (typeTag) {
                case STRING:
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
                    IAObject v = (IAObject) deserializer.deserialize(in);
                    items.add(v);
                }
            }
            AOrderedListType type = new AOrderedListType(itemType, "orderedlist");
            return new AOrderedList(type, items);

        } catch (IOException e) {
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
