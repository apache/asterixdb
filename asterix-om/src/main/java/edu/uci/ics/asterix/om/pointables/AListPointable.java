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

package edu.uci.ics.asterix.om.pointables;

import java.io.DataOutputStream;
import java.util.ArrayList;
import java.util.List;

import edu.uci.ics.asterix.common.exceptions.AsterixException;
import edu.uci.ics.asterix.dataflow.data.nontagged.serde.AInt32SerializerDeserializer;
import edu.uci.ics.asterix.om.pointables.base.IVisitablePointable;
import edu.uci.ics.asterix.om.pointables.visitor.IVisitablePointableVisitor;
import edu.uci.ics.asterix.om.types.AOrderedListType;
import edu.uci.ics.asterix.om.types.ATypeTag;
import edu.uci.ics.asterix.om.types.AbstractCollectionType;
import edu.uci.ics.asterix.om.types.EnumDeserializer;
import edu.uci.ics.asterix.om.types.IAType;
import edu.uci.ics.asterix.om.util.NonTaggedFormatUtil;
import edu.uci.ics.asterix.om.util.ResettableByteArrayOutputStream;
import edu.uci.ics.asterix.om.util.container.IObjectFactory;

/**
 * This class interprets the binary data representation of a list, one can
 * call getItems and getItemTags to get pointable objects for items and item
 * type tags.
 */
public class AListPointable extends AbstractVisitablePointable {

    /**
     * DO NOT allow to create AListPointable object arbitrarily, force to use
     * object pool based allocator, in order to have object reuse.
     */
    static IObjectFactory<IVisitablePointable, IAType> FACTORY = new IObjectFactory<IVisitablePointable, IAType>() {
        public IVisitablePointable create(IAType type) {
            return new AListPointable((AbstractCollectionType) type);
        }
    };

    private final List<IVisitablePointable> items = new ArrayList<IVisitablePointable>();
    private final List<IVisitablePointable> itemTags = new ArrayList<IVisitablePointable>();
    private final PointableAllocator allocator = new PointableAllocator();

    private final ResettableByteArrayOutputStream dataBos = new ResettableByteArrayOutputStream();
    private final DataOutputStream dataDos = new DataOutputStream(dataBos);

    private IAType itemType;
    private ATypeTag itemTag;
    private boolean typedItemList = false;
    private boolean ordered = false;

    /**
     * private constructor, to prevent constructing it arbitrarily
     * 
     * @param inputType
     */
    private AListPointable(AbstractCollectionType inputType) {
        if (inputType instanceof AOrderedListType) {
            ordered = true;
        }
        if (inputType != null && inputType.getItemType() != null) {
            itemType = inputType.getItemType();
            if (itemType.getTypeTag() == ATypeTag.ANY) {
                typedItemList = false;
            } else {
                typedItemList = true;
                itemTag = inputType.getItemType().getTypeTag();
            }
        } else {
            this.typedItemList = false;
        }
    }

    private void reset() {
        allocator.reset();
        items.clear();
        itemTags.clear();
        dataBos.reset();
    }

    @Override
    public void set(byte[] b, int s, int len) {
        reset();
        super.set(b, s, len);

        int numberOfitems = AInt32SerializerDeserializer.getInt(b, s + 6);
        int itemOffset;
        if (typedItemList) {
            switch (itemTag) {
                case STRING:
                case RECORD:
                case ORDEREDLIST:
                case UNORDEREDLIST:
                case ANY:
                    itemOffset = s + 10 + (numberOfitems * 4);
                    break;
                default:
                    itemOffset = s + 10;
            }
        } else {
            itemOffset = s + 10 + (numberOfitems * 4);
        }
        int itemLength = 0;
        try {
            if (typedItemList) {
                for (int i = 0; i < numberOfitems; i++) {
                    itemLength = NonTaggedFormatUtil.getFieldValueLength(b, itemOffset, itemTag, false);
                    IVisitablePointable tag = allocator.allocateEmpty();
                    IVisitablePointable item = allocator.allocateFieldValue(itemType);

                    // set item type tag
                    int start = dataBos.size();
                    dataDos.writeByte(itemTag.serialize());
                    int end = dataBos.size();
                    tag.set(dataBos.getByteArray(), start, end - start);
                    itemTags.add(tag);

                    // set item value
                    start = dataBos.size();
                    dataDos.writeByte(itemTag.serialize());
                    dataDos.write(b, itemOffset, itemLength);
                    end = dataBos.size();
                    item.set(dataBos.getByteArray(), start, end - start);
                    itemOffset += itemLength;
                    items.add(item);
                }
            } else {
                for (int i = 0; i < numberOfitems; i++) {
                    itemTag = EnumDeserializer.ATYPETAGDESERIALIZER.deserialize(b[itemOffset]);
                    itemLength = NonTaggedFormatUtil.getFieldValueLength(b, itemOffset, itemTag, true) + 1;
                    IVisitablePointable tag = allocator.allocateEmpty();
                    IVisitablePointable item = allocator.allocateFieldValue(itemTag);

                    // set item type tag
                    int start = dataBos.size();
                    dataDos.writeByte(itemTag.serialize());
                    int end = dataBos.size();
                    tag.set(dataBos.getByteArray(), start, end - start);
                    itemTags.add(tag);

                    // open part field already include the type tag
                    item.set(b, itemOffset, itemLength);
                    itemOffset += itemLength;
                    items.add(item);
                }
            }
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    @Override
    public <R, T> R accept(IVisitablePointableVisitor<R, T> vistor, T tag) throws AsterixException {
        return vistor.visit(this, tag);
    }

    public List<IVisitablePointable> getItems() {
        return items;
    }

    public List<IVisitablePointable> getItemTags() {
        return itemTags;
    }

    public boolean ordered() {
        return ordered;
    }
}
