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

package org.apache.asterix.om.pointables;

import java.io.DataOutputStream;
import java.util.ArrayList;
import java.util.List;

import org.apache.asterix.dataflow.data.nontagged.serde.AInt32SerializerDeserializer;
import org.apache.asterix.om.pointables.base.IVisitablePointable;
import org.apache.asterix.om.pointables.visitor.IVisitablePointableVisitor;
import org.apache.asterix.om.types.AOrderedListType;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.AbstractCollectionType;
import org.apache.asterix.om.types.EnumDeserializer;
import org.apache.asterix.om.types.IAType;
import org.apache.asterix.om.util.container.IObjectFactory;
import org.apache.asterix.om.utils.NonTaggedFormatUtil;
import org.apache.asterix.om.utils.ResettableByteArrayOutputStream;
import org.apache.hyracks.api.exceptions.HyracksDataException;

/**
 * This class interprets the binary data representation of a list, one can
 * call getItems and getItemTags to get pointable objects for items and item
 * type tags.
 */
public class AListVisitablePointable extends AbstractVisitablePointable {

    /**
     * DO NOT allow to create AListPointable object arbitrarily, force to use
     * object pool based allocator, in order to have object reuse.
     */
    static IObjectFactory<AListVisitablePointable, IAType> FACTORY =
            type -> new AListVisitablePointable((AbstractCollectionType) type);

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
    public AListVisitablePointable(AbstractCollectionType inputType) {
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

        if (typedItemList && NonTaggedFormatUtil.isFixedSizedCollection(itemTag)) {
            itemOffset = s + 10;
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
                    IVisitablePointable item = allocator.allocateFieldValue(itemTag, b, itemOffset + 1);

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
    public <R, T> R accept(IVisitablePointableVisitor<R, T> vistor, T tag) throws HyracksDataException {
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
