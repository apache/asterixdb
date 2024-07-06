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

import org.apache.asterix.builders.AbstractListBuilder;
import org.apache.asterix.builders.OrderedListBuilder;
import org.apache.asterix.builders.UnorderedListBuilder;
import org.apache.asterix.dataflow.data.nontagged.serde.AInt32SerializerDeserializer;
import org.apache.asterix.om.pointables.base.DefaultOpenFieldType;
import org.apache.asterix.om.pointables.base.ICastingPointable;
import org.apache.asterix.om.pointables.cast.ACastingPointableVisitor;
import org.apache.asterix.om.pointables.cast.CastResult;
import org.apache.asterix.om.pointables.visitor.ICastingPointableVisitor;
import org.apache.asterix.om.typecomputer.impl.TypeComputeUtils;
import org.apache.asterix.om.types.AOrderedListType;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.AbstractCollectionType;
import org.apache.asterix.om.types.EnumDeserializer;
import org.apache.asterix.om.types.IAType;
import org.apache.asterix.om.types.TypeTagUtil;
import org.apache.asterix.om.util.container.IObjectFactory;
import org.apache.asterix.om.utils.NonTaggedFormatUtil;
import org.apache.asterix.om.utils.ResettableByteArrayOutputStream;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.api.IPointable;
import org.apache.hyracks.data.std.primitive.VoidPointable;

public class AListCastingPointable extends AbstractCastingPointable {

    public static final IObjectFactory<AListCastingPointable, IAType> FACTORY =
            type -> new AListCastingPointable((AbstractCollectionType) type);
    private final ResettableByteArrayOutputStream outBos = new ResettableByteArrayOutputStream();
    private final DataOutputStream outDos = new DataOutputStream(outBos);
    private final CastResult itemCastResult = new CastResult(new VoidPointable(), null);
    private final UnorderedListBuilder unOrderedListBuilder = new UnorderedListBuilder();
    private final OrderedListBuilder orderedListBuilder = new OrderedListBuilder();
    private final IAType itemType;
    private final boolean typedItemList;
    private final boolean ordered;

    public AListCastingPointable(AbstractCollectionType inputType) {
        ordered = inputType instanceof AOrderedListType;
        itemType = TypeComputeUtils.getActualType(inputType.getItemType());
        typedItemList = itemType.getTypeTag() != ATypeTag.ANY;
    }

    @Override
    public <R, T> R accept(ICastingPointableVisitor<R, T> visitor, T arg) throws HyracksDataException {
        return visitor.visit(this, arg);
    }

    @Override
    public Type getType() {
        return Type.LIST;
    }

    @Override
    protected void reset() {
        super.reset();
        outBos.reset();
    }

    public void castList(IPointable castOutResult, AbstractCollectionType reqType, ACastingPointableVisitor visitor)
            throws HyracksDataException {
        reset();
        int tagByte = isTagged() ? 1 : 0;
        int numberOfItems = AInt32SerializerDeserializer.getInt(data, start + 5 + tagByte);
        int itemOffset;
        if (typedItemList && NonTaggedFormatUtil.isFixedSizedCollection(itemType.getTypeTag())) {
            itemOffset = start + 9 + tagByte;
        } else {
            itemOffset = start + 9 + tagByte + (numberOfItems * 4);
        }
        IAType reqItemType;
        AbstractListBuilder listBuilder;
        if (reqType.getTypeTag() == ATypeTag.MULTISET) {
            unOrderedListBuilder.reset(reqType);
            listBuilder = unOrderedListBuilder;
            reqItemType = reqType.getItemType();
        } else if (reqType.getTypeTag() == ATypeTag.ARRAY) {
            orderedListBuilder.reset(reqType);
            listBuilder = orderedListBuilder;
            reqItemType = reqType.getItemType();
        } else {
            throw new RuntimeException("NYI: " + reqType);
        }
        int itemLength;
        if (typedItemList) {
            for (int i = 0; i < numberOfItems; i++) {
                itemLength = NonTaggedFormatUtil.getFieldValueLength(data, itemOffset, itemType.getTypeTag(), false);
                castItem(itemOffset, itemLength, itemType, itemType.getTypeTag(), reqItemType, visitor, listBuilder,
                        false);
                itemOffset += itemLength;
            }
        } else {
            for (int i = 0; i < numberOfItems; i++) {
                ATypeTag itemTag = EnumDeserializer.ATYPETAGDESERIALIZER.deserialize(data[itemOffset]);
                itemLength = NonTaggedFormatUtil.getFieldValueLength(data, itemOffset, itemTag, true) + 1;
                IAType itemType = TypeTagUtil.getBuiltinTypeByTag(itemTag);
                castItem(itemOffset, itemLength, itemType, itemTag, reqItemType, visitor, listBuilder, true);
                itemOffset += itemLength;
            }
        }
        int outStart = outBos.size();
        listBuilder.write(outDos, true);
        int outEnd = outBos.size();
        castOutResult.set(outBos.getByteArray(), outStart, outEnd - outStart);
    }

    private void castItem(int itemOffset, int itemLength, IAType itemType, ATypeTag itemTag, IAType reqItemType,
            ACastingPointableVisitor visitor, AbstractListBuilder listBuilder, boolean openItem)
            throws HyracksDataException {
        ICastingPointable item = allocate(data, itemOffset, itemLength, itemType, itemTag, openItem);
        IAType outType = reqItemType == null ? null : TypeComputeUtils.getActualType(reqItemType);
        if (outType == null || outType.getTypeTag() == ATypeTag.ANY) {
            itemCastResult.setOutType(DefaultOpenFieldType.getDefaultOpenFieldType(itemTag));
        } else {
            itemCastResult.setOutType(outType);
        }
        item.accept(visitor, itemCastResult);
        listBuilder.addItem(itemCastResult.getOutPointable());
        free(item);
    }

    public boolean ordered() {
        return ordered;
    }
}
