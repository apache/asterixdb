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
package org.apache.asterix.om.lazy;

import org.apache.asterix.dataflow.data.nontagged.serde.AInt32SerializerDeserializer;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.AbstractCollectionType;
import org.apache.asterix.om.types.IAType;
import org.apache.asterix.om.utils.NonTaggedFormatUtil;
import org.apache.hyracks.api.exceptions.HyracksDataException;

/**
 * This implementation is for {@link ATypeTag#ARRAY} and {@link ATypeTag#MULTISET} with variable-length items.
 */
public class VariableListLazyVisitablePointable extends AbstractListLazyVisitablePointable {
    private final ATypeTag itemTag;
    //List builder computes the items' offset as if the type tag exists
    private final int actualChildOffset;

    public VariableListLazyVisitablePointable(boolean tagged, AbstractCollectionType listType) {
        super(tagged, listType);
        itemTag = listType.getItemType().getTypeTag();
        //-1 if not tagged. The offsets were calculated as if the tag exists.
        actualChildOffset = isTagged() ? 0 : -1;
        currentChildTypeTag = itemTag.serialize();
    }

    @Override
    public void nextChild() throws HyracksDataException {
        byte[] data = getByteArray();
        int itemOffset = getStartOffset() + AInt32SerializerDeserializer.getInt(data, itemsOffset + currentIndex * 4)
                + actualChildOffset;
        ATypeTag itemTypeTag = processTypeTag(data, itemOffset);
        int itemSize = NonTaggedFormatUtil.getFieldValueLength(data, itemOffset, itemTypeTag, isTaggedChild());
        currentValue.set(data, itemOffset, itemSize);
        currentIndex++;
    }

    private ATypeTag processTypeTag(byte[] data, int itemOffset) {
        if (itemTag == ATypeTag.ANY) {
            currentChildTypeTag = data[itemOffset];
        }
        return itemTag;
    }

    @Override
    public boolean isTaggedChild() {
        return itemTag == ATypeTag.ANY;
    }

    @Override
    AbstractLazyVisitablePointable createVisitablePointable(IAType itemType) {
        if (itemType.getTypeTag() != ATypeTag.ANY) {
            return createVisitable(itemType);
        }
        return new GenericLazyVisitablePointable();
    }
}
