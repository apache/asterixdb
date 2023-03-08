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

import java.util.Objects;

import org.apache.asterix.dataflow.data.nontagged.serde.AInt32SerializerDeserializer;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.AbstractCollectionType;
import org.apache.asterix.om.types.IAType;
import org.apache.hyracks.api.exceptions.HyracksDataException;

/**
 * Common implementation for both {@link ATypeTag#ARRAY} and {@link ATypeTag#MULTISET}
 */
public abstract class AbstractListLazyVisitablePointable extends AbstractLazyNestedVisitablePointable {
    private final int headerSize;
    private final AbstractLazyVisitablePointable itemVisitablePointable;
    private int numberOfItems;
    protected int currentIndex;
    protected int itemsOffset;

    AbstractListLazyVisitablePointable(boolean tagged, AbstractCollectionType listType) {
        super(tagged, listType.getTypeTag());
        Objects.requireNonNull(listType);
        Objects.requireNonNull(listType.getItemType());
        //1 for typeTag if tagged, 1 for itemTypeTag, 4 for length
        headerSize = (isTagged() ? 1 : 0) + 1 + 4;
        itemVisitablePointable = createVisitablePointable(listType.getItemType());
    }

    @Override
    public final int getNumberOfChildren() {
        return numberOfItems;
    }

    @Override
    final void init(byte[] data, int offset, int length) {
        int pointer = headerSize + offset;
        numberOfItems = AInt32SerializerDeserializer.getInt(data, pointer);
        itemsOffset = pointer + 4;
        currentIndex = 0;
    }

    @Override
    public AbstractLazyVisitablePointable getChildVisitablePointable() throws HyracksDataException {
        itemVisitablePointable.set(getChildValue());
        return itemVisitablePointable;
    }

    @Override
    public <R, T> R accept(ILazyVisitablePointableVisitor<R, T> visitor, T arg) throws HyracksDataException {
        return visitor.visit(this, arg);
    }

    abstract AbstractLazyVisitablePointable createVisitablePointable(IAType itemType);
}
