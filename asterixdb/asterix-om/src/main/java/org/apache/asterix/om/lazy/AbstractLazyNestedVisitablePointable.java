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

import static org.apache.asterix.om.typecomputer.impl.TypeComputeUtils.getActualType;

import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.AbstractCollectionType;
import org.apache.asterix.om.types.IAType;
import org.apache.asterix.om.utils.NonTaggedFormatUtil;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.api.IValueReference;
import org.apache.hyracks.data.std.primitive.VoidPointable;

/**
 * A common implementation for nested values (i.e., {@link ATypeTag#OBJECT}, {@link ATypeTag#ARRAY}, and
 * {@link ATypeTag#MULTISET}).
 * <p>
 * Contract:
 * <p>
 * 1- A child's value may or may not contain a type tag. Thus, it is the responsibility of the caller to check if the
 * child's value contains a type tag by calling {@link #isTaggedChild()}.
 * 2- The returned objects from {@link #getChildVisitablePointable()} and {@link #getChildValue()}, are reused
 * when possible. Thus, when the caller does the following for example:
 * <p>
 * AbstractLazyVisitablePointable child1 = visitablePointable.getChildVisitablePointable();
 * visitablePointable.nextChild();
 * AbstractLazyVisitablePointable child2 = visitablePointable.getChildVisitablePointable();
 * <p>
 * both child1 and child2 may have the same value, which is the value of the second child.
 */
public abstract class AbstractLazyNestedVisitablePointable extends AbstractLazyVisitablePointable {
    private final ATypeTag typeTag;
    protected final VoidPointable currentValue;
    protected byte currentChildTypeTag;

    AbstractLazyNestedVisitablePointable(boolean tagged, ATypeTag typeTag) {
        super(tagged);
        this.typeTag = typeTag;
        currentValue = new VoidPointable();
    }

    /**
     * Prepare the value and the tag of the next child
     */
    public abstract void nextChild() throws HyracksDataException;

    /**
     * If the child contains a tag
     *
     * @return true if the child is tagged (open value), false otherwise
     */
    public abstract boolean isTaggedChild();

    /**
     * @return number of children
     */
    public abstract int getNumberOfChildren();

    /**
     * Gets a child visitable-pointable.
     */
    public abstract AbstractLazyVisitablePointable getChildVisitablePointable() throws HyracksDataException;

    /**
     * Returns a value reference of the child. Note that this is not a visitable-pointable reference.
     */
    public final IValueReference getChildValue() {
        return currentValue;
    }

    /**
     * The serialized type tag of a child
     */
    public final byte getChildSerializedTypeTag() {
        return currentChildTypeTag;
    }

    /**
     * The type tag of a child
     */
    public final ATypeTag getChildTypeTag() {
        return ATypeTag.VALUE_TYPE_MAPPING[currentChildTypeTag];
    }

    /**
     * @return The type tag that corresponds to {@code this} visitable-pointable
     */
    @Override
    public final ATypeTag getTypeTag() {
        return ATypeTag.VALUE_TYPE_MAPPING[getSerializedTypeTag()];
    }

    /**
     * @return The serialized type tag that corresponds to {@code this} visitable-pointable
     */
    @Override
    public final byte getSerializedTypeTag() {
        return typeTag.serialize();
    }

    /**
     * Helper method to create a typed (i.e., non-tagged) visitable-pointable
     *
     * @param type the required type
     * @return a visitable pointable that corresponds to {@code type}
     */
    static AbstractLazyVisitablePointable createVisitable(IAType type) {
        IAType actualType = getActualType(type);
        ATypeTag typeTag = actualType.getTypeTag();
        switch (typeTag) {
            case OBJECT:
                return new TypedRecordLazyVisitablePointable(false, (ARecordType) actualType);
            case ARRAY:
            case MULTISET:
                AbstractCollectionType listType = (AbstractCollectionType) actualType;
                return NonTaggedFormatUtil.isFixedSizedCollection(listType.getItemType())
                        ? new FixedListLazyVisitablePointable(false, listType)
                        : new VariableListLazyVisitablePointable(false, listType);
            default:
                return new FlatLazyVisitablePointable(false, typeTag);

        }
    }
}
