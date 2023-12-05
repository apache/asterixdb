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
import org.apache.asterix.dataflow.data.nontagged.serde.AStringSerializerDeserializer;
import org.apache.asterix.om.base.AMutableString;
import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.AUnionType;
import org.apache.asterix.om.types.IAType;
import org.apache.asterix.om.utils.NonTaggedFormatUtil;
import org.apache.asterix.om.utils.RecordUtil;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.api.IPointable;
import org.apache.hyracks.data.std.api.IValueReference;
import org.apache.hyracks.data.std.primitive.VoidPointable;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.apache.hyracks.util.string.UTF8StringWriter;

/**
 * This implementation extends {@link RecordLazyVisitablePointable} to handle {@link ATypeTag#OBJECT} with open and
 * closed fields
 */
public class TypedRecordLazyVisitablePointable extends RecordLazyVisitablePointable {
    private static final IPointable MISSING_POINTABLE = createConstantPointable(ATypeTag.MISSING);
    private static final IPointable NULL_POINTABLE = createConstantPointable(ATypeTag.NULL);
    private final ARecordType recordType;

    //Closed values
    private final IValueReference[] closedFieldNames;
    private final AbstractLazyVisitablePointable[] closedVisitables;
    private final int numberOfClosedChildren;
    private final ATypeTag[] closedChildTags;
    //Record builder computes the fields' offset as if the type tag exists
    private final int actualChildOffset;
    private int currentIndex;
    private int closedValuesOffset;

    /**
     * A constructor for the root record
     *
     * @param rootRecordType root record type
     */
    public TypedRecordLazyVisitablePointable(ARecordType rootRecordType) {
        this(true, rootRecordType);
    }

    public TypedRecordLazyVisitablePointable(boolean tagged, ARecordType recordType) {
        super(tagged);
        Objects.requireNonNull(recordType);
        this.recordType = recordType;
        numberOfClosedChildren = this.recordType.getFieldTypes().length;
        closedFieldNames = createSerializedClosedFieldNames(this.recordType);
        closedVisitables = createClosedVisitables(this.recordType);
        closedChildTags = createInitialClosedTypeTags(this.recordType);
        //-1 if not tagged. The offsets were calculated as if the tag exists.
        actualChildOffset = isTagged() ? 0 : -1;
    }

    @Override
    public void nextChild() throws HyracksDataException {
        currentIndex++;
        if (isTaggedChild()) {
            super.nextChild();
        } else {
            setClosedValueInfo();
        }
    }

    @Override
    public boolean isTaggedChild() {
        return currentIndex >= numberOfClosedChildren;
    }

    @Override
    public int getNumberOfChildren() {
        return numberOfClosedChildren + super.getNumberOfChildren();
    }

    @Override
    public AbstractLazyVisitablePointable getChildVisitablePointable() throws HyracksDataException {
        AbstractLazyVisitablePointable visitablePointable;
        if (isTaggedChild()) {
            visitablePointable = openVisitable;
        } else {
            visitablePointable = getClosedChildVisitable();
        }
        visitablePointable.set(getChildValue());
        return visitablePointable;
    }

    private AbstractLazyVisitablePointable getClosedChildVisitable() {
        switch (getChildTypeTag()) {
            case MISSING:
                return MissingLazyVisitablePointable.INSTANCE;
            case NULL:
                return NullLazyVisitablePointable.INSTANCE;
            default:
                return closedVisitables[currentIndex];
        }
    }

    private void setClosedValueInfo() throws HyracksDataException {
        ATypeTag typeTag = closedChildTags[currentIndex];
        if (typeTag == ATypeTag.NULL) {
            currentValue.set(NULL_POINTABLE);
        } else if (typeTag == ATypeTag.MISSING) {
            currentValue.set(MISSING_POINTABLE);
        } else {
            byte[] data = getByteArray();
            int offset =
                    getStartOffset() + AInt32SerializerDeserializer.getInt(data, closedValuesOffset + 4 * currentIndex)
                            + actualChildOffset;
            int length = NonTaggedFormatUtil.getFieldValueLength(data, offset, typeTag, false);
            currentValue.set(data, offset, length);
        }
        currentFieldName.set(closedFieldNames[currentIndex]);
        currentChildTypeTag = typeTag.serialize();
    }

    /* ******************************************************
     * Init Open part
     * ******************************************************
     */
    @Override
    void init(byte[] data, int offset, int length) {
        /*
         * Skip length and the type tag if the current record contains a tag. Only the root can be tagged and typed
         * at the same time. Nested typed records will not be tagged.
         */
        int skipTag = isTagged() ? 1 : 0;
        currentIndex = -1;
        //initOpenPart first. It will skip type tag and length.
        int pointer = recordType.isOpen() ? initOpenPart(data, offset) : offset + skipTag + 4;
        initClosedPart(pointer, data);
    }

    private void initClosedPart(int pointer, byte[] data) {
        //+4 to skip the number of closed children
        int currentPointer = pointer + 4;
        if (NonTaggedFormatUtil.hasOptionalField(recordType)) {
            initClosedChildrenTags(data, currentPointer);
            currentPointer +=
                    (numberOfClosedChildren % 4 == 0 ? numberOfClosedChildren / 4 : numberOfClosedChildren / 4 + 1);
        }
        closedValuesOffset = currentPointer;
    }

    private static IPointable createConstantPointable(ATypeTag tag) {
        byte[] data = { tag.serialize() };
        IPointable value = new VoidPointable();
        value.set(data, 0, 1);
        return value;
    }

    private void initClosedChildrenTags(byte[] data, int nullBitMapOffset) {
        IAType[] types = recordType.getFieldTypes();
        for (int i = 0; i < numberOfClosedChildren; i++) {
            byte nullMissingOrValue = data[nullBitMapOffset + i / 4];
            if (RecordUtil.isNull(nullMissingOrValue, i)) {
                closedChildTags[i] = ATypeTag.NULL;
            } else if (RecordUtil.isMissing(nullMissingOrValue, i)) {
                closedChildTags[i] = ATypeTag.MISSING;
            } else {
                IAType type = types[i];
                type = type.getTypeTag() == ATypeTag.UNION ? ((AUnionType) type).getActualType() : type;
                closedChildTags[i] = type.getTypeTag();
            }
        }
    }

    private static ATypeTag[] createInitialClosedTypeTags(ARecordType recordType) {
        IAType[] types = recordType.getFieldTypes();
        ATypeTag[] typeTags = new ATypeTag[types.length];
        for (int i = 0; i < types.length; i++) {
            IAType type = types[i];
            if (type.getTypeTag() == ATypeTag.UNION) {
                type = ((AUnionType) type).getActualType();
            }
            typeTags[i] = type.getTypeTag();
        }
        return typeTags;
    }

    private static IValueReference[] createSerializedClosedFieldNames(ARecordType recordType) {
        UTF8StringWriter writer = new UTF8StringWriter();
        AMutableString mutableString = new AMutableString("");
        AStringSerializerDeserializer serDer = new AStringSerializerDeserializer(writer, null);

        String[] fieldNames = recordType.getFieldNames();
        IValueReference[] fieldNameReferences = new IValueReference[fieldNames.length];
        for (int i = 0; i < fieldNameReferences.length; i++) {
            mutableString.setValue(fieldNames[i]);
            fieldNameReferences[i] = createFieldName(mutableString, serDer);
        }
        return fieldNameReferences;
    }

    private static IValueReference createFieldName(AMutableString mutableString, AStringSerializerDeserializer serDer) {
        ArrayBackedValueStorage storage = new ArrayBackedValueStorage();
        try {
            serDer.serialize(mutableString, storage.getDataOutput());
        } catch (HyracksDataException e) {
            throw new IllegalStateException(e);
        }
        return storage;
    }

    private static AbstractLazyVisitablePointable[] createClosedVisitables(ARecordType recordType) {
        IAType[] types = recordType.getFieldTypes();
        AbstractLazyVisitablePointable[] visitables = new AbstractLazyVisitablePointable[types.length];
        for (int i = 0; i < types.length; i++) {
            visitables[i] = createVisitable(types[i]);
        }
        return visitables;
    }

}
