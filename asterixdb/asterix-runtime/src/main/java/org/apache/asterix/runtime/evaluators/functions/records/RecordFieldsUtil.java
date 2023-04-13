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
package org.apache.asterix.runtime.evaluators.functions.records;

import java.io.DataOutput;
import java.io.IOException;

import org.apache.asterix.builders.AbvsBuilderFactory;
import org.apache.asterix.builders.IARecordBuilder;
import org.apache.asterix.builders.IAsterixListBuilder;
import org.apache.asterix.builders.ListBuilderFactory;
import org.apache.asterix.builders.OrderedListBuilder;
import org.apache.asterix.builders.RecordBuilderFactory;
import org.apache.asterix.formats.nontagged.SerializerDeserializerProvider;
import org.apache.asterix.om.base.ABoolean;
import org.apache.asterix.om.base.AMutableString;
import org.apache.asterix.om.base.AString;
import org.apache.asterix.om.pointables.base.DefaultOpenFieldType;
import org.apache.asterix.om.pointables.nonvisitor.AListPointable;
import org.apache.asterix.om.pointables.nonvisitor.ARecordPointable;
import org.apache.asterix.om.types.AOrderedListType;
import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.AUnorderedListType;
import org.apache.asterix.om.types.AbstractCollectionType;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.asterix.om.types.EnumDeserializer;
import org.apache.asterix.om.types.IAType;
import org.apache.asterix.om.util.container.IObjectPool;
import org.apache.asterix.om.util.container.ListObjectPool;
import org.apache.hyracks.api.dataflow.value.ISerializerDeserializer;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.api.IMutableValueStorage;
import org.apache.hyracks.data.std.api.IPointable;
import org.apache.hyracks.data.std.api.IValueReference;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;

public class RecordFieldsUtil {

    private final static AString fieldName = new AString("field-name");
    private final static AString typeName = new AString("field-type");
    private final static AString isOpenName = new AString("is-open");
    private final static AString nestedName = new AString("nested");
    private final static AString listName = new AString("list");

    private final IObjectPool<IARecordBuilder, ATypeTag> recordBuilderPool =
            new ListObjectPool<>(new RecordBuilderFactory());
    private final IObjectPool<IAsterixListBuilder, ATypeTag> listBuilderPool =
            new ListObjectPool<>(new ListBuilderFactory());
    private final IObjectPool<IMutableValueStorage, ATypeTag> abvsBuilderPool =
            new ListObjectPool<>(new AbvsBuilderFactory());
    private final IObjectPool<IPointable, ATypeTag> recordPointablePool =
            new ListObjectPool<>(ARecordPointable.ALLOCATOR);
    private final IObjectPool<IPointable, ATypeTag> listPointablePool = new ListObjectPool<>(AListPointable.ALLOCATOR);

    private final static AOrderedListType listType = new AOrderedListType(BuiltinType.ANY, "fields");
    //Better not be a static object.
    @SuppressWarnings("unchecked")
    protected final ISerializerDeserializer<AString> stringSerde =
            SerializerDeserializerProvider.INSTANCE.getSerializerDeserializer(BuiltinType.ASTRING);
    @SuppressWarnings("unchecked")
    protected final ISerializerDeserializer<ABoolean> booleanSerde =
            SerializerDeserializerProvider.INSTANCE.getSerializerDeserializer(BuiltinType.ABOOLEAN);

    private final static ARecordType openType = DefaultOpenFieldType.NESTED_OPEN_RECORD_TYPE;
    private final static AOrderedListType openOrderedListType = DefaultOpenFieldType.NESTED_OPEN_AORDERED_LIST_TYPE;
    private final static AUnorderedListType openUnOrderedListType =
            DefaultOpenFieldType.NESTED_OPEN_AUNORDERED_LIST_TYPE;

    public void processRecord(ARecordPointable recordAccessor, ARecordType recType, DataOutput out, int level)
            throws IOException {
        if (level == 0) {
            // Resets pools for recycling objects before processing a top-level record.
            resetPools();
        }
        ArrayBackedValueStorage itemValue = getTempBuffer();
        ArrayBackedValueStorage fieldName = getTempBuffer();

        OrderedListBuilder orderedListBuilder = getOrderedListBuilder();
        orderedListBuilder.reset(listType);
        IARecordBuilder fieldRecordBuilder = getRecordBuilder();
        fieldRecordBuilder.reset(openType);

        int schemeFieldCount = recordAccessor.getSchemeFieldCount(recType);
        for (int i = 0; i < schemeFieldCount; ++i) {
            itemValue.reset();
            fieldRecordBuilder.init();

            // write name
            fieldName.reset();
            recordAccessor.getClosedFieldName(recType, i, fieldName.getDataOutput());
            addNameField(fieldName, fieldRecordBuilder);

            // write type
            byte tag = recordAccessor.getClosedFieldTag(recType, i);
            addFieldType(tag, fieldRecordBuilder);

            // write open
            addIsOpenField(false, fieldRecordBuilder);

            // write nested or list types
            if (tag == ATypeTag.SERIALIZED_RECORD_TYPE_TAG || tag == ATypeTag.SERIALIZED_ORDEREDLIST_TYPE_TAG
                    || tag == ATypeTag.SERIALIZED_UNORDEREDLIST_TYPE_TAG) {
                if (!recordAccessor.isClosedFieldNull(recType, i) && !recordAccessor.isClosedFieldMissing(recType, i)) {
                    IAType fieldType = recordAccessor.getClosedFieldType(recType, i);
                    ArrayBackedValueStorage tmpValue = getTempBuffer();
                    tmpValue.reset();
                    recordAccessor.getClosedFieldValue(recType, i, tmpValue.getDataOutput());
                    if (tag == ATypeTag.SERIALIZED_RECORD_TYPE_TAG) {
                        addRecordField(tmpValue, fieldType, fieldRecordBuilder, level + 1);
                    } else {
                        addListField(tmpValue, fieldType, fieldRecordBuilder, level + 1);
                    }
                }
            }

            // write record
            fieldRecordBuilder.write(itemValue.getDataOutput(), true);

            // add item to the list of fields
            orderedListBuilder.addItem(itemValue);
        }
        for (int i = recordAccessor.getOpenFieldCount(recType) - 1; i >= 0; --i) {
            itemValue.reset();
            fieldRecordBuilder.init();

            // write name
            fieldName.reset();
            recordAccessor.getOpenFieldName(recType, i, fieldName.getDataOutput());
            addNameField(fieldName, fieldRecordBuilder);

            // write type
            byte tag = recordAccessor.getOpenFieldTag(recType, i);
            addFieldType(tag, fieldRecordBuilder);

            // write open
            addIsOpenField(true, fieldRecordBuilder);

            // write nested or list types
            if (tag == ATypeTag.SERIALIZED_RECORD_TYPE_TAG || tag == ATypeTag.SERIALIZED_ORDEREDLIST_TYPE_TAG
                    || tag == ATypeTag.SERIALIZED_UNORDEREDLIST_TYPE_TAG) {
                ArrayBackedValueStorage tmpValue = getTempBuffer();
                tmpValue.reset();
                recordAccessor.getOpenFieldValue(recType, i, tmpValue.getDataOutput());
                if (tag == ATypeTag.SERIALIZED_RECORD_TYPE_TAG) {
                    addRecordField(tmpValue, openType, fieldRecordBuilder, level + 1);
                } else if (tag == ATypeTag.SERIALIZED_ORDEREDLIST_TYPE_TAG) {
                    addListField(tmpValue, openOrderedListType, fieldRecordBuilder, level + 1);
                } else {
                    addListField(tmpValue, openUnOrderedListType, fieldRecordBuilder, level + 1);
                }
            }

            // write record
            fieldRecordBuilder.write(itemValue.getDataOutput(), true);

            // add item to the list of fields
            orderedListBuilder.addItem(itemValue);
        }
        orderedListBuilder.write(out, true);
    }

    public void addNameField(IValueReference nameArg, IARecordBuilder fieldRecordBuilder) throws HyracksDataException {
        ArrayBackedValueStorage fieldAbvs = getTempBuffer();

        fieldAbvs.reset();
        stringSerde.serialize(fieldName, fieldAbvs.getDataOutput());
        fieldRecordBuilder.addField(fieldAbvs, nameArg);
    }

    public void addFieldType(byte tagId, IARecordBuilder fieldRecordBuilder) throws HyracksDataException {
        ArrayBackedValueStorage fieldAbvs = getTempBuffer();
        ArrayBackedValueStorage valueAbvs = getTempBuffer();

        // Name
        fieldAbvs.reset();
        stringSerde.serialize(typeName, fieldAbvs.getDataOutput());
        // Value
        valueAbvs.reset();
        ATypeTag tag = EnumDeserializer.ATYPETAGDESERIALIZER.deserialize(tagId);
        AMutableString aString = new AMutableString("");
        aString.setValue(tag.toString());
        stringSerde.serialize(aString, valueAbvs.getDataOutput());
        fieldRecordBuilder.addField(fieldAbvs, valueAbvs);
    }

    public void addIsOpenField(boolean isOpen, IARecordBuilder fieldRecordBuilder) throws HyracksDataException {
        ArrayBackedValueStorage fieldAbvs = getTempBuffer();
        ArrayBackedValueStorage valueAbvs = getTempBuffer();

        // Name
        fieldAbvs.reset();
        stringSerde.serialize(isOpenName, fieldAbvs.getDataOutput());
        // Value
        valueAbvs.reset();
        if (isOpen) {
            booleanSerde.serialize(ABoolean.TRUE, valueAbvs.getDataOutput());
        } else {
            booleanSerde.serialize(ABoolean.FALSE, valueAbvs.getDataOutput());
        }
        fieldRecordBuilder.addField(fieldAbvs, valueAbvs);
    }

    public void addListField(IValueReference listArg, IAType fieldType, IARecordBuilder fieldRecordBuilder, int level)
            throws IOException {
        ArrayBackedValueStorage fieldAbvs = getTempBuffer();
        ArrayBackedValueStorage valueAbvs = getTempBuffer();

        // Name
        fieldAbvs.reset();
        stringSerde.serialize(listName, fieldAbvs.getDataOutput());
        // Value
        valueAbvs.reset();
        processListValue(listArg, fieldType, valueAbvs.getDataOutput(), level);
        fieldRecordBuilder.addField(fieldAbvs, valueAbvs);
    }

    public void addRecordField(IValueReference recordArg, IAType fieldType, IARecordBuilder fieldRecordBuilder,
            int level) throws IOException {
        ArrayBackedValueStorage fieldAbvs = getTempBuffer();
        ArrayBackedValueStorage valueAbvs = getTempBuffer();

        // Name
        fieldAbvs.reset();
        stringSerde.serialize(nestedName, fieldAbvs.getDataOutput());
        // Value
        valueAbvs.reset();
        ARecordType newType = (ARecordType) fieldType;
        ARecordPointable recordP = getRecordPointable();
        recordP.set(recordArg);
        processRecord(recordP, newType, valueAbvs.getDataOutput(), level);
        fieldRecordBuilder.addField(fieldAbvs, valueAbvs);
    }

    public void processListValue(IValueReference listArg, IAType listType, DataOutput out, int level)
            throws IOException {
        ArrayBackedValueStorage itemValue = getTempBuffer();
        IARecordBuilder listRecordBuilder = getRecordBuilder();

        AListPointable list = getListPointable(listType.getTypeTag());
        list.set(listArg);

        OrderedListBuilder innerListBuilder = getOrderedListBuilder();
        innerListBuilder.reset(RecordFieldsUtil.listType);

        listRecordBuilder.reset(openType);
        AbstractCollectionType act = (AbstractCollectionType) listType;
        int itemCount = list.getItemCount();
        for (int l = 0; l < itemCount; l++) {
            itemValue.reset();
            listRecordBuilder.init();

            byte tagId = list.getItemTag(act, l);
            addFieldType(tagId, listRecordBuilder);

            if (tagId == ATypeTag.SERIALIZED_RECORD_TYPE_TAG || tagId == ATypeTag.SERIALIZED_ORDEREDLIST_TYPE_TAG
                    || tagId == ATypeTag.SERIALIZED_UNORDEREDLIST_TYPE_TAG) {
                ArrayBackedValueStorage tmpAbvs = getTempBuffer();
                tmpAbvs.reset();
                list.getItemValue(act, l, tmpAbvs.getDataOutput());
                IAType itemType;

                if (tagId == ATypeTag.SERIALIZED_RECORD_TYPE_TAG) {
                    itemType = act.getItemType().getTypeTag() != ATypeTag.ANY ? act.getItemType() : openType;
                    addRecordField(tmpAbvs, itemType, listRecordBuilder, level + 1);
                } else if (tagId == ATypeTag.SERIALIZED_ORDEREDLIST_TYPE_TAG) {
                    itemType = act.getItemType().getTypeTag() != ATypeTag.ANY ? act.getItemType() : openOrderedListType;
                    addListField(tmpAbvs, itemType, listRecordBuilder, level + 1);
                } else {
                    itemType =
                            act.getItemType().getTypeTag() != ATypeTag.ANY ? act.getItemType() : openUnOrderedListType;
                    addListField(tmpAbvs, itemType, listRecordBuilder, level + 1);
                }
            }

            listRecordBuilder.write(itemValue.getDataOutput(), true);
            innerListBuilder.addItem(itemValue);
        }
        innerListBuilder.write(out, true);
    }

    private ARecordPointable getRecordPointable() {
        return (ARecordPointable) recordPointablePool.allocate(ATypeTag.OBJECT);
    }

    private AListPointable getListPointable(ATypeTag tag) {
        return (AListPointable) listPointablePool.allocate(tag);
    }

    private IARecordBuilder getRecordBuilder() {
        return recordBuilderPool.allocate(ATypeTag.OBJECT);
    }

    private OrderedListBuilder getOrderedListBuilder() {
        return (OrderedListBuilder) listBuilderPool.allocate(ATypeTag.ARRAY);
    }

    private ArrayBackedValueStorage getTempBuffer() {
        return (ArrayBackedValueStorage) abvsBuilderPool.allocate(ATypeTag.BINARY);
    }

    private void resetPools() {
        abvsBuilderPool.reset();
        listBuilderPool.reset();
        recordBuilderPool.reset();
        recordPointablePool.reset();
        listPointablePool.reset();
    }
}
