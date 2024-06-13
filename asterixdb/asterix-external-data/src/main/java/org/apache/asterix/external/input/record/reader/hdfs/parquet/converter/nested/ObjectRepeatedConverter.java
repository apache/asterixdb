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
package org.apache.asterix.external.input.record.reader.hdfs.parquet.converter.nested;

import java.io.DataOutput;
import java.io.IOException;

import org.apache.asterix.builders.IARecordBuilder;
import org.apache.asterix.builders.IAsterixListBuilder;
import org.apache.asterix.external.input.filter.embedder.IExternalFilterValueEmbedder;
import org.apache.asterix.external.input.record.reader.hdfs.parquet.converter.AbstractComplexConverter;
import org.apache.asterix.external.input.record.reader.hdfs.parquet.converter.IFieldValue;
import org.apache.asterix.external.input.record.reader.hdfs.parquet.converter.ParquetConverterContext;
import org.apache.asterix.external.input.record.reader.hdfs.parquet.converter.primitve.PrimitiveConverterProvider;
import org.apache.asterix.om.pointables.base.DefaultOpenFieldType;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.api.IMutableValueStorage;
import org.apache.hyracks.data.std.api.IValueReference;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.apache.parquet.io.api.PrimitiveConverter;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.PrimitiveType;

/**
 * Handles the following non-standard parquet schema scenario:
 * a group repeated list that is not contained in a LIST structure, for example
 *
 * {
 *     "my_group_list": [{"date": "123", "author": "abc"}, {"date": "456", "author": "xyz"}]
 * }
 *
 * Represented as:
 * message Product {
 *   repeated group name=my_group_list {
 *     required binary name=date (STRING);
 *     required binary name=author (STRING);
 *   }
 * }
 *
 * Instead of:
 * message arrow_schema {
 *   required group myGroupArray (LIST) {
 *     repeated group list {
 *       optional group  {
 *         optional binary hello (STRING);
 *         optional binary foo (STRING);
 *       }
 *     }
 *   }
 * }
 *
 *  In this case, this is a list and the type of the repeated is the type of the elements in the list
 */
public class ObjectRepeatedConverter extends AbstractComplexConverter {
    private IAsterixListBuilder listBuilder;
    private IMutableValueStorage listStorage;

    private IARecordBuilder recordBuilder;
    /**
     * {@link IExternalFilterValueEmbedder} decides whether the object should be ignored entirely
     */
    private boolean ignore = false;

    public ObjectRepeatedConverter(AbstractComplexConverter parent, String stringFieldName, int index,
            GroupType parquetType, ParquetConverterContext context) throws IOException {
        super(parent, stringFieldName, index, parquetType, context);
    }

    @Override
    public void start() {
        tempStorage = context.enterObject();
        recordBuilder = context.getObjectBuilder(DefaultOpenFieldType.NESTED_OPEN_RECORD_TYPE);
        IExternalFilterValueEmbedder valueEmbedder = context.getValueEmbedder();
        if (isRoot()) {
            valueEmbedder.reset();
            valueEmbedder.enterObject();
        } else {
            ignore = checkValueEmbedder(valueEmbedder);
        }
    }

    @Override
    public void end() {
        closeDirectRepeatedChildren();
        if (!ignore) {
            writeToList();
            context.getValueEmbedder().exitObject();
        }
        context.exitObject(tempStorage, null, recordBuilder);
        tempStorage = null;
        recordBuilder = null;
        ignore = false;
    }

    private void writeToList() {
        try {
            finalizeEmbedding();
            recordBuilder.write(getListDataOutput(), true);
            addValueToList();
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }

    private void addValueToList() {
        try {
            listBuilder.addItem(listStorage);
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }

    public DataOutput getListDataOutput() {
        if (listStorage == null) {
            internalStart();
        }
        listStorage.reset();
        return listStorage.getDataOutput();
    }

    @Override
    public ATypeTag getTypeTag() {
        return ATypeTag.ARRAY;
    }

    private void internalStart() {
        listStorage = context.enterCollection();
        listBuilder = context.getCollectionBuilder(DefaultOpenFieldType.NESTED_OPEN_AORDERED_LIST_TYPE);
    }

    @Override
    public void internalEnd() {
        closeDirectRepeatedChildren();
        try {
            listBuilder.write(parent.getDataOutput(), true);
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
        parent.addValue(this);
        context.exitCollection(listStorage, listBuilder);
        listStorage = null;
        listBuilder = null;
    }

    @Override
    public void addValue(IFieldValue value) {
        if (ignore) {
            // The value was embedded already
            return;
        }
        IExternalFilterValueEmbedder valueEmbedder = context.getValueEmbedder();
        IValueReference fieldName = value.getFieldName();
        try {
            if (valueEmbedder.shouldEmbed(value.getStringFieldName(), value.getTypeTag())) {
                recordBuilder.addField(fieldName, valueEmbedder.getEmbeddedValue());
            } else {
                recordBuilder.addField(fieldName, getValue());
            }
        } catch (HyracksDataException e) {
            throw new IllegalStateException(e);
        }
    }

    @Override
    protected PrimitiveConverter createAtomicConverter(GroupType type, int index) {
        try {
            PrimitiveType primitiveType = type.getType(index).asPrimitiveType();
            String childFieldName = type.getFieldName(index);
            return PrimitiveConverterProvider.createPrimitiveConverter(primitiveType, this, childFieldName, index,
                    context);
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }

    @Override
    protected ArrayConverter createArrayConverter(GroupType type, int index) {
        try {
            GroupType arrayType = type.getType(index).asGroupType();
            String childFieldName = type.getFieldName(index);
            return new ArrayConverter(this, childFieldName, index, arrayType, context);
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }

    @Override
    protected ObjectConverter createObjectConverter(GroupType type, int index) {
        try {
            GroupType objectType = type.getType(index).asGroupType();
            String childFieldName = type.getFieldName(index);
            return new ObjectConverter(this, childFieldName, index, objectType, context);
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }

    protected boolean isRoot() {
        return false;
    }

    private boolean checkValueEmbedder(IExternalFilterValueEmbedder valueEmbedder) {
        boolean embed = valueEmbedder.shouldEmbed(getStringFieldName(), ATypeTag.OBJECT);
        if (embed) {
            ((ArrayBackedValueStorage) parent.getValue()).set(valueEmbedder.getEmbeddedValue());
            addValueToList();
        } else {
            valueEmbedder.enterObject();
        }
        return embed;
    }

    private void finalizeEmbedding() throws IOException {
        IExternalFilterValueEmbedder valueEmbedder = context.getValueEmbedder();
        if (valueEmbedder.isMissingEmbeddedValues()) {
            String[] embeddedFieldNames = valueEmbedder.getEmbeddedFieldNames();
            for (int i = 0; i < embeddedFieldNames.length; i++) {
                String embeddedFieldName = embeddedFieldNames[i];
                if (valueEmbedder.isMissing(embeddedFieldName)) {
                    IValueReference embeddedValue = valueEmbedder.getEmbeddedValue();
                    recordBuilder.addField(context.getSerializedFieldName(embeddedFieldName), embeddedValue);
                }
            }
        }
    }
}
