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

import static org.apache.asterix.runtime.evaluators.functions.PointableHelper.NULL_REF;

import java.io.IOException;

import org.apache.asterix.builders.IARecordBuilder;
import org.apache.asterix.external.input.filter.embedder.IExternalFilterValueEmbedder;
import org.apache.asterix.external.input.record.reader.hdfs.parquet.converter.AbstractComplexConverter;
import org.apache.asterix.external.input.record.reader.hdfs.parquet.converter.IFieldValue;
import org.apache.asterix.external.input.record.reader.hdfs.parquet.converter.ParquetConverterContext;
import org.apache.asterix.external.input.record.reader.hdfs.parquet.converter.primitve.PrimitiveConverterProvider;
import org.apache.asterix.om.pointables.base.DefaultOpenFieldType;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.api.IValueReference;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.apache.parquet.io.api.PrimitiveConverter;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.PrimitiveType;

public class ObjectConverter extends AbstractComplexConverter {
    private IARecordBuilder builder;
    /**
     * {@link IExternalFilterValueEmbedder} decides whether the object should be ignored entirely
     */
    private boolean ignore = false;
    private final GroupType parquetType;
    private final boolean[] isValueAdded;

    public ObjectConverter(AbstractComplexConverter parent, int index, GroupType parquetType,
            ParquetConverterContext context) throws IOException {
        super(parent, index, parquetType, context);
        this.parquetType = parquetType;
        isValueAdded = new boolean[parquetType.getFieldCount()];
    }

    public ObjectConverter(AbstractComplexConverter parent, String stringFieldName, int index, GroupType parquetType,
            ParquetConverterContext context) throws IOException {
        super(parent, stringFieldName, index, parquetType, context);
        this.parquetType = parquetType;
        isValueAdded = new boolean[parquetType.getFieldCount()];
    }

    @Override
    public void start() {
        tempStorage = context.enterObject();
        builder = context.getObjectBuilder(DefaultOpenFieldType.NESTED_OPEN_RECORD_TYPE);
        IExternalFilterValueEmbedder valueEmbedder = context.getValueEmbedder();
        if (isRoot()) {
            valueEmbedder.reset();
            valueEmbedder.enterObject();
        } else {
            ignore = checkValueEmbedder(valueEmbedder);
        }
        for (int i = 0; i < parquetType.getFieldCount(); i++) {
            isValueAdded[i] = false;
        }

    }

    @Override
    public void end() {
        closeDirectRepeatedChildren();
        if (!ignore) {
            IExternalFilterValueEmbedder valueEmbedder = context.getValueEmbedder();
            for (int i = 0; i < parquetType.getFieldCount(); i++) {
                if (!isValueAdded[i]) {
                    String childFieldName = parquetType.getFieldName(i);
                    try {
                        if (valueEmbedder.shouldEmbed(childFieldName, ATypeTag.NULL)) {
                            builder.addField(context.getSerializedFieldName(childFieldName),
                                    valueEmbedder.getEmbeddedValue());
                        } else {
                            builder.addField(context.getSerializedFieldName(childFieldName), NULL_REF);
                        }
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                }
            }

            writeToParent();
            context.getValueEmbedder().exitObject();
        }

        context.exitObject(tempStorage, null, builder);
        tempStorage = null;
        builder = null;
        ignore = false;
    }

    @Override
    public ATypeTag getTypeTag() {
        return ATypeTag.OBJECT;
    }

    @Override
    public void addValue(IFieldValue value) {
        if (ignore) {
            // The value was embedded already
            return;
        }
        IExternalFilterValueEmbedder valueEmbedder = context.getValueEmbedder();
        IValueReference fieldName = value.getFieldName();
        String fieldNameStr = value.getStringFieldName();
        int fieldIndex = parquetType.getFieldIndex(fieldNameStr);
        isValueAdded[fieldIndex] = true;
        try {
            if (valueEmbedder.shouldEmbed(value.getStringFieldName(), value.getTypeTag())) {
                builder.addField(fieldName, valueEmbedder.getEmbeddedValue());
            } else {
                builder.addField(fieldName, getValue());
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
            addThisValueToParent();
        } else {
            valueEmbedder.enterObject();
        }
        return embed;
    }

    private void writeToParent() {
        try {
            finalizeEmbedding();
            builder.write(getParentDataOutput(), true);
            addThisValueToParent();
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }

    private void finalizeEmbedding() throws IOException {
        IExternalFilterValueEmbedder valueEmbedder = context.getValueEmbedder();
        if (valueEmbedder.isMissingEmbeddedValues()) {
            String[] embeddedFieldNames = valueEmbedder.getEmbeddedFieldNames();
            for (int i = 0; i < embeddedFieldNames.length; i++) {
                String embeddedFieldName = embeddedFieldNames[i];
                if (valueEmbedder.isMissing(embeddedFieldName)) {
                    IValueReference embeddedValue = valueEmbedder.getEmbeddedValue();
                    builder.addField(context.getSerializedFieldName(embeddedFieldName), embeddedValue);
                }
            }
        }
    }
}
