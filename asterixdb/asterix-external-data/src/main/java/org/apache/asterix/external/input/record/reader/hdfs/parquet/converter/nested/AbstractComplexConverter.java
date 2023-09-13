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

import org.apache.asterix.external.input.record.reader.hdfs.parquet.converter.IFieldValue;
import org.apache.asterix.external.input.record.reader.hdfs.parquet.converter.ParquetConverterContext;
import org.apache.hyracks.data.std.api.IMutableValueStorage;
import org.apache.hyracks.data.std.api.IValueReference;
import org.apache.parquet.io.api.Converter;
import org.apache.parquet.io.api.GroupConverter;
import org.apache.parquet.io.api.PrimitiveConverter;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.Type;
import org.apache.parquet.schema.Type.Repetition;

public abstract class AbstractComplexConverter extends GroupConverter implements IFieldValue {
    protected final AbstractComplexConverter parent;
    private final IValueReference fieldName;
    private final String stringFieldName;
    private final int index;
    private final Converter[] converters;
    protected final ParquetConverterContext context;
    protected IMutableValueStorage tempStorage;

    AbstractComplexConverter(AbstractComplexConverter parent, int index, GroupType parquetType,
            ParquetConverterContext context) throws IOException {
        this(parent, null, index, parquetType, context);
    }

    AbstractComplexConverter(AbstractComplexConverter parent, String stringFieldName, int index, GroupType parquetType,
            ParquetConverterContext context) throws IOException {
        this.parent = parent;
        this.stringFieldName = stringFieldName;
        this.fieldName = context.getSerializedFieldName(stringFieldName);
        this.index = index;
        this.context = context;
        converters = new Converter[parquetType.getFieldCount()];
        for (int i = 0; i < parquetType.getFieldCount(); i++) {
            final Type type = parquetType.getType(i);
            if (type.isPrimitive()) {
                converters[i] = createAtomicConverter(parquetType, i);
            } else if (LogicalTypeAnnotation.listType().equals(type.getLogicalTypeAnnotation())) {
                converters[i] = createArrayConverter(parquetType, i);
            } else if (type.getRepetition() == Repetition.REPEATED) {
                converters[i] = createRepeatedConverter(parquetType, i);
            } else if (type.getLogicalTypeAnnotation() == LogicalTypeAnnotation.mapType()) {
                converters[i] = createArrayConverter(parquetType, i);
            } else {
                converters[i] = createObjectConverter(parquetType, i);
            }
        }
    }

    /**
     * Add child value (the caller is the child itself)
     *
     * @param value Child value
     */
    public abstract void addValue(IFieldValue value);

    protected abstract PrimitiveConverter createAtomicConverter(GroupType type, int index);

    protected abstract AbstractComplexConverter createArrayConverter(GroupType type, int index);

    protected abstract AbstractComplexConverter createObjectConverter(GroupType type, int index);

    /**
     * Parquet file created by (old) Avro writer treat repeated values differently from files created by Spark.
     * Example:
     * Let us consider the object <pre>{"urls":[{"display_url": "string", "expanded_url": "string"}]}</pre>
     *
     * @formatter:off
     *
     * In Avro:
     * optional group urls (LIST) {
     *    repeated group array {
     *       optional binary display_url (UTF8);
     *       optional binary expanded_url (UTF8);
     *    }
     * }
     *
     * In Spark:
     * optional group urls (LIST) {
     *    repeated group list {
     *       // Similar to JSON, the object fields are placed in an inner group
     *       optional group item {
     *          optional binary display_url (UTF8);
     *          optional binary expanded_url (UTF8);
     *       }
     *    }
     * }
     *
     * Map type:
     * required group mapField (MAP) {
     *    repeated group key_value {
     *       required int32 key;
     *       required int32 value;
     *    }
     * }
     *
     * @formatter:on
     */
    protected AbstractComplexConverter createRepeatedConverter(GroupType type, int index) throws IOException {
        GroupType repeatedType = type.getType(index).asGroupType();
        String name = repeatedType.getName();
        if (repeatedType.getFieldCount() > 1 || "array".equals(name) || "key_value".equals(name)) {
            //The name "array" and "key_value" are reserved names to represent array of objects
            //"key_value" are for MAP type
            return new ObjectConverter(this, index, repeatedType, context);
        }
        return new RepeatedConverter(this, index, repeatedType, context);
    }

    @Override
    public String getStringFieldName() {
        return stringFieldName;
    }

    @Override
    public IValueReference getFieldName() {
        return fieldName;
    }

    @Override
    public int getIndex() {
        return index;
    }

    @Override
    public Converter getConverter(int fieldIndex) {
        return converters[fieldIndex];
    }

    public DataOutput getDataOutput() {
        tempStorage.reset();
        return tempStorage.getDataOutput();
    }

    public IMutableValueStorage getValue() {
        return tempStorage;
    }

    protected DataOutput getParentDataOutput() {
        return parent.getDataOutput();
    }

    protected void addThisValueToParent() {
        if (parent == null) {
            //root
            return;
        }
        parent.addValue(this);
    }
}
