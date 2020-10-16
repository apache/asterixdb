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
package org.apache.asterix.external.input.record.reader.hdfs.parquet;

import java.io.DataOutput;

import org.apache.asterix.external.parser.jackson.ParserContext;
import org.apache.hyracks.data.std.api.IMutableValueStorage;
import org.apache.hyracks.data.std.api.IValueReference;
import org.apache.parquet.io.api.Converter;
import org.apache.parquet.io.api.GroupConverter;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.OriginalType;
import org.apache.parquet.schema.Type;
import org.apache.parquet.schema.Type.Repetition;

abstract class AbstractComplexConverter extends GroupConverter implements IFieldValue {
    protected final AbstractComplexConverter parent;
    private final IValueReference fieldName;
    private final int index;
    private final Converter[] converters;
    protected final ParserContext context;
    protected IMutableValueStorage tempStorage;

    public AbstractComplexConverter(AbstractComplexConverter parent, int index, GroupType parquetType,
            ParserContext context) {
        this(parent, null, index, parquetType, context);
    }

    public AbstractComplexConverter(AbstractComplexConverter parent, IValueReference fieldName, int index,
            GroupType parquetType, ParserContext context) {
        this.parent = parent;
        this.fieldName = fieldName;
        this.index = index;
        this.context = context;
        converters = new Converter[parquetType.getFieldCount()];
        for (int i = 0; i < parquetType.getFieldCount(); i++) {
            final Type type = parquetType.getType(i);
            if (type.isPrimitive()) {
                converters[i] = createAtomicConverter(parquetType, i);
            } else if (type.getOriginalType() == OriginalType.LIST) {
                converters[i] = createArrayConverter(parquetType, i);
            } else if (type.getRepetition() == Repetition.REPEATED) {
                converters[i] = createRepeatedConverter(parquetType, i);
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
    protected abstract void addValue(IFieldValue value);

    protected abstract AtomicConverter createAtomicConverter(GroupType type, int index);

    protected abstract ArrayConverter createArrayConverter(GroupType type, int index);

    protected abstract ObjectConverter createObjectConverter(GroupType type, int index);

    /**
     * Parquet file created by (old) Avro writer treat repeated values differently from files created by Spark.
     * Example:
     * Let us consider the object <pre>{"urls":[{"display_url": "string", "expanded_url": "string"}]}</pre>
     *
     * @formatter:off
     *
     * In Avro:
     * optional group urls (LIST) {
     *    // if number of fields > 1, then should be treated as array of objects
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
     * @formatter:on
     */
    public AbstractComplexConverter createRepeatedConverter(GroupType type, int index) {
        final GroupType arrayType = type.getType(index).asGroupType();
        if (arrayType.getFieldCount() == 1) {
            return new RepeatedConverter(this, index, arrayType, context);
        }
        return new ObjectConverter(this, index, arrayType, context);
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

    protected DataOutput getDataOutput() {
        tempStorage.reset();
        return tempStorage.getDataOutput();
    }

    protected IMutableValueStorage getValue() {
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
