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
package org.apache.asterix.external.input.record.reader.hdfs.parquet.converter;

import java.io.DataOutput;
import java.io.IOException;

import org.apache.asterix.external.input.record.reader.hdfs.parquet.AsterixTypeToParquetTypeVisitor;
import org.apache.asterix.external.input.record.reader.hdfs.parquet.converter.nested.ObjectConverter;
import org.apache.asterix.external.input.record.reader.hdfs.parquet.converter.nested.ObjectRepeatedConverter;
import org.apache.asterix.external.input.record.reader.hdfs.parquet.converter.nested.RepeatedConverter;
import org.apache.asterix.external.input.record.reader.hdfs.parquet.converter.primitve.IClosableRepeatedConverter;
import org.apache.asterix.external.input.record.reader.hdfs.parquet.converter.primitve.PrimitiveRepeatedConverter;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.hyracks.data.std.api.IMutableValueStorage;
import org.apache.hyracks.data.std.api.IValueReference;
import org.apache.parquet.io.api.Converter;
import org.apache.parquet.io.api.GroupConverter;
import org.apache.parquet.io.api.PrimitiveConverter;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.Type;
import org.apache.parquet.schema.Type.Repetition;

public abstract class AbstractComplexConverter extends GroupConverter
        implements IFieldValue, IClosableRepeatedConverter {
    protected AbstractComplexConverter parent;
    protected IValueReference fieldName;
    protected String stringFieldName;
    private int index;
    protected Converter[] converters;
    protected ParquetConverterContext context;
    protected IMutableValueStorage tempStorage;

    protected AbstractComplexConverter(AbstractComplexConverter parent, int index, GroupType parquetType,
            ParquetConverterContext context) throws IOException {
        this(parent, null, index, parquetType, context);
    }

    protected AbstractComplexConverter(AbstractComplexConverter parent, String stringFieldName, int index,
            GroupType parquetType, ParquetConverterContext context) throws IOException {
        this.parent = parent;
        this.stringFieldName = stringFieldName;
        this.fieldName = context.getSerializedFieldName(stringFieldName);
        this.index = index;
        this.context = context;
        converters = new Converter[parquetType.getFieldCount()];
        for (int i = 0; i < parquetType.getFieldCount(); i++) {
            final Type type = parquetType.getType(i);

            LogicalTypeAnnotation typeAnnotation = type.getLogicalTypeAnnotation();
            if (type.isPrimitive() && type.getRepetition() != Repetition.REPEATED) {
                converters[i] = createAtomicConverter(parquetType, i);
            } else if (LogicalTypeAnnotation.listType().equals(typeAnnotation)) {
                converters[i] = createArrayConverter(parquetType, i);
            } else if (LogicalTypeAnnotation.mapType().equals(typeAnnotation)
                    || LogicalTypeAnnotation.MapKeyValueTypeAnnotation.getInstance().equals(typeAnnotation)) {
                converters[i] = createArrayConverter(parquetType, i);
            } else if (isRepeated(parquetType, i)) {
                continue;
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

    private boolean isRepeated(GroupType parquetType, int index) throws IOException {
        Type repeatedType = parquetType.getType(index);
        if (repeatedType.getRepetition() != Repetition.REPEATED) {
            return false;
        }

        // primitive type
        if (repeatedType.isPrimitive()) {
            if (this.getTypeTag().isListType()) {
                // legacy 2-levels list, list -> repeated primitive type
                converters[index] = createAtomicConverter(parquetType, index);
            } else {
                // primitive outside ArrayConverter, this is a legacy list of the provided type on its own
                ATypeTag typeTag = AsterixTypeToParquetTypeVisitor.mapType(repeatedType, context, null);
                converters[index] =
                        new PrimitiveRepeatedConverter(typeTag, this, repeatedType.asPrimitiveType(), index, context);
            }
        } else {
            GroupType groupRepeatedType = repeatedType.asGroupType();
            String name = groupRepeatedType.getName();

            // if contained inside a list, then this is a repeated objects for the list
            if (this.getTypeTag().isListType()) {
                if (groupRepeatedType.getFieldCount() > 1 || "array".equals(name) || "key_value".equals(name)) {
                    // The name "array" and "key_value" are reserved names to represent array of objects
                    // "key_value" are for MAP type
                    converters[index] = new ObjectConverter(this, index, groupRepeatedType, context);
                } else {
                    // implementation following standards
                    converters[index] = new RepeatedConverter(this, index, groupRepeatedType, context);
                }
            } else {
                // repeated
                converters[index] = new ObjectRepeatedConverter(this, name, index, groupRepeatedType, context);
            }
        }
        return true;
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

    /**
     * This closes non-standard repeated converters, check implementers of {@link IClosableRepeatedConverter)
     */
    protected void closeDirectRepeatedChildren() {
        for (Converter converter : converters) {
            ((IClosableRepeatedConverter) converter).internalEnd();
        }
    }
}
