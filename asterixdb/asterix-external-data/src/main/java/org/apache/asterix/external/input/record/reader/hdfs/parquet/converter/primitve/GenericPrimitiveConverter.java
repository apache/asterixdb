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
package org.apache.asterix.external.input.record.reader.hdfs.parquet.converter.primitve;

import java.io.IOException;

import org.apache.asterix.external.input.record.reader.hdfs.parquet.converter.IFieldValue;
import org.apache.asterix.external.input.record.reader.hdfs.parquet.converter.ParquetConverterContext;
import org.apache.asterix.external.input.record.reader.hdfs.parquet.converter.nested.AbstractComplexConverter;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.hyracks.data.std.api.IValueReference;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.io.api.PrimitiveConverter;

public class GenericPrimitiveConverter extends PrimitiveConverter implements IFieldValue {
    private final ATypeTag typeTag;
    protected final AbstractComplexConverter parent;
    protected final String stringFieldName;
    protected final IValueReference fieldName;
    protected final int index;
    protected final ParquetConverterContext context;

    GenericPrimitiveConverter(ATypeTag typeTag, AbstractComplexConverter parent, String stringFieldName, int index,
            ParquetConverterContext context) throws IOException {
        this.typeTag = typeTag;
        this.parent = parent;
        this.stringFieldName = stringFieldName;
        this.fieldName = context.getSerializedFieldName(stringFieldName);
        this.index = index;
        this.context = context;
    }

    @Override
    public ATypeTag getTypeTag() {
        return typeTag;
    }

    @Override
    public String getStringFieldName() {
        return stringFieldName;
    }

    @Override
    public final IValueReference getFieldName() {
        return fieldName;
    }

    @Override
    public final int getIndex() {
        return index;
    }

    @Override
    public void addBinary(Binary value) {
        context.serializeString(value, parent.getDataOutput());
        parent.addValue(this);
    }

    @Override
    public void addBoolean(boolean value) {
        context.serializeBoolean(value, parent.getDataOutput());
        parent.addValue(this);
    }

    @Override
    public void addFloat(float value) {
        addDouble(value);
    }

    @Override
    public void addDouble(double value) {
        context.serializeDouble(value, parent.getDataOutput());
        parent.addValue(this);
    }

    @Override
    public void addInt(int value) {
        addLong(value);
    }

    @Override
    public void addLong(long value) {
        context.serializeInt64(value, parent.getDataOutput());
        parent.addValue(this);
    }
}
