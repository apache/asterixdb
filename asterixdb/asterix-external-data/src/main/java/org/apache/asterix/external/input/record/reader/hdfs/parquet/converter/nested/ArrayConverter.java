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

import java.io.IOException;

import org.apache.asterix.builders.IAsterixListBuilder;
import org.apache.asterix.external.input.record.reader.hdfs.parquet.converter.IFieldValue;
import org.apache.asterix.external.input.record.reader.hdfs.parquet.converter.ParquetConverterContext;
import org.apache.asterix.external.input.record.reader.hdfs.parquet.converter.primitve.PrimitiveConverterProvider;
import org.apache.asterix.om.pointables.base.DefaultOpenFieldType;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.parquet.io.api.PrimitiveConverter;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.PrimitiveType;

class ArrayConverter extends AbstractComplexConverter {
    private IAsterixListBuilder builder;

    public ArrayConverter(AbstractComplexConverter parent, int index, GroupType parquetType,
            ParquetConverterContext context) throws IOException {
        super(parent, index, parquetType, context);
    }

    public ArrayConverter(AbstractComplexConverter parent, String stringFieldName, int index, GroupType parquetType,
            ParquetConverterContext context) throws IOException {
        super(parent, stringFieldName, index, parquetType, context);
    }

    @Override
    public void start() {
        tempStorage = context.enterCollection();
        builder = context.getCollectionBuilder(DefaultOpenFieldType.NESTED_OPEN_AORDERED_LIST_TYPE);
    }

    @Override
    public void end() {
        try {
            builder.write(getParentDataOutput(), true);
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
        addThisValueToParent();
        context.exitCollection(tempStorage, builder);
        tempStorage = null;
        builder = null;
    }

    @Override
    public ATypeTag getTypeTag() {
        return ATypeTag.ARRAY;
    }

    @Override
    public void addValue(IFieldValue value) {
        try {
            builder.addItem(tempStorage);
        } catch (HyracksDataException e) {
            throw new IllegalStateException(e);
        }
    }

    @Override
    protected PrimitiveConverter createAtomicConverter(GroupType type, int index) {
        try {
            PrimitiveType primitiveType = type.getType(index).asPrimitiveType();
            return PrimitiveConverterProvider.createPrimitiveConverter(primitiveType, this, index, context);
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }

    @Override
    protected ArrayConverter createArrayConverter(GroupType type, int index) {
        try {
            GroupType arrayType = type.getType(index).asGroupType();
            return new ArrayConverter(this, index, arrayType, context);
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }

    @Override
    protected ObjectConverter createObjectConverter(GroupType type, int index) {
        try {
            GroupType objectType = type.getType(index).asGroupType();
            return new ObjectConverter(this, index, objectType, context);
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }
}
