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

import java.io.DataOutput;
import java.io.IOException;

import org.apache.asterix.builders.IAsterixListBuilder;
import org.apache.asterix.external.input.record.reader.hdfs.parquet.converter.AbstractComplexConverter;
import org.apache.asterix.external.input.record.reader.hdfs.parquet.converter.ParquetConverterContext;
import org.apache.asterix.om.pointables.base.DefaultOpenFieldType;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.api.IMutableValueStorage;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.PrimitiveType;

/**
 * Handles the following non-standard parquet schema scenario:
 * a primitive repeated list that is not contained in a LIST structure, for example
 *
 * {
 *     "my_primitive_array": ["item1", "item2"]
 * }
 *
 * Represented as:
 * message schema {
 *  repeated binary name=my_primitive_array (STRING);
 * }
 *
 * Instead of:
 * message schema {
 *   required group my_primitive_array (LIST) {
 *     repeated group list {
 *       optional binary  (STRING);
 *     }
 *   }
 * }
 *
 *  In this case, this is a list and the type of the repeated is the type of the elements in the list
 */
public class PrimitiveRepeatedConverter extends GenericPrimitiveConverter {
    private IAsterixListBuilder builder;

    protected IMutableValueStorage tempStorage;

    public PrimitiveRepeatedConverter(ATypeTag typeTag, AbstractComplexConverter parent, PrimitiveType type, int index,
            ParquetConverterContext context) throws IOException {
        super(typeTag, parent, type.getName(), index, context);
    }

    private void internalStart() {
        tempStorage = context.enterCollection();
        builder = context.getCollectionBuilder(DefaultOpenFieldType.NESTED_OPEN_AORDERED_LIST_TYPE);
    }

    @Override
    public void internalEnd() {
        try {
            builder.write(parent.getDataOutput(), true);
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
        parent.addValue(this);
        context.exitCollection(tempStorage, builder);
        tempStorage = null;
        builder = null;
    }

    public DataOutput getDataOutput() {
        if (tempStorage == null) {
            internalStart();
        }
        tempStorage.reset();
        return tempStorage.getDataOutput();
    }

    public void addValue() {
        try {
            builder.addItem(tempStorage);
        } catch (HyracksDataException e) {
            throw new IllegalStateException(e);
        }
    }

    @Override
    public void addBinary(Binary value) {
        context.serializeString(value, getDataOutput());
        addValue();
    }

    @Override
    public void addBoolean(boolean value) {
        context.serializeBoolean(value, getDataOutput());
        addValue();
    }

    @Override
    public void addFloat(float value) {
        addDouble(value);
    }

    @Override
    public void addDouble(double value) {
        context.serializeDouble(value, getDataOutput());
        addValue();
    }

    @Override
    public void addInt(int value) {
        addLong(value);
    }

    @Override
    public void addLong(long value) {
        context.serializeInt64(value, getDataOutput());
        addValue();
    }
}
