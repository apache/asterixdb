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
import java.io.IOException;

import org.apache.asterix.external.input.stream.StandardUTF8ToModifiedUTF8DataOutput;
import org.apache.asterix.external.parser.jackson.ParserContext;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.hyracks.data.std.api.IValueReference;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.io.api.PrimitiveConverter;

/**
 * Currently, only JSON types are supported (string, number, boolean)
 */
class AtomicConverter extends PrimitiveConverter implements IFieldValue {
    private final AbstractComplexConverter parent;
    private final IValueReference fieldName;
    private final int index;
    private final ParserContext context;

    public AtomicConverter(AbstractComplexConverter parent, int index, ParserContext context) {
        this(parent, null, index, context);
    }

    public AtomicConverter(AbstractComplexConverter parent, IValueReference fieldName, int index,
            ParserContext context) {
        this.parent = parent;
        this.fieldName = fieldName;
        this.index = index;
        this.context = context;
    }

    @Override
    public void addBinary(Binary value) {
        final DataOutput out = parent.getDataOutput();
        final StandardUTF8ToModifiedUTF8DataOutput stringOut = context.getModifiedUTF8DataOutput();
        stringOut.setDataOutput(out);
        try {
            out.writeByte(ATypeTag.STRING.serialize());
            value.writeTo(stringOut);
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
        parent.addValue(this);
    }

    @Override
    public void addBoolean(boolean value) {
        final DataOutput out = parent.getDataOutput();
        try {
            out.writeByte(ATypeTag.BOOLEAN.serialize());
            out.writeBoolean(value);
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
        parent.addValue(this);
    }

    @Override
    public void addFloat(float value) {
        addDouble(value);
    }

    @Override
    public void addDouble(double value) {
        final DataOutput out = parent.getDataOutput();
        try {
            out.writeByte(ATypeTag.DOUBLE.serialize());
            out.writeDouble(value);
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
        parent.addValue(this);
    }

    @Override
    public void addInt(int value) {
        addLong(value);
    }

    @Override
    public void addLong(long value) {
        final DataOutput out = parent.getDataOutput();
        try {
            out.writeByte(ATypeTag.BIGINT.serialize());
            out.writeLong(value);
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
        parent.addValue(this);
    }

    @Override
    public IValueReference getFieldName() {
        return fieldName;
    }

    @Override
    public int getIndex() {
        return index;
    }
}
