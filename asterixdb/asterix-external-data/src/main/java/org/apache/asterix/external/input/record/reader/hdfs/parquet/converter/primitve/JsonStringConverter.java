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

import org.apache.asterix.external.input.record.reader.hdfs.parquet.converter.ParquetConverterContext;
import org.apache.asterix.external.input.record.reader.hdfs.parquet.converter.nested.AbstractComplexConverter;
import org.apache.asterix.external.parser.JSONDataParser;
import org.apache.asterix.om.pointables.base.DefaultOpenFieldType;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.api.IValueReference;
import org.apache.hyracks.data.std.util.ByteArrayAccessibleInputStream;
import org.apache.parquet.io.api.Binary;

import com.fasterxml.jackson.core.JsonFactory;

class JsonStringConverter extends GenericPrimitiveConverter {
    private static final byte[] EMPTY = new byte[0];
    private final JSONDataParser parser;
    private final ByteArrayAccessibleInputStream in;

    JsonStringConverter(AbstractComplexConverter parent, String stringFieldName, int index,
            ParquetConverterContext context) throws IOException {
        super(ATypeTag.ANY, parent, stringFieldName, index, context);
        parser = new JSONDataParser(DefaultOpenFieldType.NESTED_OPEN_RECORD_TYPE, new JsonFactory());
        in = new ByteArrayAccessibleInputStream(EMPTY, 0, 0);
        try {
            parser.setInputStream(in);
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }

    @Override
    public ATypeTag getTypeTag() {
        IValueReference value = parent.getValue();
        byte[] bytes = value.getByteArray();
        int startOffset = value.getStartOffset();
        return ATypeTag.VALUE_TYPE_MAPPING[bytes[startOffset]];
    }

    @Override
    public void addBinary(Binary value) {
        byte[] bytes = value.getBytes();
        in.setContent(bytes, 0, value.length());

        DataOutput out = parent.getDataOutput();
        try {
            if (parser.parseAnyValue(out)) {
                parent.addValue(this);
            } else {
                resetParser();
            }
        } catch (HyracksDataException e) {
            resetParser();
        }
    }

    private void resetParser() {
        try {
            parser.reset(in);
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }

    }
}
