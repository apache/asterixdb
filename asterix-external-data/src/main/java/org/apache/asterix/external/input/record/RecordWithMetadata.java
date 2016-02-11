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
package org.apache.asterix.external.input.record;

import java.io.DataOutput;
import java.io.IOException;

import org.apache.asterix.external.api.IDataParser;
import org.apache.asterix.external.api.IRawRecord;
import org.apache.asterix.external.util.ExternalDataUtils;
import org.apache.asterix.formats.nontagged.AqlSerializerDeserializerProvider;
import org.apache.asterix.om.base.ABoolean;
import org.apache.asterix.om.base.ADouble;
import org.apache.asterix.om.base.AInt32;
import org.apache.asterix.om.base.AInt64;
import org.apache.asterix.om.base.AMutableDouble;
import org.apache.asterix.om.base.AMutableInt32;
import org.apache.asterix.om.base.AMutableInt64;
import org.apache.asterix.om.base.AMutableString;
import org.apache.asterix.om.base.AString;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.asterix.om.types.IAType;
import org.apache.hyracks.api.dataflow.value.ISerializerDeserializer;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.apache.hyracks.dataflow.common.data.parsers.IValueParserFactory;

public class RecordWithMetadata<T> {

    private ArrayBackedValueStorage[] fieldValueBuffers;
    private DataOutput[] fieldValueBufferOutputs;
    private IValueParserFactory[] valueParserFactories;
    private byte[] fieldTypeTags;
    private IRawRecord<T> record;

    // Serializers
    @SuppressWarnings("unchecked")
    private ISerializerDeserializer<ADouble> doubleSerde = AqlSerializerDeserializerProvider.INSTANCE
            .getSerializerDeserializer(BuiltinType.ADOUBLE);
    private AMutableDouble mutableDouble = new AMutableDouble(0);
    @SuppressWarnings("unchecked")
    private ISerializerDeserializer<AString> stringSerde = AqlSerializerDeserializerProvider.INSTANCE
            .getSerializerDeserializer(BuiltinType.ASTRING);
    private AMutableString mutableString = new AMutableString(null);
    @SuppressWarnings("unchecked")
    private ISerializerDeserializer<AInt32> int32Serde = AqlSerializerDeserializerProvider.INSTANCE
            .getSerializerDeserializer(BuiltinType.AINT32);
    private AMutableInt32 mutableInt = new AMutableInt32(0);
    @SuppressWarnings("unchecked")
    protected ISerializerDeserializer<AInt64> int64Serde = AqlSerializerDeserializerProvider.INSTANCE
            .getSerializerDeserializer(BuiltinType.AINT64);
    private AMutableInt64 mutableLong = new AMutableInt64(0);
    @SuppressWarnings("unchecked")
    private ISerializerDeserializer<ABoolean> booleanSerde = AqlSerializerDeserializerProvider.INSTANCE
            .getSerializerDeserializer(BuiltinType.ABOOLEAN);

    public RecordWithMetadata(Class<? extends T> recordClass) {
    }

    public RecordWithMetadata(IAType[] metaTypes, Class<? extends T> recordClass) {
        int n = metaTypes.length;
        this.fieldValueBuffers = new ArrayBackedValueStorage[n];
        this.fieldValueBufferOutputs = new DataOutput[n];
        this.valueParserFactories = new IValueParserFactory[n];
        this.fieldTypeTags = new byte[n];
        for (int i = 0; i < n; i++) {
            ATypeTag tag = metaTypes[i].getTypeTag();
            fieldTypeTags[i] = tag.serialize();
            fieldValueBuffers[i] = new ArrayBackedValueStorage();
            fieldValueBufferOutputs[i] = fieldValueBuffers[i].getDataOutput();
            valueParserFactories[i] = ExternalDataUtils.getParserFactory(tag);
        }
    }

    public IRawRecord<T> getRecord() {
        return record;
    }

    public ArrayBackedValueStorage getMetadata(int index) {
        return fieldValueBuffers[index];
    }

    public void setRecord(IRawRecord<T> record) {
        this.record = record;
    }

    public void reset() {
        record.reset();
        for (ArrayBackedValueStorage fieldBuffer : fieldValueBuffers) {
            fieldBuffer.reset();
        }
    }

    public void setMetadata(int index, int value) throws IOException {
        fieldValueBufferOutputs[index].writeByte(fieldTypeTags[index]);
        mutableInt.setValue(value);
        IDataParser.toBytes(mutableInt, fieldValueBuffers[index], int32Serde);
    }

    public void setMetadata(int index, long value) throws IOException {
        fieldValueBufferOutputs[index].writeByte(fieldTypeTags[index]);
        mutableLong.setValue(value);
        IDataParser.toBytes(mutableLong, fieldValueBuffers[index], int64Serde);
    }

    public void setMetadata(int index, String value) throws IOException {
        fieldValueBufferOutputs[index].writeByte(fieldTypeTags[index]);
        mutableString.setValue(value);
        IDataParser.toBytes(mutableString, fieldValueBuffers[index], stringSerde);
    }

    public void setMeta(int index, boolean value) throws IOException {
        fieldValueBufferOutputs[index].writeByte(fieldTypeTags[index]);
        IDataParser.toBytes(value ? ABoolean.TRUE : ABoolean.FALSE, fieldValueBuffers[index], booleanSerde);
    }

    public void setMeta(int index, double value) throws IOException {
        fieldValueBufferOutputs[index].writeByte(fieldTypeTags[index]);
        mutableDouble.setValue(value);
        IDataParser.toBytes(mutableDouble, fieldValueBuffers[index], doubleSerde);
    }
}
