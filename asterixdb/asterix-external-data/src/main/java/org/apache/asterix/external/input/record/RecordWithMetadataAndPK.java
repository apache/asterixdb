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

import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.common.exceptions.RuntimeDataException;
import org.apache.asterix.external.api.IDataParser;
import org.apache.asterix.external.api.IRawRecord;
import org.apache.asterix.external.util.ExternalDataUtils;
import org.apache.asterix.formats.nontagged.SerializerDeserializerProvider;
import org.apache.asterix.om.base.ABoolean;
import org.apache.asterix.om.base.ADouble;
import org.apache.asterix.om.base.AInt32;
import org.apache.asterix.om.base.AInt64;
import org.apache.asterix.om.base.AMutableDouble;
import org.apache.asterix.om.base.AMutableInt32;
import org.apache.asterix.om.base.AMutableInt64;
import org.apache.asterix.om.base.AMutableString;
import org.apache.asterix.om.base.AString;
import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.asterix.om.types.IAType;
import org.apache.asterix.om.utils.NonTaggedFormatUtil;
import org.apache.hyracks.api.dataflow.value.ISerializerDeserializer;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.apache.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import org.apache.hyracks.dataflow.common.data.parsers.IValueParser;

public class RecordWithMetadataAndPK<T> extends RecordWithPK<T> {

    private final ArrayBackedValueStorage[] fieldValueBuffers;
    private final DataOutput[] fieldValueBufferOutputs;
    private final IValueParser[] valueParsers;
    private final byte[] fieldTypeTags;
    private final IAType[] metaTypes;

    // Serializers
    @SuppressWarnings("unchecked")
    private final ISerializerDeserializer<ADouble> doubleSerde =
            SerializerDeserializerProvider.INSTANCE.getSerializerDeserializer(BuiltinType.ADOUBLE);
    private final AMutableDouble mutableDouble = new AMutableDouble(0);
    @SuppressWarnings("unchecked")
    private final ISerializerDeserializer<AString> stringSerde =
            SerializerDeserializerProvider.INSTANCE.getSerializerDeserializer(BuiltinType.ASTRING);
    private final AMutableString mutableString = new AMutableString(null);
    @SuppressWarnings("unchecked")
    private final ISerializerDeserializer<AInt32> int32Serde =
            SerializerDeserializerProvider.INSTANCE.getSerializerDeserializer(BuiltinType.AINT32);
    private final AMutableInt32 mutableInt = new AMutableInt32(0);
    @SuppressWarnings("unchecked")
    protected ISerializerDeserializer<AInt64> int64Serde =
            SerializerDeserializerProvider.INSTANCE.getSerializerDeserializer(BuiltinType.AINT64);
    private final AMutableInt64 mutableLong = new AMutableInt64(0);
    @SuppressWarnings("unchecked")
    private final ISerializerDeserializer<ABoolean> booleanSerde =
            SerializerDeserializerProvider.INSTANCE.getSerializerDeserializer(BuiltinType.ABOOLEAN);
    private final int[] keyIndicator;

    public RecordWithMetadataAndPK(final IRawRecord<T> record, final IAType[] metaTypes, final ARecordType recordType,
            final int[] keyIndicator, final int[] pkIndexes, final IAType[] keyTypes) {
        super(record, keyTypes, pkIndexes);
        this.metaTypes = metaTypes;
        this.fieldValueBuffers = new ArrayBackedValueStorage[metaTypes.length];
        this.fieldValueBufferOutputs = new DataOutput[metaTypes.length];
        this.valueParsers = new IValueParser[metaTypes.length];
        this.fieldTypeTags = new byte[metaTypes.length];
        for (int i = 0; i < metaTypes.length; i++) {
            final ATypeTag tag = metaTypes[i].getTypeTag();
            fieldTypeTags[i] = tag.serialize();
            fieldValueBuffers[i] = new ArrayBackedValueStorage();
            fieldValueBufferOutputs[i] = fieldValueBuffers[i].getDataOutput();
            valueParsers[i] = ExternalDataUtils.getParserFactory(tag).createValueParser();
        }
        this.keyIndicator = keyIndicator;
    }

    @Override
    public IRawRecord<T> getRecord() {
        return record;
    }

    public IAType[] getMetaTypes() {
        return metaTypes;
    }

    public ArrayBackedValueStorage getMetadata(final int index) {
        return fieldValueBuffers[index];
    }

    @Override
    public void reset() {
        record.reset();
        for (final ArrayBackedValueStorage fieldBuffer : fieldValueBuffers) {
            fieldBuffer.reset();
        }
    }

    public void setMetadata(final int index, final int value) throws IOException {
        fieldValueBufferOutputs[index].writeByte(fieldTypeTags[index]);
        mutableInt.setValue(value);
        IDataParser.toBytes(mutableInt, fieldValueBuffers[index], int32Serde);
    }

    public void setMetadata(final int index, final long value) throws IOException {
        fieldValueBufferOutputs[index].writeByte(fieldTypeTags[index]);
        mutableLong.setValue(value);
        IDataParser.toBytes(mutableLong, fieldValueBuffers[index], int64Serde);
    }

    public void setMetadata(final int index, final String value) throws IOException {
        fieldValueBufferOutputs[index].writeByte(fieldTypeTags[index]);
        mutableString.setValue(value);
        IDataParser.toBytes(mutableString, fieldValueBuffers[index], stringSerde);
    }

    public void setMetadata(final int index, final boolean value) throws IOException {
        fieldValueBufferOutputs[index].writeByte(fieldTypeTags[index]);
        IDataParser.toBytes(value ? ABoolean.TRUE : ABoolean.FALSE, fieldValueBuffers[index], booleanSerde);
    }

    public void setMetadata(final int index, final double value) throws IOException {
        fieldValueBufferOutputs[index].writeByte(fieldTypeTags[index]);
        mutableDouble.setValue(value);
        IDataParser.toBytes(mutableDouble, fieldValueBuffers[index], doubleSerde);
    }

    public void setRawMetadata(final int index, final char[] src, final int offset, final int length)
            throws IOException {
        if (length == 0) {
            if (!NonTaggedFormatUtil.isOptional(metaTypes[index])) {
                throw new RuntimeDataException(ErrorCode.INPUT_RECORD_RECORD_WITH_METADATA_AND_PK_NULL_IN_NON_OPTIONAL,
                        index);
            }
            fieldValueBufferOutputs[index].writeByte(ATypeTag.SERIALIZED_NULL_TYPE_TAG);
        } else {
            fieldValueBufferOutputs[index].writeByte(fieldTypeTags[index]);
            valueParsers[index].parse(src, offset, length, fieldValueBufferOutputs[index]);
        }
    }

    @Override
    public void appendPrimaryKeyToTuple(final ArrayTupleBuilder tb) throws HyracksDataException {
        for (int i = 0; i < pkIndexes.length; i++) {
            if (keyIndicator[i] == 1) {
                tb.addField(getMetadata(pkIndexes[i]));
            } else {
                throw new RuntimeDataException(ErrorCode.INPUT_RECORD_RECORD_WITH_METADATA_AND_PK_CANNT_GET_PKEY);
            }
        }
    }

    @Override
    public byte[] getBytes() {
        return record.getBytes();
    }

    @Override
    public T get() {
        return record.get();
    }

    @Override
    public int size() {
        return record.size();
    }

    @Override
    public void set(final T t) {
        record.set(t);
    }
}
