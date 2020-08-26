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

package org.apache.asterix.metadata.entitytupletranslators;

import java.io.ByteArrayInputStream;
import java.io.DataInput;
import java.io.DataInputStream;

import org.apache.asterix.builders.IARecordBuilder;
import org.apache.asterix.builders.RecordBuilder;
import org.apache.asterix.formats.nontagged.SerializerDeserializerProvider;
import org.apache.asterix.metadata.api.IMetadataEntityTupleTranslator;
import org.apache.asterix.metadata.api.IMetadataIndex;
import org.apache.asterix.om.base.ABoolean;
import org.apache.asterix.om.base.AInt32;
import org.apache.asterix.om.base.AInt64;
import org.apache.asterix.om.base.AMutableString;
import org.apache.asterix.om.base.ANull;
import org.apache.asterix.om.base.ARecord;
import org.apache.asterix.om.base.AString;
import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.api.dataflow.value.ISerializerDeserializer;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.apache.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import org.apache.hyracks.dataflow.common.comm.io.ArrayTupleReference;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;

/**
 * Contains common members shared across all concrete implementations of
 * IMetadataEntityTupleTranslator.
 */
public abstract class AbstractTupleTranslator<T> implements IMetadataEntityTupleTranslator<T> {

    @SuppressWarnings("unchecked")
    protected final ISerializerDeserializer<ANull> nullSerde =
            SerializerDeserializerProvider.INSTANCE.getSerializerDeserializer(BuiltinType.ANULL);
    @SuppressWarnings("unchecked")
    protected final ISerializerDeserializer<AString> stringSerde =
            SerializerDeserializerProvider.INSTANCE.getSerializerDeserializer(BuiltinType.ASTRING);
    @SuppressWarnings("unchecked")
    protected final ISerializerDeserializer<ABoolean> booleanSerde =
            SerializerDeserializerProvider.INSTANCE.getSerializerDeserializer(BuiltinType.ABOOLEAN);
    @SuppressWarnings("unchecked")
    protected final ISerializerDeserializer<AInt32> int32Serde =
            SerializerDeserializerProvider.INSTANCE.getSerializerDeserializer(BuiltinType.AINT32);
    @SuppressWarnings("unchecked")
    protected final ISerializerDeserializer<AInt64> int64Serde =
            SerializerDeserializerProvider.INSTANCE.getSerializerDeserializer(BuiltinType.AINT64);

    protected final ISerializerDeserializer<ARecord> recordSerDes;

    protected AMutableString aString = new AMutableString("");

    protected final ARecordType payloadRecordType;
    protected final int payloadTupleFieldIndex;
    protected final IARecordBuilder recordBuilder;
    protected final ArrayBackedValueStorage fieldName;
    protected final ArrayBackedValueStorage fieldValue;
    protected final ArrayTupleBuilder tupleBuilder;
    protected final ArrayTupleReference tuple;

    @SuppressWarnings("unchecked")
    protected AbstractTupleTranslator(boolean getTuple, IMetadataIndex metadataIndex, int payloadTupleFieldIndex) {
        payloadRecordType = metadataIndex.getPayloadRecordType();
        recordSerDes = SerializerDeserializerProvider.INSTANCE.getSerializerDeserializer(payloadRecordType);
        this.payloadTupleFieldIndex = payloadTupleFieldIndex;
        if (getTuple) {
            recordBuilder = new RecordBuilder();
            fieldName = payloadRecordType.isOpen() ? new ArrayBackedValueStorage() : null;
            fieldValue = new ArrayBackedValueStorage();
            tupleBuilder = new ArrayTupleBuilder(metadataIndex.getFieldCount());
            tuple = new ArrayTupleReference();
        } else {
            recordBuilder = null;
            fieldName = null;
            fieldValue = null;
            tupleBuilder = null;
            tuple = null;
        }
    }

    @Override
    public final T getMetadataEntityFromTuple(ITupleReference frameTuple)
            throws HyracksDataException, AlgebricksException {
        byte[] serRecord = frameTuple.getFieldData(payloadTupleFieldIndex);
        int recordStartOffset = frameTuple.getFieldStart(payloadTupleFieldIndex);
        int recordLength = frameTuple.getFieldLength(payloadTupleFieldIndex);
        ByteArrayInputStream stream = new ByteArrayInputStream(serRecord, recordStartOffset, recordLength);
        DataInput in = new DataInputStream(stream);
        ARecord datasetRecord = recordSerDes.deserialize(in);
        return createMetadataEntityFromARecord(datasetRecord);
    }

    protected abstract T createMetadataEntityFromARecord(ARecord aRecord)
            throws HyracksDataException, AlgebricksException;
}
