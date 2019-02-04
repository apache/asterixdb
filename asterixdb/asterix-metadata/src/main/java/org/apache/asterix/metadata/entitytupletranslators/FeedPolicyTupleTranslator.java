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
import java.io.DataOutput;
import java.util.HashMap;
import java.util.Map;

import org.apache.asterix.builders.IARecordBuilder;
import org.apache.asterix.builders.RecordBuilder;
import org.apache.asterix.builders.UnorderedListBuilder;
import org.apache.asterix.formats.nontagged.SerializerDeserializerProvider;
import org.apache.asterix.metadata.bootstrap.MetadataPrimaryIndexes;
import org.apache.asterix.metadata.bootstrap.MetadataRecordTypes;
import org.apache.asterix.metadata.entities.FeedPolicyEntity;
import org.apache.asterix.om.base.AInt32;
import org.apache.asterix.om.base.AMutableString;
import org.apache.asterix.om.base.ARecord;
import org.apache.asterix.om.base.AString;
import org.apache.asterix.om.base.AUnorderedList;
import org.apache.asterix.om.base.IACursor;
import org.apache.asterix.om.types.AUnorderedListType;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.api.dataflow.value.ISerializerDeserializer;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;

/**
 * Translates a Dataset metadata entity to an ITupleReference and vice versa.
 */
public class FeedPolicyTupleTranslator extends AbstractTupleTranslator<FeedPolicyEntity> {
    private static final long serialVersionUID = 826298425589924684L;

    // Field indexes of serialized FeedPolicy in a tuple.
    // Key field.
    public static final int FEED_POLICY_DATAVERSE_NAME_FIELD_INDEX = 0;

    public static final int FEED_POLICY_POLICY_NAME_FIELD_INDEX = 1;

    // Payload field containing serialized feedPolicy.
    public static final int FEED_POLICY_PAYLOAD_TUPLE_FIELD_INDEX = 2;

    @SuppressWarnings("unchecked")
    private ISerializerDeserializer<ARecord> recordSerDes = SerializerDeserializerProvider.INSTANCE
            .getSerializerDeserializer(MetadataRecordTypes.FEED_POLICY_RECORDTYPE);
    protected ISerializerDeserializer<AInt32> aInt32Serde;

    @SuppressWarnings("unchecked")
    protected FeedPolicyTupleTranslator(boolean getTuple) {
        super(getTuple, MetadataPrimaryIndexes.FEED_POLICY_DATASET.getFieldCount());
        aInt32Serde = SerializerDeserializerProvider.INSTANCE.getSerializerDeserializer(BuiltinType.AINT32);
    }

    @Override
    public FeedPolicyEntity getMetadataEntityFromTuple(ITupleReference frameTuple) throws HyracksDataException {
        byte[] serRecord = frameTuple.getFieldData(FEED_POLICY_PAYLOAD_TUPLE_FIELD_INDEX);
        int recordStartOffset = frameTuple.getFieldStart(FEED_POLICY_PAYLOAD_TUPLE_FIELD_INDEX);
        int recordLength = frameTuple.getFieldLength(FEED_POLICY_PAYLOAD_TUPLE_FIELD_INDEX);
        ByteArrayInputStream stream = new ByteArrayInputStream(serRecord, recordStartOffset, recordLength);
        DataInput in = new DataInputStream(stream);
        ARecord feedPolicyRecord = recordSerDes.deserialize(in);
        return createFeedPolicyFromARecord(feedPolicyRecord);
    }

    private FeedPolicyEntity createFeedPolicyFromARecord(ARecord feedPolicyRecord) {
        FeedPolicyEntity feedPolicy = null;
        String dataverseName = ((AString) feedPolicyRecord
                .getValueByPos(MetadataRecordTypes.FEED_POLICY_ARECORD_DATAVERSE_NAME_FIELD_INDEX)).getStringValue();
        String policyName = ((AString) feedPolicyRecord
                .getValueByPos(MetadataRecordTypes.FEED_POLICY_ARECORD_POLICY_NAME_FIELD_INDEX)).getStringValue();

        String description = ((AString) feedPolicyRecord
                .getValueByPos(MetadataRecordTypes.FEED_POLICY_ARECORD_DESCRIPTION_FIELD_INDEX)).getStringValue();

        IACursor cursor = ((AUnorderedList) feedPolicyRecord
                .getValueByPos(MetadataRecordTypes.FEED_POLICY_ARECORD_PROPERTIES_FIELD_INDEX)).getCursor();
        Map<String, String> policyParamters = new HashMap<>();
        String key;
        String value;
        while (cursor.next()) {
            ARecord field = (ARecord) cursor.get();
            key = ((AString) field.getValueByPos(MetadataRecordTypes.PROPERTIES_NAME_FIELD_INDEX)).getStringValue();
            value = ((AString) field.getValueByPos(MetadataRecordTypes.PROPERTIES_VALUE_FIELD_INDEX)).getStringValue();
            policyParamters.put(key, value);
        }

        feedPolicy = new FeedPolicyEntity(dataverseName, policyName, description, policyParamters);
        return feedPolicy;
    }

    @Override
    public ITupleReference getTupleFromMetadataEntity(FeedPolicyEntity feedPolicy)
            throws HyracksDataException, AlgebricksException {
        // write the key in the first three fields of the tuple
        ArrayBackedValueStorage itemValue = new ArrayBackedValueStorage();

        tupleBuilder.reset();
        aString.setValue(feedPolicy.getDataverseName());
        stringSerde.serialize(aString, tupleBuilder.getDataOutput());
        tupleBuilder.addFieldEndOffset();

        aString.setValue(feedPolicy.getPolicyName());
        stringSerde.serialize(aString, tupleBuilder.getDataOutput());
        tupleBuilder.addFieldEndOffset();

        recordBuilder.reset(MetadataRecordTypes.FEED_POLICY_RECORDTYPE);

        // write field 0
        fieldValue.reset();
        aString.setValue(feedPolicy.getDataverseName());
        stringSerde.serialize(aString, fieldValue.getDataOutput());
        recordBuilder.addField(MetadataRecordTypes.FEED_POLICY_ARECORD_DATAVERSE_NAME_FIELD_INDEX, fieldValue);

        // write field 1
        fieldValue.reset();
        aString.setValue(feedPolicy.getPolicyName());
        stringSerde.serialize(aString, fieldValue.getDataOutput());
        recordBuilder.addField(MetadataRecordTypes.FEED_POLICY_ARECORD_POLICY_NAME_FIELD_INDEX, fieldValue);

        // write field 2
        fieldValue.reset();
        aString.setValue(feedPolicy.getDescription());
        stringSerde.serialize(aString, fieldValue.getDataOutput());
        recordBuilder.addField(MetadataRecordTypes.FEED_POLICY_ARECORD_DESCRIPTION_FIELD_INDEX, fieldValue);

        // write field 3 (properties)
        Map<String, String> properties = feedPolicy.getProperties();
        UnorderedListBuilder listBuilder = new UnorderedListBuilder();
        listBuilder.reset((AUnorderedListType) MetadataRecordTypes.FEED_POLICY_RECORDTYPE
                .getFieldTypes()[MetadataRecordTypes.FEED_POLICY_ARECORD_PROPERTIES_FIELD_INDEX]);
        for (Map.Entry<String, String> property : properties.entrySet()) {
            String name = property.getKey();
            String value = property.getValue();
            itemValue.reset();
            writePropertyTypeRecord(name, value, itemValue.getDataOutput());
            listBuilder.addItem(itemValue);
        }
        fieldValue.reset();
        listBuilder.write(fieldValue.getDataOutput(), true);
        recordBuilder.addField(MetadataRecordTypes.FEED_POLICY_ARECORD_PROPERTIES_FIELD_INDEX, fieldValue);

        // write record
        recordBuilder.write(tupleBuilder.getDataOutput(), true);
        tupleBuilder.addFieldEndOffset();

        tuple.reset(tupleBuilder.getFieldEndOffsets(), tupleBuilder.getByteArray());
        return tuple;
    }

    public void writePropertyTypeRecord(String name, String value, DataOutput out) throws HyracksDataException {
        IARecordBuilder propertyRecordBuilder = new RecordBuilder();
        ArrayBackedValueStorage fieldValue = new ArrayBackedValueStorage();
        propertyRecordBuilder.reset(MetadataRecordTypes.POLICY_PARAMS_RECORDTYPE);
        AMutableString aString = new AMutableString("");
        ISerializerDeserializer<AString> stringSerde =
                SerializerDeserializerProvider.INSTANCE.getSerializerDeserializer(BuiltinType.ASTRING);

        // write field 0
        fieldValue.reset();
        aString.setValue(name);
        stringSerde.serialize(aString, fieldValue.getDataOutput());
        propertyRecordBuilder.addField(0, fieldValue);

        // write field 1
        fieldValue.reset();
        aString.setValue(value);
        stringSerde.serialize(aString, fieldValue.getDataOutput());
        propertyRecordBuilder.addField(1, fieldValue);

        propertyRecordBuilder.write(out, true);
    }
}
