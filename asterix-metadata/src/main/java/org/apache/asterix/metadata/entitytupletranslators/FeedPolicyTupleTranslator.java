/*
 * Copyright 2009-2013 by The Regents of the University of California
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * you may obtain a copy of the License from
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package edu.uci.ics.asterix.metadata.entitytupletranslators;

import java.io.ByteArrayInputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import edu.uci.ics.asterix.builders.IARecordBuilder;
import edu.uci.ics.asterix.builders.RecordBuilder;
import edu.uci.ics.asterix.builders.UnorderedListBuilder;
import edu.uci.ics.asterix.common.exceptions.AsterixException;
import edu.uci.ics.asterix.formats.nontagged.AqlSerializerDeserializerProvider;
import edu.uci.ics.asterix.metadata.MetadataException;
import edu.uci.ics.asterix.metadata.bootstrap.MetadataPrimaryIndexes;
import edu.uci.ics.asterix.metadata.bootstrap.MetadataRecordTypes;
import edu.uci.ics.asterix.metadata.entities.FeedPolicy;
import edu.uci.ics.asterix.om.base.AInt32;
import edu.uci.ics.asterix.om.base.AMutableInt32;
import edu.uci.ics.asterix.om.base.AMutableString;
import edu.uci.ics.asterix.om.base.ARecord;
import edu.uci.ics.asterix.om.base.AString;
import edu.uci.ics.asterix.om.base.AUnorderedList;
import edu.uci.ics.asterix.om.base.IACursor;
import edu.uci.ics.asterix.om.types.AUnorderedListType;
import edu.uci.ics.asterix.om.types.BuiltinType;
import edu.uci.ics.hyracks.api.dataflow.value.ISerializerDeserializer;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.data.std.util.ArrayBackedValueStorage;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.ITupleReference;

/**
 * Translates a Dataset metadata entity to an ITupleReference and vice versa.
 */
public class FeedPolicyTupleTranslator extends AbstractTupleTranslator<FeedPolicy> {
    // Field indexes of serialized FeedPolicy in a tuple.
    // Key field.
    public static final int FEED_POLICY_DATAVERSE_NAME_FIELD_INDEX = 0;

    public static final int FEED_POLICY_POLICY_NAME_FIELD_INDEX = 1;

    // Payload field containing serialized feedPolicy.
    public static final int FEED_POLICY_PAYLOAD_TUPLE_FIELD_INDEX = 2;

    @SuppressWarnings("unchecked")
    private ISerializerDeserializer<ARecord> recordSerDes = AqlSerializerDeserializerProvider.INSTANCE
            .getSerializerDeserializer(MetadataRecordTypes.FEED_POLICY_RECORDTYPE);
    private AMutableInt32 aInt32;
    protected ISerializerDeserializer<AInt32> aInt32Serde;

    @SuppressWarnings("unchecked")
    public FeedPolicyTupleTranslator(boolean getTuple) {
        super(getTuple, MetadataPrimaryIndexes.FEED_POLICY_DATASET.getFieldCount());
        aInt32 = new AMutableInt32(-1);
        aInt32Serde = AqlSerializerDeserializerProvider.INSTANCE.getSerializerDeserializer(BuiltinType.AINT32);
    }

    @Override
    public FeedPolicy getMetadataEntityFromTuple(ITupleReference frameTuple) throws IOException {
        byte[] serRecord = frameTuple.getFieldData(FEED_POLICY_PAYLOAD_TUPLE_FIELD_INDEX);
        int recordStartOffset = frameTuple.getFieldStart(FEED_POLICY_PAYLOAD_TUPLE_FIELD_INDEX);
        int recordLength = frameTuple.getFieldLength(FEED_POLICY_PAYLOAD_TUPLE_FIELD_INDEX);
        ByteArrayInputStream stream = new ByteArrayInputStream(serRecord, recordStartOffset, recordLength);
        DataInput in = new DataInputStream(stream);
        ARecord feedPolicyRecord = (ARecord) recordSerDes.deserialize(in);
        return createFeedPolicyFromARecord(feedPolicyRecord);
    }

    private FeedPolicy createFeedPolicyFromARecord(ARecord feedPolicyRecord) {
        FeedPolicy feedPolicy = null;
        String dataverseName = ((AString) feedPolicyRecord
                .getValueByPos(MetadataRecordTypes.FEED_POLICY_ARECORD_DATAVERSE_NAME_FIELD_INDEX)).getStringValue();
        String policyName = ((AString) feedPolicyRecord
                .getValueByPos(MetadataRecordTypes.FEED_POLICY_ARECORD_POLICY_NAME_FIELD_INDEX)).getStringValue();

        String description = ((AString) feedPolicyRecord
                .getValueByPos(MetadataRecordTypes.FEED_POLICY_ARECORD_DESCRIPTION_FIELD_INDEX)).getStringValue();

        IACursor cursor = ((AUnorderedList) feedPolicyRecord
                .getValueByPos(MetadataRecordTypes.FEED_POLICY_ARECORD_PROPERTIES_FIELD_INDEX)).getCursor();
        Map<String, String> policyParamters = new HashMap<String, String>();
        String key;
        String value;
        while (cursor.next()) {
            ARecord field = (ARecord) cursor.get();
            key = ((AString) field.getValueByPos(MetadataRecordTypes.PROPERTIES_NAME_FIELD_INDEX)).getStringValue();
            value = ((AString) field.getValueByPos(MetadataRecordTypes.PROPERTIES_VALUE_FIELD_INDEX)).getStringValue();
            policyParamters.put(key, value);
        }

        feedPolicy = new FeedPolicy(dataverseName, policyName, description, policyParamters);
        return feedPolicy;
    }

    @Override
    public ITupleReference getTupleFromMetadataEntity(FeedPolicy feedPolicy) throws IOException, MetadataException {
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
        recordBuilder.addField(MetadataRecordTypes.FEED_POLICY_ARECORD_POLICY_NAME_FIELD_INDEX, fieldValue);

        // write field 3 (properties)
        Map<String, String> properties = feedPolicy.getProperties();
        UnorderedListBuilder listBuilder = new UnorderedListBuilder();
        listBuilder
                .reset((AUnorderedListType) MetadataRecordTypes.FEED_POLICY_RECORDTYPE.getFieldTypes()[MetadataRecordTypes.FEED_POLICY_ARECORD_PROPERTIES_FIELD_INDEX]);
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
        try {
            recordBuilder.write(tupleBuilder.getDataOutput(), true);
        } catch (AsterixException e) {
            throw new MetadataException(e);
        }
        tupleBuilder.addFieldEndOffset();

        tuple.reset(tupleBuilder.getFieldEndOffsets(), tupleBuilder.getByteArray());
        return tuple;
    }

    public void writePropertyTypeRecord(String name, String value, DataOutput out) throws HyracksDataException {
        IARecordBuilder propertyRecordBuilder = new RecordBuilder();
        ArrayBackedValueStorage fieldValue = new ArrayBackedValueStorage();
        propertyRecordBuilder.reset(MetadataRecordTypes.POLICY_PARAMS_RECORDTYPE);
        AMutableString aString = new AMutableString("");
        ISerializerDeserializer<AString> stringSerde = AqlSerializerDeserializerProvider.INSTANCE
                .getSerializerDeserializer(BuiltinType.ASTRING);

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

        try {
            propertyRecordBuilder.write(out, true);
        } catch (IOException | AsterixException e) {
            throw new HyracksDataException(e);
        }
    }
}
