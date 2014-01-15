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
import java.util.Calendar;
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
import edu.uci.ics.asterix.metadata.entities.FeedActivity;
import edu.uci.ics.asterix.metadata.entities.FeedActivity.FeedActivityType;
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
public class FeedActivityTupleTranslator extends AbstractTupleTranslator<FeedActivity> {
    // Field indexes of serialized FeedActivity in a tuple.
    // Key field.
    public static final int FEED_ACTIVITY_ACTIVITY_DATAVERSE_NAME_FIELD_INDEX = 0;

    public static final int FEED_ACTIVITY_ACTIVITY_FEED_NAME_FIELD_INDEX = 1;

    public static final int FEED_ACTIVITY_ACTIVITY_DATASET_NAME_FIELD_INDEX = 2;

    public static final int FEED_ACTIVITY_ACTIVITY_ID_FIELD_INDEX = 3;

    // Payload field containing serialized FeedActivity.
    public static final int FEED_ACTIVITY_PAYLOAD_TUPLE_FIELD_INDEX = 4;

    @SuppressWarnings("unchecked")
    private ISerializerDeserializer<ARecord> recordSerDes = AqlSerializerDeserializerProvider.INSTANCE
            .getSerializerDeserializer(MetadataRecordTypes.FEED_ACTIVITY_RECORDTYPE);
    private AMutableInt32 aInt32;
    protected ISerializerDeserializer<AInt32> aInt32Serde;

    @SuppressWarnings("unchecked")
    public FeedActivityTupleTranslator(boolean getTuple) {
        super(getTuple, MetadataPrimaryIndexes.FEED_ACTIVITY_DATASET.getFieldCount());
        aInt32 = new AMutableInt32(-1);
        aInt32Serde = AqlSerializerDeserializerProvider.INSTANCE.getSerializerDeserializer(BuiltinType.AINT32);
    }

    @Override
    public FeedActivity getMetadataEntityFromTuple(ITupleReference frameTuple) throws IOException {
        byte[] serRecord = frameTuple.getFieldData(FEED_ACTIVITY_PAYLOAD_TUPLE_FIELD_INDEX);
        int recordStartOffset = frameTuple.getFieldStart(FEED_ACTIVITY_PAYLOAD_TUPLE_FIELD_INDEX);
        int recordLength = frameTuple.getFieldLength(FEED_ACTIVITY_PAYLOAD_TUPLE_FIELD_INDEX);
        ByteArrayInputStream stream = new ByteArrayInputStream(serRecord, recordStartOffset, recordLength);
        DataInput in = new DataInputStream(stream);
        ARecord feedActivityRecord = (ARecord) recordSerDes.deserialize(in);
        return createFeedActivityFromARecord(feedActivityRecord);
    }

    private FeedActivity createFeedActivityFromARecord(ARecord feedActivityRecord) {

        String dataverseName = ((AString) feedActivityRecord
                .getValueByPos(MetadataRecordTypes.FEED_ACTIVITY_ARECORD_DATAVERSE_NAME_FIELD_INDEX)).getStringValue();
        String feedName = ((AString) feedActivityRecord
                .getValueByPos(MetadataRecordTypes.FEED_ACTIVITY_ARECORD_FEED_NAME_FIELD_INDEX)).getStringValue();
        String datasetName = ((AString) feedActivityRecord
                .getValueByPos(MetadataRecordTypes.FEED_ACTIVITY_ARECORD_DATASET_NAME_FIELD_INDEX)).getStringValue();
        int activityId = ((AInt32) feedActivityRecord
                .getValueByPos(MetadataRecordTypes.FEED_ACTIVITY_ARECORD_ACTIVITY_ID_FIELD_INDEX)).getIntegerValue();
        String feedActivityType = ((AString) feedActivityRecord
                .getValueByPos(MetadataRecordTypes.FEED_ACTIVITY_ARECORD_ACTIVITY_TYPE_FIELD_INDEX)).getStringValue();

        IACursor cursor = ((AUnorderedList) feedActivityRecord
                .getValueByPos(MetadataRecordTypes.FEED_ACTIVITY_ARECORD_DETAILS_FIELD_INDEX)).getCursor();
        Map<String, String> activityDetails = new HashMap<String, String>();
        String key;
        String value;
        while (cursor.next()) {
            ARecord field = (ARecord) cursor.get();
            key = ((AString) field.getValueByPos(MetadataRecordTypes.PROPERTIES_NAME_FIELD_INDEX)).getStringValue();
            value = ((AString) field.getValueByPos(MetadataRecordTypes.PROPERTIES_VALUE_FIELD_INDEX)).getStringValue();
            activityDetails.put(key, value);
        }

        String feedActivityTimestamp = ((AString) feedActivityRecord
                .getValueByPos(MetadataRecordTypes.FEED_ACTIVITY_ARECORD_LAST_UPDATE_TIMESTAMP_FIELD_INDEX))
                .getStringValue();

        FeedActivity fa = new FeedActivity(dataverseName, feedName, datasetName,
                FeedActivityType.valueOf(feedActivityType), activityDetails);
        fa.setLastUpdatedTimestamp(feedActivityTimestamp);
        fa.setActivityId(activityId);
        return fa;
    }

    @Override
    public ITupleReference getTupleFromMetadataEntity(FeedActivity feedActivity) throws IOException, MetadataException {
        // write the key in the first three fields of the tuple
        ArrayBackedValueStorage itemValue = new ArrayBackedValueStorage();

        tupleBuilder.reset();
        aString.setValue(feedActivity.getDataverseName());
        stringSerde.serialize(aString, tupleBuilder.getDataOutput());
        tupleBuilder.addFieldEndOffset();

        aString.setValue(feedActivity.getFeedName());
        stringSerde.serialize(aString, tupleBuilder.getDataOutput());
        tupleBuilder.addFieldEndOffset();

        aString.setValue(feedActivity.getDatasetName());
        stringSerde.serialize(aString, tupleBuilder.getDataOutput());
        tupleBuilder.addFieldEndOffset();

        aInt32.setValue(feedActivity.getActivityId());
        int32Serde.serialize(aInt32, tupleBuilder.getDataOutput());
        tupleBuilder.addFieldEndOffset();
        // write the pay-load in the 2nd field of the tuple

        recordBuilder.reset(MetadataRecordTypes.FEED_ACTIVITY_RECORDTYPE);

        // write field 0
        fieldValue.reset();
        aString.setValue(feedActivity.getDataverseName());
        stringSerde.serialize(aString, fieldValue.getDataOutput());
        recordBuilder.addField(MetadataRecordTypes.FEED_ACTIVITY_ARECORD_DATAVERSE_NAME_FIELD_INDEX, fieldValue);

        // write field 1
        fieldValue.reset();
        aString.setValue(feedActivity.getFeedName());
        stringSerde.serialize(aString, fieldValue.getDataOutput());
        recordBuilder.addField(MetadataRecordTypes.FEED_ACTIVITY_ARECORD_FEED_NAME_FIELD_INDEX, fieldValue);

        // write field 2
        fieldValue.reset();
        aString.setValue(feedActivity.getDatasetName());
        stringSerde.serialize(aString, fieldValue.getDataOutput());
        recordBuilder.addField(MetadataRecordTypes.FEED_ACTIVITY_ARECORD_DATASET_NAME_FIELD_INDEX, fieldValue);

        // write field 3
        fieldValue.reset();
        aInt32.setValue(feedActivity.getActivityId());
        int32Serde.serialize(aInt32, fieldValue.getDataOutput());
        recordBuilder.addField(MetadataRecordTypes.FEED_ACTIVITY_ARECORD_ACTIVITY_ID_FIELD_INDEX, fieldValue);

        // write field 4
        fieldValue.reset();
        aString.setValue(feedActivity.getFeedActivityType().name());
        stringSerde.serialize(aString, fieldValue.getDataOutput());
        recordBuilder.addField(MetadataRecordTypes.FEED_ACTIVITY_ARECORD_ACTIVITY_TYPE_FIELD_INDEX, fieldValue);

        // write field 5
        Map<String, String> properties = feedActivity.getFeedActivityDetails();
        UnorderedListBuilder listBuilder = new UnorderedListBuilder();
        listBuilder
                .reset((AUnorderedListType) MetadataRecordTypes.FEED_ACTIVITY_RECORDTYPE.getFieldTypes()[MetadataRecordTypes.FEED_ACTIVITY_ARECORD_DETAILS_FIELD_INDEX]);
        for (Map.Entry<String, String> property : properties.entrySet()) {
            String name = property.getKey();
            String value = property.getValue();
            itemValue.reset();
            writePropertyTypeRecord(name, value, itemValue.getDataOutput());
            listBuilder.addItem(itemValue);
        }
        fieldValue.reset();
        listBuilder.write(fieldValue.getDataOutput(), true);
        recordBuilder.addField(MetadataRecordTypes.FEED_ACTIVITY_ARECORD_DETAILS_FIELD_INDEX, fieldValue);

        // write field 6
        fieldValue.reset();
        aString.setValue(Calendar.getInstance().getTime().toString());
        stringSerde.serialize(aString, fieldValue.getDataOutput());
        recordBuilder.addField(MetadataRecordTypes.FEED_ACTIVITY_ARECORD_LAST_UPDATE_TIMESTAMP_FIELD_INDEX, fieldValue);

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
        propertyRecordBuilder.reset(MetadataRecordTypes.FEED_ACTIVITY_DETAILS_RECORDTYPE);
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