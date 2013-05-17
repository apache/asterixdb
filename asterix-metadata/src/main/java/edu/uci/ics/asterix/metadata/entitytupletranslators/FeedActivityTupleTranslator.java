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
import java.io.IOException;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;

import edu.uci.ics.asterix.builders.UnorderedListBuilder;
import edu.uci.ics.asterix.common.exceptions.AsterixException;
import edu.uci.ics.asterix.formats.nontagged.AqlSerializerDeserializerProvider;
import edu.uci.ics.asterix.metadata.MetadataException;
import edu.uci.ics.asterix.metadata.bootstrap.MetadataPrimaryIndexes;
import edu.uci.ics.asterix.metadata.bootstrap.MetadataRecordTypes;
import edu.uci.ics.asterix.metadata.entities.FeedActivity;
import edu.uci.ics.asterix.om.base.AInt32;
import edu.uci.ics.asterix.om.base.AMutableInt32;
import edu.uci.ics.asterix.om.base.ARecord;
import edu.uci.ics.asterix.om.base.AString;
import edu.uci.ics.asterix.om.base.AUnorderedList;
import edu.uci.ics.asterix.om.base.IACursor;
import edu.uci.ics.asterix.om.types.AUnorderedListType;
import edu.uci.ics.asterix.om.types.BuiltinType;
import edu.uci.ics.hyracks.api.dataflow.value.ISerializerDeserializer;
import edu.uci.ics.hyracks.data.std.util.ArrayBackedValueStorage;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.ITupleReference;

/**
 * Translates a Dataset metadata entity to an ITupleReference and vice versa.
 */
public class FeedActivityTupleTranslator extends AbstractTupleTranslator<FeedActivity> {
    // Field indexes of serialized Dataset in a tuple.
    // First key field.
    public static final int FEED_ACTIVITY_DATAVERSENAME_TUPLE_FIELD_INDEX = 0;
    // Second key field.
    public static final int FEED_ACTIVITY_DATASET_DATASETNAME_TUPLE_FIELD_INDEX = 1;
    // Payload field containing serialized Dataset.
    public static final int FEED_ACTIVITY_PAYLOAD_TUPLE_FIELD_INDEX = 2;

    @SuppressWarnings("unchecked")
    private ISerializerDeserializer<ARecord> recordSerDes = AqlSerializerDeserializerProvider.INSTANCE
            .getSerializerDeserializer(MetadataRecordTypes.FEED_ACTIVITY_RECORDTYPE);
    private AMutableInt32 aInt32;
    protected ISerializerDeserializer<AInt32> aInt32Serde;

    @SuppressWarnings("unchecked")
    public FeedActivityTupleTranslator(boolean getTuple) {
        super(getTuple, MetadataPrimaryIndexes.DATASET_DATASET.getFieldCount());
        aInt32 = new AMutableInt32(-1);
        aInt32Serde = AqlSerializerDeserializerProvider.INSTANCE.getSerializerDeserializer(BuiltinType.AINT32);
    }

    @Override
    public FeedActivity getMetadataEntytiFromTuple(ITupleReference frameTuple) throws IOException {
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
        String datasetName = ((AString) feedActivityRecord
                .getValueByPos(MetadataRecordTypes.FEED_ACTIVITY_ARECORD_DATASET_NAME_FIELD_INDEX)).getStringValue();
        String status = ((AString) feedActivityRecord
                .getValueByPos(MetadataRecordTypes.FEED_ACTIVITY_ARECORD_STATUS_FIELD_INDEX)).getStringValue();

        List<String> ingestNodes = new ArrayList<String>();
        IACursor cursor = ((AUnorderedList) feedActivityRecord
                .getValueByPos(MetadataRecordTypes.FEED_ACTIVITY_ARECORD_INGEST_NODES_FIELD_INDEX)).getCursor();
        while (cursor.next()) {
            ingestNodes.add(((AString) cursor.get()).getStringValue());
        }

        List<String> computeNodes = new ArrayList<String>();
        cursor = ((AUnorderedList) feedActivityRecord
                .getValueByPos(MetadataRecordTypes.FEED_ACTIVITY_ARECORD_COMPUTE_NODES_FIELD_INDEX)).getCursor();
        while (cursor.next()) {
            computeNodes.add(((AString) cursor.get()).getStringValue());
        }

        return new FeedActivity(dataverseName, datasetName, status, ingestNodes, computeNodes);
    }

    @Override
    public ITupleReference getTupleFromMetadataEntity(FeedActivity feedActivity) throws IOException, MetadataException {
        // write the key in the first 2 fields of the tuple
        ArrayBackedValueStorage itemValue = new ArrayBackedValueStorage();
        tupleBuilder.reset();
        aString.setValue(feedActivity.getDataverseName());
        stringSerde.serialize(aString, tupleBuilder.getDataOutput());
        tupleBuilder.addFieldEndOffset();
        aString.setValue(feedActivity.getDatasetName());
        stringSerde.serialize(aString, tupleBuilder.getDataOutput());
        tupleBuilder.addFieldEndOffset();

        // write the pay-load in the third field of the tuple

        recordBuilder.reset(MetadataRecordTypes.FEED_ACTIVITY_RECORDTYPE);

        // write field 0
        fieldValue.reset();
        aString.setValue(feedActivity.getDataverseName());
        stringSerde.serialize(aString, fieldValue.getDataOutput());
        recordBuilder.addField(MetadataRecordTypes.FEED_ACTIVITY_ARECORD_DATAVERSE_NAME_FIELD_INDEX, fieldValue);

        // write field 1
        fieldValue.reset();
        aString.setValue(feedActivity.getDatasetName());
        stringSerde.serialize(aString, fieldValue.getDataOutput());
        recordBuilder.addField(MetadataRecordTypes.FEED_ACTIVITY_ARECORD_DATASET_NAME_FIELD_INDEX, fieldValue);

        // write field 2
        fieldValue.reset();
        aString.setValue(feedActivity.getFeedState().name());
        stringSerde.serialize(aString, fieldValue.getDataOutput());
        recordBuilder.addField(MetadataRecordTypes.FEED_ACTIVITY_ARECORD_STATUS_FIELD_INDEX, fieldValue);

        // write field 3
        UnorderedListBuilder listBuilder = new UnorderedListBuilder();
        listBuilder
                .reset((AUnorderedListType) MetadataRecordTypes.FEED_ACTIVITY_RECORDTYPE.getFieldTypes()[MetadataRecordTypes.FEED_ACTIVITY_ARECORD_INGEST_NODES_FIELD_INDEX]);
        for (String field : feedActivity.getIngestNodes()) {
            itemValue.reset();
            aString.setValue(field);
            stringSerde.serialize(aString, itemValue.getDataOutput());
            listBuilder.addItem(itemValue);
        }
        fieldValue.reset();
        listBuilder.write(fieldValue.getDataOutput(), true);
        recordBuilder.addField(MetadataRecordTypes.FEED_ACTIVITY_ARECORD_INGEST_NODES_FIELD_INDEX, fieldValue);

        // write field 4
        listBuilder
                .reset((AUnorderedListType) MetadataRecordTypes.FEED_ACTIVITY_RECORDTYPE.getFieldTypes()[MetadataRecordTypes.FEED_ACTIVITY_ARECORD_COMPUTE_NODES_FIELD_INDEX]);
        for (String field : feedActivity.getIngestNodes()) {
            itemValue.reset();
            aString.setValue(field);
            stringSerde.serialize(aString, itemValue.getDataOutput());
            listBuilder.addItem(itemValue);
        }
        fieldValue.reset();
        listBuilder.write(fieldValue.getDataOutput(), true);
        recordBuilder.addField(MetadataRecordTypes.FEED_ACTIVITY_ARECORD_COMPUTE_NODES_FIELD_INDEX, fieldValue);

        // write field 5
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

}