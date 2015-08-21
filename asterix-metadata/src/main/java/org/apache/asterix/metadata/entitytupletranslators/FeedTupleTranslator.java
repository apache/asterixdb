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
import edu.uci.ics.asterix.builders.OrderedListBuilder;
import edu.uci.ics.asterix.builders.RecordBuilder;
import edu.uci.ics.asterix.common.exceptions.AsterixException;
import edu.uci.ics.asterix.common.functions.FunctionSignature;
import edu.uci.ics.asterix.formats.nontagged.AqlSerializerDeserializerProvider;
import edu.uci.ics.asterix.metadata.MetadataException;
import edu.uci.ics.asterix.metadata.bootstrap.MetadataPrimaryIndexes;
import edu.uci.ics.asterix.metadata.bootstrap.MetadataRecordTypes;
import edu.uci.ics.asterix.metadata.entities.Feed;
import edu.uci.ics.asterix.metadata.entities.Feed.FeedType;
import edu.uci.ics.asterix.metadata.entities.PrimaryFeed;
import edu.uci.ics.asterix.metadata.entities.SecondaryFeed;
import edu.uci.ics.asterix.om.base.AMutableString;
import edu.uci.ics.asterix.om.base.ANull;
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
 * Translates a Feed metadata entity to an ITupleReference and vice versa.
 */
public class FeedTupleTranslator extends AbstractTupleTranslator<Feed> {
    // Field indexes of serialized Feed in a tuple.
    // Key field.
    public static final int FEED_DATAVERSE_NAME_FIELD_INDEX = 0;

    public static final int FEED_NAME_FIELD_INDEX = 1;

    // Payload field containing serialized feed.
    public static final int FEED_PAYLOAD_TUPLE_FIELD_INDEX = 2;

    @SuppressWarnings("unchecked")
    private ISerializerDeserializer<ARecord> recordSerDes = AqlSerializerDeserializerProvider.INSTANCE
            .getSerializerDeserializer(MetadataRecordTypes.FEED_RECORDTYPE);

    public FeedTupleTranslator(boolean getTuple) {
        super(getTuple, MetadataPrimaryIndexes.FEED_DATASET.getFieldCount());
    }

    @Override
    public Feed getMetadataEntityFromTuple(ITupleReference frameTuple) throws IOException {
        byte[] serRecord = frameTuple.getFieldData(FEED_PAYLOAD_TUPLE_FIELD_INDEX);
        int recordStartOffset = frameTuple.getFieldStart(FEED_PAYLOAD_TUPLE_FIELD_INDEX);
        int recordLength = frameTuple.getFieldLength(FEED_PAYLOAD_TUPLE_FIELD_INDEX);
        ByteArrayInputStream stream = new ByteArrayInputStream(serRecord, recordStartOffset, recordLength);
        DataInput in = new DataInputStream(stream);
        ARecord feedRecord = (ARecord) recordSerDes.deserialize(in);
        return createFeedFromARecord(feedRecord);
    }

    private Feed createFeedFromARecord(ARecord feedRecord) {
        Feed feed = null;
        String dataverseName = ((AString) feedRecord
                .getValueByPos(MetadataRecordTypes.FEED_ARECORD_DATAVERSE_NAME_FIELD_INDEX)).getStringValue();
        String feedName = ((AString) feedRecord.getValueByPos(MetadataRecordTypes.FEED_ARECORD_FEED_NAME_FIELD_INDEX))
                .getStringValue();

        Object o = feedRecord.getValueByPos(MetadataRecordTypes.FEED_ARECORD_FUNCTION_FIELD_INDEX);
        FunctionSignature signature = null;
        if (!(o instanceof ANull)) {
            String functionName = ((AString) o).getStringValue();
            signature = new FunctionSignature(dataverseName, functionName, 1);
        }

        String feedType = ((AString) feedRecord.getValueByPos(MetadataRecordTypes.FEED_ARECORD_FEED_TYPE_FIELD_INDEX))
                .getStringValue();

        FeedType feedTypeEnum = FeedType.valueOf(feedType.toUpperCase());
        switch (feedTypeEnum) {
            case PRIMARY: {
                ARecord feedTypeDetailsRecord = (ARecord) feedRecord
                        .getValueByPos(MetadataRecordTypes.FEED_ARECORD_PRIMARY_TYPE_DETAILS_FIELD_INDEX);
                String adapterName = ((AString) feedTypeDetailsRecord
                        .getValueByPos(MetadataRecordTypes.FEED_ARECORD_PRIMARY_FIELD_DETAILS_ADAPTOR_NAME_FIELD_INDEX))
                        .getStringValue();

                IACursor cursor = ((AUnorderedList) feedTypeDetailsRecord
                        .getValueByPos(MetadataRecordTypes.FEED_ARECORD_PRIMARY_FIELD_DETAILS_ADAPTOR_CONFIGURATION_FIELD_INDEX))
                        .getCursor();
                String key;
                String value;
                Map<String, String> adaptorConfiguration = new HashMap<String, String>();
                while (cursor.next()) {
                    ARecord field = (ARecord) cursor.get();
                    key = ((AString) field.getValueByPos(MetadataRecordTypes.PROPERTIES_NAME_FIELD_INDEX))
                            .getStringValue();
                    value = ((AString) field.getValueByPos(MetadataRecordTypes.PROPERTIES_VALUE_FIELD_INDEX))
                            .getStringValue();
                    adaptorConfiguration.put(key, value);
                }
                feed = new PrimaryFeed(dataverseName, feedName, adapterName, adaptorConfiguration, signature);

            }
                break;
            case SECONDARY: {
                ARecord feedTypeDetailsRecord = (ARecord) feedRecord
                        .getValueByPos(MetadataRecordTypes.FEED_ARECORD_SECONDARY_TYPE_DETAILS_FIELD_INDEX);

                String sourceFeedName = ((AString) feedTypeDetailsRecord
                        .getValueByPos(MetadataRecordTypes.FEED_TYPE_SECONDARY_ARECORD_SOURCE_FEED_NAME_FIELD_INDEX))
                        .getStringValue();

                feed = new SecondaryFeed(dataverseName, feedName, sourceFeedName, signature);

            }
                break;
        }

        return feed;
    }

    @Override
    public ITupleReference getTupleFromMetadataEntity(Feed feed) throws IOException, MetadataException {
        // write the key in the first two fields of the tuple
        tupleBuilder.reset();
        aString.setValue(feed.getDataverseName());
        stringSerde.serialize(aString, tupleBuilder.getDataOutput());
        tupleBuilder.addFieldEndOffset();

        aString.setValue(feed.getFeedName());
        stringSerde.serialize(aString, tupleBuilder.getDataOutput());
        tupleBuilder.addFieldEndOffset();

        recordBuilder.reset(MetadataRecordTypes.FEED_RECORDTYPE);

        // write field 0
        fieldValue.reset();
        aString.setValue(feed.getDataverseName());
        stringSerde.serialize(aString, fieldValue.getDataOutput());
        recordBuilder.addField(MetadataRecordTypes.FEED_ARECORD_DATAVERSE_NAME_FIELD_INDEX, fieldValue);

        // write field 1
        fieldValue.reset();
        aString.setValue(feed.getFeedName());
        stringSerde.serialize(aString, fieldValue.getDataOutput());
        recordBuilder.addField(MetadataRecordTypes.FEED_ARECORD_FEED_NAME_FIELD_INDEX, fieldValue);

        // write field 2
        fieldValue.reset();
        if (feed.getAppliedFunction() != null) {
            aString.setValue(feed.getAppliedFunction().getName());
            stringSerde.serialize(aString, fieldValue.getDataOutput());
            recordBuilder.addField(MetadataRecordTypes.FEED_ARECORD_FUNCTION_FIELD_INDEX, fieldValue);
        }

        // write field 3
        fieldValue.reset();
        aString.setValue(feed.getFeedType().name().toUpperCase());
        stringSerde.serialize(aString, fieldValue.getDataOutput());
        recordBuilder.addField(MetadataRecordTypes.FEED_ARECORD_FEED_TYPE_FIELD_INDEX, fieldValue);

        // write field 4/5
        fieldValue.reset();
        writeFeedTypeDetailsRecordType(recordBuilder, feed, fieldValue);

        // write field 6
        fieldValue.reset();
        aString.setValue(Calendar.getInstance().getTime().toString());
        stringSerde.serialize(aString, fieldValue.getDataOutput());
        recordBuilder.addField(MetadataRecordTypes.FEED_ARECORD_TIMESTAMP_FIELD_INDEX, fieldValue);

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

    @SuppressWarnings("unchecked")
    private void writeFeedTypeDetailsRecordType(IARecordBuilder recordBuilder, Feed feed,
            ArrayBackedValueStorage fieldValue) throws HyracksDataException {

        switch (feed.getFeedType()) {
            case PRIMARY: {
                PrimaryFeed primaryFeed = (PrimaryFeed) feed;

                IARecordBuilder primaryDetailsRecordBuilder = new RecordBuilder();
                OrderedListBuilder listBuilder = new OrderedListBuilder();
                ArrayBackedValueStorage primaryRecordfieldValue = new ArrayBackedValueStorage();
                ArrayBackedValueStorage primaryRecordItemValue = new ArrayBackedValueStorage();
                primaryDetailsRecordBuilder.reset(MetadataRecordTypes.PRIMARY_FEED_DETAILS_RECORDTYPE);

                AMutableString aString = new AMutableString("");
                ISerializerDeserializer<AString> stringSerde = AqlSerializerDeserializerProvider.INSTANCE
                        .getSerializerDeserializer(BuiltinType.ASTRING);

                // write field 0
                fieldValue.reset();
                aString.setValue(primaryFeed.getAdaptorName());
                stringSerde.serialize(aString, primaryRecordfieldValue.getDataOutput());
                primaryDetailsRecordBuilder.addField(
                        MetadataRecordTypes.FEED_ARECORD_PRIMARY_FIELD_DETAILS_ADAPTOR_NAME_FIELD_INDEX,
                        primaryRecordfieldValue);

                // write field 1
                listBuilder
                        .reset((AUnorderedListType) MetadataRecordTypes.PRIMARY_FEED_DETAILS_RECORDTYPE.getFieldTypes()[MetadataRecordTypes.FEED_ARECORD_PRIMARY_FIELD_DETAILS_ADAPTOR_CONFIGURATION_FIELD_INDEX]);
                for (Map.Entry<String, String> property : primaryFeed.getAdaptorConfiguration().entrySet()) {
                    String name = property.getKey();
                    String value = property.getValue();
                    primaryRecordItemValue.reset();
                    writePropertyTypeRecord(name, value, primaryRecordItemValue.getDataOutput());
                    listBuilder.addItem(primaryRecordItemValue);
                }
                primaryRecordfieldValue.reset();
                listBuilder.write(primaryRecordfieldValue.getDataOutput(), true);
                primaryDetailsRecordBuilder.addField(
                        MetadataRecordTypes.FEED_ARECORD_PRIMARY_FIELD_DETAILS_ADAPTOR_CONFIGURATION_FIELD_INDEX,
                        primaryRecordfieldValue);

                try {
                    primaryDetailsRecordBuilder.write(fieldValue.getDataOutput(), true);
                } catch (IOException | AsterixException e) {
                    throw new HyracksDataException(e);
                }

                recordBuilder.addField(MetadataRecordTypes.FEED_ARECORD_PRIMARY_TYPE_DETAILS_FIELD_INDEX, fieldValue);
            }
                break;

            case SECONDARY:
                SecondaryFeed secondaryFeed = (SecondaryFeed) feed;

                IARecordBuilder secondaryDetailsRecordBuilder = new RecordBuilder();
                ArrayBackedValueStorage secondaryFieldValue = new ArrayBackedValueStorage();
                secondaryDetailsRecordBuilder.reset(MetadataRecordTypes.SECONDARY_FEED_DETAILS_RECORDTYPE);

                // write field 0
                fieldValue.reset();
                aString.setValue(secondaryFeed.getSourceFeedName());
                stringSerde.serialize(aString, secondaryFieldValue.getDataOutput());
                secondaryDetailsRecordBuilder.addField(
                        MetadataRecordTypes.FEED_ARECORD_SECONDARY_FIELD_DETAILS_SOURCE_FEED_NAME_FIELD_INDEX,
                        secondaryFieldValue);

                try {
                    secondaryDetailsRecordBuilder.write(fieldValue.getDataOutput(), true);
                } catch (IOException | AsterixException e) {
                    throw new HyracksDataException(e);
                }
                recordBuilder.addField(MetadataRecordTypes.FEED_ARECORD_SECONDARY_TYPE_DETAILS_FIELD_INDEX, fieldValue);
                break;
        }

    }

    @SuppressWarnings("unchecked")
    public void writePropertyTypeRecord(String name, String value, DataOutput out) throws HyracksDataException {
        IARecordBuilder propertyRecordBuilder = new RecordBuilder();
        ArrayBackedValueStorage fieldValue = new ArrayBackedValueStorage();
        propertyRecordBuilder.reset(MetadataRecordTypes.FEED_ADAPTER_CONFIGURATION_RECORDTYPE);
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
