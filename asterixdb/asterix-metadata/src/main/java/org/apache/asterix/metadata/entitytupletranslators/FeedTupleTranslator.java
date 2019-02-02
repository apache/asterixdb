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
import java.util.Calendar;
import java.util.HashMap;
import java.util.Map;

import org.apache.asterix.builders.IARecordBuilder;
import org.apache.asterix.builders.RecordBuilder;
import org.apache.asterix.builders.UnorderedListBuilder;
import org.apache.asterix.formats.nontagged.SerializerDeserializerProvider;
import org.apache.asterix.metadata.bootstrap.MetadataPrimaryIndexes;
import org.apache.asterix.metadata.bootstrap.MetadataRecordTypes;
import org.apache.asterix.metadata.entities.Feed;
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
 * Translates a Feed metadata entity to an ITupleReference and vice versa.
 */
public class FeedTupleTranslator extends AbstractTupleTranslator<Feed> {
    private static final long serialVersionUID = -5967081194106401387L;

    // Field indexes of serialized Feed in a tuple.
    // Key field.
    public static final int FEED_DATAVERSE_NAME_FIELD_INDEX = 0;

    public static final int FEED_NAME_FIELD_INDEX = 1;

    // Payload field containing serialized feed.
    public static final int FEED_PAYLOAD_TUPLE_FIELD_INDEX = 2;

    @SuppressWarnings("unchecked")
    private ISerializerDeserializer<ARecord> recordSerDes =
            SerializerDeserializerProvider.INSTANCE.getSerializerDeserializer(MetadataRecordTypes.FEED_RECORDTYPE);

    protected FeedTupleTranslator(boolean getTuple) {
        super(getTuple, MetadataPrimaryIndexes.FEED_DATASET.getFieldCount());
    }

    @Override
    public Feed getMetadataEntityFromTuple(ITupleReference frameTuple) throws HyracksDataException {
        byte[] serRecord = frameTuple.getFieldData(FEED_PAYLOAD_TUPLE_FIELD_INDEX);
        int recordStartOffset = frameTuple.getFieldStart(FEED_PAYLOAD_TUPLE_FIELD_INDEX);
        int recordLength = frameTuple.getFieldLength(FEED_PAYLOAD_TUPLE_FIELD_INDEX);
        ByteArrayInputStream stream = new ByteArrayInputStream(serRecord, recordStartOffset, recordLength);
        DataInput in = new DataInputStream(stream);
        ARecord feedRecord = recordSerDes.deserialize(in);
        return createFeedFromARecord(feedRecord);
    }

    private Feed createFeedFromARecord(ARecord feedRecord) {
        Feed feed;
        String dataverseName =
                ((AString) feedRecord.getValueByPos(MetadataRecordTypes.FEED_ARECORD_DATAVERSE_NAME_FIELD_INDEX))
                        .getStringValue();
        String feedName = ((AString) feedRecord.getValueByPos(MetadataRecordTypes.FEED_ARECORD_FEED_NAME_FIELD_INDEX))
                .getStringValue();

        AUnorderedList feedConfig =
                (AUnorderedList) feedRecord.getValueByPos(MetadataRecordTypes.FEED_ARECORD_ADAPTOR_CONFIG_INDEX);

        IACursor cursor = feedConfig.getCursor();

        // restore configurations
        String key;
        String value;
        Map<String, String> adaptorConfiguration = new HashMap<>();
        while (cursor.next()) {
            ARecord field = (ARecord) cursor.get();
            key = ((AString) field.getValueByPos(MetadataRecordTypes.PROPERTIES_NAME_FIELD_INDEX)).getStringValue();
            value = ((AString) field.getValueByPos(MetadataRecordTypes.PROPERTIES_VALUE_FIELD_INDEX)).getStringValue();
            adaptorConfiguration.put(key, value);
        }
        feed = new Feed(dataverseName, feedName, adaptorConfiguration);
        return feed;
    }

    @Override
    public ITupleReference getTupleFromMetadataEntity(Feed feed) throws HyracksDataException, AlgebricksException {
        // write the key in the first two fields of the tuple
        tupleBuilder.reset();
        aString.setValue(feed.getDataverseName());
        stringSerde.serialize(aString, tupleBuilder.getDataOutput());
        tupleBuilder.addFieldEndOffset();

        aString.setValue(feed.getFeedName());
        stringSerde.serialize(aString, tupleBuilder.getDataOutput());
        tupleBuilder.addFieldEndOffset();

        recordBuilder.reset(MetadataRecordTypes.FEED_RECORDTYPE);

        // write dataverse name
        fieldValue.reset();
        aString.setValue(feed.getDataverseName());
        stringSerde.serialize(aString, fieldValue.getDataOutput());
        recordBuilder.addField(MetadataRecordTypes.FEED_ARECORD_DATAVERSE_NAME_FIELD_INDEX, fieldValue);

        // write feed name
        fieldValue.reset();
        aString.setValue(feed.getFeedName());
        stringSerde.serialize(aString, fieldValue.getDataOutput());
        recordBuilder.addField(MetadataRecordTypes.FEED_ARECORD_FEED_NAME_FIELD_INDEX, fieldValue);

        // write adaptor configuration
        fieldValue.reset();
        writeFeedAdaptorField(recordBuilder, feed, fieldValue);

        // write timestamp
        fieldValue.reset();
        aString.setValue(Calendar.getInstance().getTime().toString());
        stringSerde.serialize(aString, fieldValue.getDataOutput());
        recordBuilder.addField(MetadataRecordTypes.FEED_ARECORD_TIMESTAMP_FIELD_INDEX, fieldValue);

        // write record
        recordBuilder.write(tupleBuilder.getDataOutput(), true);
        tupleBuilder.addFieldEndOffset();

        tuple.reset(tupleBuilder.getFieldEndOffsets(), tupleBuilder.getByteArray());
        return tuple;
    }

    private void writeFeedAdaptorField(IARecordBuilder recordBuilder, Feed feed,
            ArrayBackedValueStorage fieldValueBuffer) throws HyracksDataException {
        UnorderedListBuilder listBuilder = new UnorderedListBuilder();
        ArrayBackedValueStorage listEleBuffer = new ArrayBackedValueStorage();

        listBuilder.reset((AUnorderedListType) MetadataRecordTypes.FEED_RECORDTYPE
                .getFieldTypes()[MetadataRecordTypes.FEED_ARECORD_ADAPTOR_CONFIG_INDEX]);
        for (Map.Entry<String, String> property : feed.getConfiguration().entrySet()) {
            String name = property.getKey();
            String value = property.getValue();
            listEleBuffer.reset();
            writePropertyTypeRecord(name, value, listEleBuffer.getDataOutput());
            listBuilder.addItem(listEleBuffer);
        }
        listBuilder.write(fieldValueBuffer.getDataOutput(), true);
        recordBuilder.addField(MetadataRecordTypes.FEED_ARECORD_ADAPTOR_CONFIG_INDEX, fieldValueBuffer);
    }

    @SuppressWarnings("unchecked")
    public void writePropertyTypeRecord(String name, String value, DataOutput out) throws HyracksDataException {
        IARecordBuilder propertyRecordBuilder = new RecordBuilder();
        ArrayBackedValueStorage fieldValue = new ArrayBackedValueStorage();
        propertyRecordBuilder.reset(MetadataRecordTypes.DATASOURCE_ADAPTER_PROPERTIES_RECORDTYPE);
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
