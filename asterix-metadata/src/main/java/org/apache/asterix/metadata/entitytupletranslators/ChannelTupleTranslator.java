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

package org.apache.asterix.metadata.entitytupletranslators;

import java.io.ByteArrayInputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.IOException;

import org.apache.asterix.common.exceptions.AsterixException;
import org.apache.asterix.common.functions.FunctionSignature;
import org.apache.asterix.formats.nontagged.AqlSerializerDeserializerProvider;
import org.apache.asterix.metadata.MetadataException;
import org.apache.asterix.metadata.bootstrap.MetadataPrimaryIndexes;
import org.apache.asterix.metadata.bootstrap.MetadataRecordTypes;
import org.apache.asterix.metadata.entities.Channel;
import org.apache.asterix.om.base.ARecord;
import org.apache.asterix.om.base.AString;
import org.apache.hyracks.api.dataflow.value.ISerializerDeserializer;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;

/**
 * Translates a Channel metadata entity to an ITupleReference and vice versa.
 */
public class ChannelTupleTranslator extends AbstractTupleTranslator<Channel> {
    // Field indexes of serialized Feed in a tuple.
    // Key field.
    public static final int CHANNEL_DATAVERSE_NAME_FIELD_INDEX = 0;

    public static final int CHANNEL_NAME_FIELD_INDEX = 1;

    // Payload field containing serialized feed.
    public static final int CHANNEL_PAYLOAD_TUPLE_FIELD_INDEX = 2;

    @SuppressWarnings("unchecked")
    private ISerializerDeserializer<ARecord> recordSerDes = AqlSerializerDeserializerProvider.INSTANCE
            .getSerializerDeserializer(MetadataRecordTypes.CHANNEL_RECORDTYPE);

    @SuppressWarnings("unchecked")
    public ChannelTupleTranslator(boolean getTuple) {
        super(getTuple, MetadataPrimaryIndexes.CHANNEL_DATASET.getFieldCount());
    }

    @Override
    public Channel getMetadataEntityFromTuple(ITupleReference frameTuple) throws IOException {
        byte[] serRecord = frameTuple.getFieldData(CHANNEL_PAYLOAD_TUPLE_FIELD_INDEX);
        int recordStartOffset = frameTuple.getFieldStart(CHANNEL_PAYLOAD_TUPLE_FIELD_INDEX);
        int recordLength = frameTuple.getFieldLength(CHANNEL_PAYLOAD_TUPLE_FIELD_INDEX);
        ByteArrayInputStream stream = new ByteArrayInputStream(serRecord, recordStartOffset, recordLength);
        DataInput in = new DataInputStream(stream);
        ARecord channelRecord = recordSerDes.deserialize(in);
        return createChannelFromARecord(channelRecord);
    }

    private Channel createChannelFromARecord(ARecord channelRecord) {
        Channel channel = null;
        String dataverseName = ((AString) channelRecord
                .getValueByPos(MetadataRecordTypes.CHANNEL_ARECORD_DATAVERSE_NAME_FIELD_INDEX)).getStringValue();
        String channelName = ((AString) channelRecord
                .getValueByPos(MetadataRecordTypes.CHANNEL_ARECORD_CHANNEL_NAME_FIELD_INDEX)).getStringValue();
        String subscriptionsName = ((AString) channelRecord
                .getValueByPos(MetadataRecordTypes.CHANNEL_ARECORD_SUBSCRIPTIONS_NAME_FIELD_INDEX)).getStringValue();
        String resultsName = ((AString) channelRecord
                .getValueByPos(MetadataRecordTypes.CHANNEL_ARECORD_RESULTS_NAME_FIELD_INDEX)).getStringValue();
        String fName = ((AString) channelRecord.getValueByPos(MetadataRecordTypes.CHANNEL_ARECORD_FUNCTION_FIELD_INDEX))
                .getStringValue();
        String duration = ((AString) channelRecord
                .getValueByPos(MetadataRecordTypes.CHANNEL_ARECORD_DURATION_FIELD_INDEX)).getStringValue();

        FunctionSignature signature = null;

        String[] qnameComponents = fName.split("\\.");
        String functionDataverse;
        String functionName;
        if (qnameComponents.length == 2) {
            functionDataverse = qnameComponents[0];
            functionName = qnameComponents[1];
        } else {
            functionDataverse = dataverseName;
            functionName = qnameComponents[0];
        }

        String[] nameComponents = functionName.split("@");
        signature = new FunctionSignature(functionDataverse, nameComponents[0], Integer.parseInt(nameComponents[1]));

        channel = new Channel(dataverseName, channelName, subscriptionsName, resultsName, signature, duration);
        return channel;
    }

    @Override
    public ITupleReference getTupleFromMetadataEntity(Channel channel) throws IOException, MetadataException {
        // write the key in the first fields of the tuple

        tupleBuilder.reset();
        aString.setValue(channel.getChannelId().getDataverse());
        stringSerde.serialize(aString, tupleBuilder.getDataOutput());
        tupleBuilder.addFieldEndOffset();

        aString.setValue(channel.getChannelId().getName());
        stringSerde.serialize(aString, tupleBuilder.getDataOutput());
        tupleBuilder.addFieldEndOffset();

        recordBuilder.reset(MetadataRecordTypes.CHANNEL_RECORDTYPE);

        // write field 0
        fieldValue.reset();
        aString.setValue(channel.getChannelId().getDataverse());
        stringSerde.serialize(aString, fieldValue.getDataOutput());
        recordBuilder.addField(MetadataRecordTypes.CHANNEL_ARECORD_DATAVERSE_NAME_FIELD_INDEX, fieldValue);

        // write field 1
        fieldValue.reset();
        aString.setValue(channel.getChannelId().getName());
        stringSerde.serialize(aString, fieldValue.getDataOutput());
        recordBuilder.addField(MetadataRecordTypes.CHANNEL_ARECORD_CHANNEL_NAME_FIELD_INDEX, fieldValue);

        // write field 2
        fieldValue.reset();
        aString.setValue(channel.getSubscriptionsDataset());
        stringSerde.serialize(aString, fieldValue.getDataOutput());
        recordBuilder.addField(MetadataRecordTypes.CHANNEL_ARECORD_SUBSCRIPTIONS_NAME_FIELD_INDEX, fieldValue);

        // write field 3
        fieldValue.reset();
        aString.setValue(channel.getResultsDatasetName());
        stringSerde.serialize(aString, fieldValue.getDataOutput());
        recordBuilder.addField(MetadataRecordTypes.CHANNEL_ARECORD_RESULTS_NAME_FIELD_INDEX, fieldValue);

        // write field 4
        fieldValue.reset();
        aString.setValue(channel.getFunction().toString());
        stringSerde.serialize(aString, fieldValue.getDataOutput());
        recordBuilder.addField(MetadataRecordTypes.CHANNEL_ARECORD_FUNCTION_FIELD_INDEX, fieldValue);

        // write field 5
        fieldValue.reset();
        aString.setValue(channel.getDuration());
        stringSerde.serialize(aString, fieldValue.getDataOutput());
        recordBuilder.addField(MetadataRecordTypes.CHANNEL_ARECORD_DURATION_FIELD_INDEX, fieldValue);

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