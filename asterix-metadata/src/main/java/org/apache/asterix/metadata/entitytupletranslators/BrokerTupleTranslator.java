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
import org.apache.asterix.formats.nontagged.AqlSerializerDeserializerProvider;
import org.apache.asterix.metadata.MetadataException;
import org.apache.asterix.metadata.bootstrap.MetadataPrimaryIndexes;
import org.apache.asterix.metadata.bootstrap.MetadataRecordTypes;
import org.apache.asterix.metadata.entities.Broker;
import org.apache.asterix.om.base.ARecord;
import org.apache.asterix.om.base.AString;
import org.apache.hyracks.api.dataflow.value.ISerializerDeserializer;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;

/**
 * Translates a Channel metadata entity to an ITupleReference and vice versa.
 */
public class BrokerTupleTranslator extends AbstractTupleTranslator<Broker> {
    // Field indexes of serialized Broker in a tuple.
    // Key field.
    public static final int BROKER_NAME_FIELD_INDEX = 0;

    // Payload field containing serialized broker.
    public static final int BROKER_PAYLOAD_TUPLE_FIELD_INDEX = 1;

    @SuppressWarnings("unchecked")
    private ISerializerDeserializer<ARecord> recordSerDes = AqlSerializerDeserializerProvider.INSTANCE
            .getSerializerDeserializer(MetadataRecordTypes.BROKER_RECORDTYPE);

    @SuppressWarnings("unchecked")
    public BrokerTupleTranslator(boolean getTuple) {
        super(getTuple, MetadataPrimaryIndexes.BROKER_DATASET.getFieldCount());
    }

    @Override
    public Broker getMetadataEntityFromTuple(ITupleReference frameTuple) throws IOException {
        byte[] serRecord = frameTuple.getFieldData(BROKER_PAYLOAD_TUPLE_FIELD_INDEX);
        int recordStartOffset = frameTuple.getFieldStart(BROKER_PAYLOAD_TUPLE_FIELD_INDEX);
        int recordLength = frameTuple.getFieldLength(BROKER_PAYLOAD_TUPLE_FIELD_INDEX);
        ByteArrayInputStream stream = new ByteArrayInputStream(serRecord, recordStartOffset, recordLength);
        DataInput in = new DataInputStream(stream);
        ARecord channelRecord = recordSerDes.deserialize(in);
        return createBrokerFromARecord(channelRecord);
    }

    private Broker createBrokerFromARecord(ARecord brokerRecord) {
        Broker broker = null;
        String brokerName = ((AString) brokerRecord.getValueByPos(MetadataRecordTypes.BROKER_NAME_FIELD_INDEX))
                .getStringValue();
        String endPointName = ((AString) brokerRecord.getValueByPos(MetadataRecordTypes.BROKER_ENDPOINT_FIELD_INDEX))
                .getStringValue();

        broker = new Broker(brokerName, endPointName);
        return broker;
    }

    @Override
    public ITupleReference getTupleFromMetadataEntity(Broker broker) throws IOException, MetadataException {
        // write the key in the first fields of the tuple

        tupleBuilder.reset();
        aString.setValue(broker.getBrokerName());
        stringSerde.serialize(aString, tupleBuilder.getDataOutput());
        tupleBuilder.addFieldEndOffset();

        recordBuilder.reset(MetadataRecordTypes.BROKER_RECORDTYPE);

        // write field 0
        fieldValue.reset();
        aString.setValue(broker.getBrokerName());
        stringSerde.serialize(aString, fieldValue.getDataOutput());
        recordBuilder.addField(MetadataRecordTypes.BROKER_NAME_FIELD_INDEX, fieldValue);

        // write field 1
        fieldValue.reset();
        aString.setValue(broker.getEndPointName());
        stringSerde.serialize(aString, fieldValue.getDataOutput());
        recordBuilder.addField(MetadataRecordTypes.BROKER_ENDPOINT_FIELD_INDEX, fieldValue);

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