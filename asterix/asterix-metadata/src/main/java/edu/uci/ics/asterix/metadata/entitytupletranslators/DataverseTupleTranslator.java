/*
 * Copyright 2009-2010 by The Regents of the University of California
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
import java.util.Calendar;

import edu.uci.ics.asterix.formats.nontagged.AqlSerializerDeserializerProvider;
import edu.uci.ics.asterix.metadata.bootstrap.MetadataPrimaryIndexes;
import edu.uci.ics.asterix.metadata.bootstrap.MetadataRecordTypes;
import edu.uci.ics.asterix.metadata.entities.Dataverse;
import edu.uci.ics.asterix.om.base.ARecord;
import edu.uci.ics.asterix.om.base.AString;
import edu.uci.ics.hyracks.api.dataflow.value.ISerializerDeserializer;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.ITupleReference;

/**
 * Translates a Dataverse metadata entity to an ITupleReference and vice versa.
 */
public class DataverseTupleTranslator extends AbstractTupleTranslator<Dataverse> {
    // Field indexes of serialized Dataverse in a tuple.
    // Key field.
    public static final int DATAVERSE_DATAVERSENAME_TUPLE_FIELD_INDEX = 0;
    // Payload field containing serialized Dataverse.
    public static final int DATAVERSE_PAYLOAD_TUPLE_FIELD_INDEX = 1;

    @SuppressWarnings("unchecked")
    private ISerializerDeserializer<ARecord> recordSerDes = AqlSerializerDeserializerProvider.INSTANCE
            .getSerializerDeserializer(MetadataRecordTypes.DATAVERSE_RECORDTYPE);

    public DataverseTupleTranslator(boolean getTuple) {
        super(getTuple, MetadataPrimaryIndexes.DATAVERSE_DATASET.getFieldCount());
    }

    @Override
    public Dataverse getMetadataEntytiFromTuple(ITupleReference frameTuple) throws IOException {
        byte[] serRecord = frameTuple.getFieldData(DATAVERSE_PAYLOAD_TUPLE_FIELD_INDEX);
        int recordStartOffset = frameTuple.getFieldStart(DATAVERSE_PAYLOAD_TUPLE_FIELD_INDEX);
        int recordLength = frameTuple.getFieldLength(DATAVERSE_PAYLOAD_TUPLE_FIELD_INDEX);
        ByteArrayInputStream stream = new ByteArrayInputStream(serRecord, recordStartOffset, recordLength);
        DataInput in = new DataInputStream(stream);
        ARecord dataverseRecord = recordSerDes.deserialize(in);
        return new Dataverse(((AString) dataverseRecord.getValueByPos(0)).getStringValue(),
                ((AString) dataverseRecord.getValueByPos(1)).getStringValue());
    }

    @Override
    public ITupleReference getTupleFromMetadataEntity(Dataverse instance) throws IOException {
        // write the key in the first field of the tuple
        tupleBuilder.reset();
        aString.setValue(instance.getDataverseName());
        stringSerde.serialize(aString, tupleBuilder.getDataOutput());
        tupleBuilder.addFieldEndOffset();

        // write the payload in the second field of the tuple
        recordBuilder.reset(MetadataRecordTypes.DATAVERSE_RECORDTYPE);
        // write field 0
        fieldValue.reset();
        aString.setValue(instance.getDataverseName());
        stringSerde.serialize(aString, fieldValue.getDataOutput());
        recordBuilder.addField(MetadataRecordTypes.DATAVERSE_ARECORD_NAME_FIELD_INDEX, fieldValue);

        // write field 1
        fieldValue.reset();
        aString.setValue(instance.getDataFormat());
        stringSerde.serialize(aString, fieldValue.getDataOutput());
        recordBuilder.addField(MetadataRecordTypes.DATAVERSE_ARECORD_FORMAT_FIELD_INDEX, fieldValue);

        // write field 2
        fieldValue.reset();
        aString.setValue(Calendar.getInstance().getTime().toString());
        stringSerde.serialize(aString, fieldValue.getDataOutput());
        recordBuilder.addField(MetadataRecordTypes.DATAVERSE_ARECORD_TIMESTAMP_FIELD_INDEX, fieldValue);

        recordBuilder.write(tupleBuilder.getDataOutput(), true);
        tupleBuilder.addFieldEndOffset();

        tuple.reset(tupleBuilder.getFieldEndOffsets(), tupleBuilder.getByteArray());
        return tuple;
    }
}
