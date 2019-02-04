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
import java.util.Calendar;

import org.apache.asterix.formats.nontagged.SerializerDeserializerProvider;
import org.apache.asterix.metadata.bootstrap.MetadataPrimaryIndexes;
import org.apache.asterix.metadata.bootstrap.MetadataRecordTypes;
import org.apache.asterix.metadata.entities.Dataverse;
import org.apache.asterix.om.base.AInt32;
import org.apache.asterix.om.base.AMutableInt32;
import org.apache.asterix.om.base.ARecord;
import org.apache.asterix.om.base.AString;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.api.dataflow.value.ISerializerDeserializer;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;

/**
 * Translates a Dataverse metadata entity to an ITupleReference and vice versa.
 */
public class DataverseTupleTranslator extends AbstractTupleTranslator<Dataverse> {
    private static final long serialVersionUID = -3196752600543191613L;

    // Field indexes of serialized Dataverse in a tuple.
    // Key field.
    public static final int DATAVERSE_DATAVERSENAME_TUPLE_FIELD_INDEX = 0;
    // Payload field containing serialized Dataverse.
    public static final int DATAVERSE_PAYLOAD_TUPLE_FIELD_INDEX = 1;

    private transient AMutableInt32 aInt32;
    protected ISerializerDeserializer<AInt32> aInt32Serde;

    @SuppressWarnings("unchecked")
    private ISerializerDeserializer<ARecord> recordSerDes =
            SerializerDeserializerProvider.INSTANCE.getSerializerDeserializer(MetadataRecordTypes.DATAVERSE_RECORDTYPE);

    @SuppressWarnings("unchecked")
    protected DataverseTupleTranslator(boolean getTuple) {
        super(getTuple, MetadataPrimaryIndexes.DATAVERSE_DATASET.getFieldCount());
        aInt32 = new AMutableInt32(-1);
        aInt32Serde = SerializerDeserializerProvider.INSTANCE.getSerializerDeserializer(BuiltinType.AINT32);
    }

    @Override
    public Dataverse getMetadataEntityFromTuple(ITupleReference frameTuple) throws HyracksDataException {
        byte[] serRecord = frameTuple.getFieldData(DATAVERSE_PAYLOAD_TUPLE_FIELD_INDEX);
        int recordStartOffset = frameTuple.getFieldStart(DATAVERSE_PAYLOAD_TUPLE_FIELD_INDEX);
        int recordLength = frameTuple.getFieldLength(DATAVERSE_PAYLOAD_TUPLE_FIELD_INDEX);
        ByteArrayInputStream stream = new ByteArrayInputStream(serRecord, recordStartOffset, recordLength);
        DataInput in = new DataInputStream(stream);
        ARecord dataverseRecord = recordSerDes.deserialize(in);
        return new Dataverse(((AString) dataverseRecord.getValueByPos(0)).getStringValue(),
                ((AString) dataverseRecord.getValueByPos(1)).getStringValue(),
                ((AInt32) dataverseRecord.getValueByPos(3)).getIntegerValue());
    }

    @Override
    public ITupleReference getTupleFromMetadataEntity(Dataverse instance)
            throws HyracksDataException, AlgebricksException {
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

        // write field 3
        fieldValue.reset();
        aInt32.setValue(instance.getPendingOp());
        aInt32Serde.serialize(aInt32, fieldValue.getDataOutput());
        recordBuilder.addField(MetadataRecordTypes.DATAVERSE_ARECORD_PENDINGOP_FIELD_INDEX, fieldValue);

        recordBuilder.write(tupleBuilder.getDataOutput(), true);
        tupleBuilder.addFieldEndOffset();

        tuple.reset(tupleBuilder.getFieldEndOffsets(), tupleBuilder.getByteArray());
        return tuple;
    }
}
