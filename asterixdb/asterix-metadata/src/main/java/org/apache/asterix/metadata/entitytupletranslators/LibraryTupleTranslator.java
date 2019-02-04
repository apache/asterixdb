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
import org.apache.asterix.metadata.entities.Library;
import org.apache.asterix.om.base.ARecord;
import org.apache.asterix.om.base.AString;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.api.dataflow.value.ISerializerDeserializer;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;

/**
 * Translates a Library metadata entity to an ITupleReference and vice versa.
 */
public class LibraryTupleTranslator extends AbstractTupleTranslator<Library> {
    private static final long serialVersionUID = -7574173417999340281L;

    // Field indexes of serialized Library in a tuple.
    // First key field.
    public static final int LIBRARY_DATAVERSENAME_TUPLE_FIELD_INDEX = 0;
    // Second key field.
    public static final int LIBRARY_NAME_TUPLE_FIELD_INDEX = 1;

    // Payload field containing serialized Library.
    public static final int LIBRARY_PAYLOAD_TUPLE_FIELD_INDEX = 2;

    @SuppressWarnings("unchecked")
    private ISerializerDeserializer<ARecord> recordSerDes =
            SerializerDeserializerProvider.INSTANCE.getSerializerDeserializer(MetadataRecordTypes.LIBRARY_RECORDTYPE);

    protected LibraryTupleTranslator(boolean getTuple) {
        super(getTuple, MetadataPrimaryIndexes.LIBRARY_DATASET.getFieldCount());
    }

    @Override
    public Library getMetadataEntityFromTuple(ITupleReference frameTuple) throws HyracksDataException {
        byte[] serRecord = frameTuple.getFieldData(LIBRARY_PAYLOAD_TUPLE_FIELD_INDEX);
        int recordStartOffset = frameTuple.getFieldStart(LIBRARY_PAYLOAD_TUPLE_FIELD_INDEX);
        int recordLength = frameTuple.getFieldLength(LIBRARY_PAYLOAD_TUPLE_FIELD_INDEX);
        ByteArrayInputStream stream = new ByteArrayInputStream(serRecord, recordStartOffset, recordLength);
        DataInput in = new DataInputStream(stream);
        ARecord libraryRecord = recordSerDes.deserialize(in);
        return createLibraryFromARecord(libraryRecord);
    }

    private Library createLibraryFromARecord(ARecord libraryRecord) {
        String dataverseName =
                ((AString) libraryRecord.getValueByPos(MetadataRecordTypes.LIBRARY_ARECORD_DATAVERSENAME_FIELD_INDEX))
                        .getStringValue();
        String libraryName =
                ((AString) libraryRecord.getValueByPos(MetadataRecordTypes.LIBRARY_ARECORD_NAME_FIELD_INDEX))
                        .getStringValue();

        return new Library(dataverseName, libraryName);
    }

    @Override
    public ITupleReference getTupleFromMetadataEntity(Library library)
            throws HyracksDataException, AlgebricksException {
        // write the key in the first 2 fields of the tuple
        tupleBuilder.reset();
        aString.setValue(library.getDataverseName());
        stringSerde.serialize(aString, tupleBuilder.getDataOutput());
        tupleBuilder.addFieldEndOffset();
        aString.setValue(library.getName());
        stringSerde.serialize(aString, tupleBuilder.getDataOutput());
        tupleBuilder.addFieldEndOffset();

        // write the pay-load in the third field of the tuple

        recordBuilder.reset(MetadataRecordTypes.LIBRARY_RECORDTYPE);

        // write field 0
        fieldValue.reset();
        aString.setValue(library.getDataverseName());
        stringSerde.serialize(aString, fieldValue.getDataOutput());
        recordBuilder.addField(MetadataRecordTypes.LIBRARY_ARECORD_DATAVERSENAME_FIELD_INDEX, fieldValue);

        // write field 1
        fieldValue.reset();
        aString.setValue(library.getName());
        stringSerde.serialize(aString, fieldValue.getDataOutput());
        recordBuilder.addField(MetadataRecordTypes.LIBRARY_ARECORD_NAME_FIELD_INDEX, fieldValue);

        // write field 2
        fieldValue.reset();
        aString.setValue(Calendar.getInstance().getTime().toString());
        stringSerde.serialize(aString, fieldValue.getDataOutput());
        recordBuilder.addField(MetadataRecordTypes.LIBRARY_ARECORD_TIMESTAMP_FIELD_INDEX, fieldValue);

        // write record
        recordBuilder.write(tupleBuilder.getDataOutput(), true);
        tupleBuilder.addFieldEndOffset();
        tuple.reset(tupleBuilder.getFieldEndOffsets(), tupleBuilder.getByteArray());
        return tuple;
    }

}
