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
import java.util.Calendar;

import edu.uci.ics.asterix.common.exceptions.AsterixException;
import edu.uci.ics.asterix.formats.nontagged.AqlSerializerDeserializerProvider;
import edu.uci.ics.asterix.metadata.MetadataException;
import edu.uci.ics.asterix.metadata.bootstrap.MetadataPrimaryIndexes;
import edu.uci.ics.asterix.metadata.bootstrap.MetadataRecordTypes;
import edu.uci.ics.asterix.metadata.entities.Library;
import edu.uci.ics.asterix.om.base.ARecord;
import edu.uci.ics.asterix.om.base.AString;
import edu.uci.ics.hyracks.api.dataflow.value.ISerializerDeserializer;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.ITupleReference;

/**
 * Translates a Library metadata entity to an ITupleReference and vice versa.
 */
public class LibraryTupleTranslator extends AbstractTupleTranslator<Library> {
    // Field indexes of serialized Library in a tuple.
    // First key field.
    public static final int LIBRARY_DATAVERSENAME_TUPLE_FIELD_INDEX = 0;
    // Second key field.
    public static final int LIBRARY_NAME_TUPLE_FIELD_INDEX = 1;

    // Payload field containing serialized Library.
    public static final int LIBRARY_PAYLOAD_TUPLE_FIELD_INDEX = 2;

    @SuppressWarnings("unchecked")
    private ISerializerDeserializer<ARecord> recordSerDes = AqlSerializerDeserializerProvider.INSTANCE
            .getSerializerDeserializer(MetadataRecordTypes.LIBRARY_RECORDTYPE);

    public LibraryTupleTranslator(boolean getTuple) {
        super(getTuple, MetadataPrimaryIndexes.LIBRARY_DATASET.getFieldCount());
    }

    @Override
    public Library getMetadataEntityFromTuple(ITupleReference frameTuple) throws IOException {
        byte[] serRecord = frameTuple.getFieldData(LIBRARY_PAYLOAD_TUPLE_FIELD_INDEX);
        int recordStartOffset = frameTuple.getFieldStart(LIBRARY_PAYLOAD_TUPLE_FIELD_INDEX);
        int recordLength = frameTuple.getFieldLength(LIBRARY_PAYLOAD_TUPLE_FIELD_INDEX);
        ByteArrayInputStream stream = new ByteArrayInputStream(serRecord, recordStartOffset, recordLength);
        DataInput in = new DataInputStream(stream);
        ARecord libraryRecord = (ARecord) recordSerDes.deserialize(in);
        return createLibraryFromARecord(libraryRecord);
    }

    private Library createLibraryFromARecord(ARecord libraryRecord) {
        String dataverseName = ((AString) libraryRecord
                .getValueByPos(MetadataRecordTypes.LIBRARY_ARECORD_DATAVERSENAME_FIELD_INDEX)).getStringValue();
        String libraryName = ((AString) libraryRecord
                .getValueByPos(MetadataRecordTypes.LIBRARY_ARECORD_NAME_FIELD_INDEX)).getStringValue();

        return new Library(dataverseName, libraryName);
    }

    @Override
    public ITupleReference getTupleFromMetadataEntity(Library library) throws IOException, MetadataException {
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