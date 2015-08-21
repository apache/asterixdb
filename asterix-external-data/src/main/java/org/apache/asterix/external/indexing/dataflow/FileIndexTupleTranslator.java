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
package edu.uci.ics.asterix.external.indexing.dataflow;

import java.io.IOException;

import edu.uci.ics.asterix.builders.RecordBuilder;
import edu.uci.ics.asterix.common.exceptions.AsterixException;
import edu.uci.ics.asterix.formats.nontagged.AqlSerializerDeserializerProvider;
import edu.uci.ics.asterix.metadata.entities.ExternalFile;
import edu.uci.ics.asterix.metadata.external.FilesIndexDescription;
import edu.uci.ics.asterix.om.base.ADateTime;
import edu.uci.ics.asterix.om.base.AInt64;
import edu.uci.ics.asterix.om.base.AMutableDateTime;
import edu.uci.ics.asterix.om.base.AMutableInt32;
import edu.uci.ics.asterix.om.base.AMutableInt64;
import edu.uci.ics.asterix.om.base.AMutableString;
import edu.uci.ics.asterix.om.base.AString;
import edu.uci.ics.asterix.om.types.BuiltinType;
import edu.uci.ics.hyracks.api.dataflow.value.ISerializerDeserializer;
import edu.uci.ics.hyracks.data.std.util.ArrayBackedValueStorage;
import edu.uci.ics.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import edu.uci.ics.hyracks.dataflow.common.comm.io.ArrayTupleReference;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.ITupleReference;

@SuppressWarnings("unchecked")
public class FileIndexTupleTranslator {
    private ArrayTupleBuilder tupleBuilder = new ArrayTupleBuilder(FilesIndexDescription.FILE_INDEX_RECORD_DESCRIPTOR.getFieldCount());
    private RecordBuilder recordBuilder = new RecordBuilder();
    private ArrayBackedValueStorage fieldValue = new ArrayBackedValueStorage();
    private AMutableInt32 aInt32 = new AMutableInt32(0);
    private AMutableInt64 aInt64 = new AMutableInt64(0);
    private AMutableString aString = new AMutableString(null);
    private AMutableDateTime aDateTime = new AMutableDateTime(0);
    private ISerializerDeserializer<AString> stringSerde = AqlSerializerDeserializerProvider.INSTANCE.getSerializerDeserializer(BuiltinType.ASTRING);
    private ISerializerDeserializer<ADateTime> dateTimeSerde = AqlSerializerDeserializerProvider.INSTANCE.getSerializerDeserializer(BuiltinType.ADATETIME);
    private ISerializerDeserializer<AInt64> longSerde = AqlSerializerDeserializerProvider.INSTANCE.getSerializerDeserializer(BuiltinType.AINT64);
    private ArrayTupleReference tuple = new ArrayTupleReference();
    
    public ITupleReference getTupleFromFile(ExternalFile file) throws IOException, AsterixException{
        tupleBuilder.reset();
        //File Number
        aInt32.setValue(file.getFileNumber());
        FilesIndexDescription.FILE_INDEX_RECORD_DESCRIPTOR.getFields()[0].serialize(aInt32, tupleBuilder.getDataOutput());
        tupleBuilder.addFieldEndOffset();
        
        //File Record
        recordBuilder.reset(FilesIndexDescription.EXTERNAL_FILE_RECORD_TYPE);
        // write field 0 (File Name)
        fieldValue.reset();
        aString.setValue(file.getFileName());
        stringSerde.serialize(aString, fieldValue.getDataOutput());
        recordBuilder.addField(0, fieldValue);
        
        //write field 1 (File Size)
        fieldValue.reset();
        aInt64.setValue(file.getSize());
        longSerde.serialize(aInt64, fieldValue.getDataOutput());
        recordBuilder.addField(1, fieldValue);
        
        //write field 2 (File Mod Date)
        fieldValue.reset();
        aDateTime.setValue(file.getLastModefiedTime().getTime());
        dateTimeSerde.serialize(aDateTime, fieldValue.getDataOutput());
        recordBuilder.addField(2, fieldValue);
        
        //write the record
        recordBuilder.write(tupleBuilder.getDataOutput(), true);
        tupleBuilder.addFieldEndOffset();
        tuple.reset(tupleBuilder.getFieldEndOffsets(), tupleBuilder.getByteArray());
        return tuple;
    }
}
