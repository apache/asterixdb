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
package org.apache.asterix.external.indexing;

import java.io.IOException;

import org.apache.asterix.builders.RecordBuilder;
import org.apache.asterix.common.exceptions.AsterixException;
import org.apache.asterix.formats.nontagged.AqlSerializerDeserializerProvider;
import org.apache.asterix.om.base.AMutableDateTime;
import org.apache.asterix.om.base.AMutableInt32;
import org.apache.asterix.om.base.AMutableInt64;
import org.apache.asterix.om.base.AMutableString;
import org.apache.asterix.om.base.IAObject;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.hyracks.api.dataflow.value.ISerializerDeserializer;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.apache.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import org.apache.hyracks.dataflow.common.comm.io.ArrayTupleReference;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;

@SuppressWarnings("unchecked")
public class FileIndexTupleTranslator {
    private final FilesIndexDescription filesIndexDescription = new FilesIndexDescription();
    private ArrayTupleBuilder tupleBuilder = new ArrayTupleBuilder(
            filesIndexDescription.FILE_INDEX_RECORD_DESCRIPTOR.getFieldCount());
    private RecordBuilder recordBuilder = new RecordBuilder();
    private ArrayBackedValueStorage fieldValue = new ArrayBackedValueStorage();
    private AMutableInt32 aInt32 = new AMutableInt32(0);
    private AMutableInt64 aInt64 = new AMutableInt64(0);
    private AMutableString aString = new AMutableString(null);
    private AMutableDateTime aDateTime = new AMutableDateTime(0);
    private ISerializerDeserializer<IAObject> stringSerde = AqlSerializerDeserializerProvider.INSTANCE
            .getSerializerDeserializer(BuiltinType.ASTRING);
    private ISerializerDeserializer<IAObject> dateTimeSerde = AqlSerializerDeserializerProvider.INSTANCE
            .getSerializerDeserializer(BuiltinType.ADATETIME);
    private ISerializerDeserializer<IAObject> longSerde = AqlSerializerDeserializerProvider.INSTANCE
            .getSerializerDeserializer(BuiltinType.AINT64);
    private ArrayTupleReference tuple = new ArrayTupleReference();

    public ITupleReference getTupleFromFile(ExternalFile file) throws IOException, AsterixException {
        tupleBuilder.reset();
        //File Number
        aInt32.setValue(file.getFileNumber());
        filesIndexDescription.FILE_INDEX_RECORD_DESCRIPTOR.getFields()[0].serialize(aInt32,
                tupleBuilder.getDataOutput());
        tupleBuilder.addFieldEndOffset();

        //File Record
        recordBuilder.reset(filesIndexDescription.EXTERNAL_FILE_RECORD_TYPE);
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
