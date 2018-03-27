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
package org.apache.asterix.external.parser;

import java.io.DataOutput;
import java.io.IOException;

import org.apache.asterix.builders.RecordBuilder;
import org.apache.asterix.external.api.IDataParser;
import org.apache.asterix.external.api.IRawRecord;
import org.apache.asterix.external.api.IRecordConverter;
import org.apache.asterix.external.api.IRecordDataParser;
import org.apache.asterix.external.api.IRecordWithMetadataParser;
import org.apache.asterix.external.input.record.RecordWithMetadataAndPK;
import org.apache.asterix.formats.nontagged.SerializerDeserializerProvider;
import org.apache.asterix.om.base.AMutableString;
import org.apache.asterix.om.base.AString;
import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.hyracks.api.dataflow.value.ISerializerDeserializer;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.apache.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;

public class RecordWithMetadataParser<T, O> implements IRecordWithMetadataParser<T> {

    private final IRecordConverter<T, RecordWithMetadataAndPK<O>> converter;
    private RecordWithMetadataAndPK<O> rwm;
    private final IRecordDataParser<O> recordParser;
    private final ARecordType metaType;
    private final RecordBuilder metaBuilder;
    private final ArrayBackedValueStorage[] metaFieldsNamesBuffers;
    private final int numberOfMetaFields;
    @SuppressWarnings("unchecked")
    private final ISerializerDeserializer<AString> stringSerde =
            SerializerDeserializerProvider.INSTANCE.getSerializerDeserializer(BuiltinType.ASTRING);

    public RecordWithMetadataParser(ARecordType metaType, IRecordDataParser<O> valueParser,
            IRecordConverter<T, RecordWithMetadataAndPK<O>> converter) throws HyracksDataException {
        this.recordParser = valueParser;
        this.converter = converter;
        this.metaType = metaType;
        this.numberOfMetaFields = metaType.getFieldNames().length;
        metaBuilder = new RecordBuilder();
        metaBuilder.reset(metaType);
        metaBuilder.init();
        metaFieldsNamesBuffers = new ArrayBackedValueStorage[numberOfMetaFields];
        AMutableString str = new AMutableString(null);
        for (int i = 0; i < numberOfMetaFields; i++) {
            String name = metaType.getFieldNames()[i];
            metaFieldsNamesBuffers[i] = new ArrayBackedValueStorage();
            str.setValue(name);
            IDataParser.toBytes(str, metaFieldsNamesBuffers[i], stringSerde);
        }
    }

    @Override
    public void parse(IRawRecord<? extends T> record, DataOutput out) throws HyracksDataException {
        try {
            rwm = converter.convert(record);
            if (rwm.getRecord().size() == 0) {
                // null record
                out.writeByte(ATypeTag.SERIALIZED_MISSING_TYPE_TAG);
            } else {
                recordParser.parse(rwm.getRecord(), out);
            }
        } catch (IOException e) {
            throw HyracksDataException.create(e);
        }
    }

    @Override
    public void parseMeta(DataOutput out) throws HyracksDataException {
        try {
            if (rwm.getRecord().size() == 0) {
                out.writeByte(ATypeTag.SERIALIZED_MISSING_TYPE_TAG);
            } else {
                metaBuilder.reset(metaType);
                metaBuilder.init();
                // parse meta-Fields
                for (int i = 0; i < numberOfMetaFields; i++) {
                    metaBuilder.addField(i, rwm.getMetadata(i));
                }
                // write the meta record
                metaBuilder.write(out, true);
            }
        } catch (IOException e) {
            throw HyracksDataException.create(e);
        }
    }

    @Override
    public void appendLastParsedPrimaryKeyToTuple(ArrayTupleBuilder tb) throws HyracksDataException {
        rwm.appendPrimaryKeyToTuple(tb);
    }
}
