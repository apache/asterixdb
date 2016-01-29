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
import java.util.Map;

import org.apache.asterix.builders.RecordBuilder;
import org.apache.asterix.external.api.IDataParser;
import org.apache.asterix.external.api.IExternalDataSourceFactory.DataSourceType;
import org.apache.asterix.external.api.IRawRecord;
import org.apache.asterix.external.api.IRecordDataParser;
import org.apache.asterix.external.input.record.RecordWithMetadata;
import org.apache.asterix.formats.nontagged.AqlSerializerDeserializerProvider;
import org.apache.asterix.om.base.AMutableString;
import org.apache.asterix.om.base.AString;
import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.hyracks.api.dataflow.value.ISerializerDeserializer;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;

public class RecordWithMetadataParser<T> implements IRecordDataParser<RecordWithMetadata<T>> {

    private final Class<? extends RecordWithMetadata<T>> clazz;
    private final int[] metaIndexes;
    private final int valueIndex;
    private ARecordType recordType;
    private IRecordDataParser<T> valueParser;
    private RecordBuilder recBuilder;
    private ArrayBackedValueStorage[] nameBuffers;
    private int numberOfFields;
    private ArrayBackedValueStorage valueBuffer = new ArrayBackedValueStorage();
    @SuppressWarnings("unchecked")
    private ISerializerDeserializer<AString> stringSerde = AqlSerializerDeserializerProvider.INSTANCE
            .getSerializerDeserializer(BuiltinType.ASTRING);

    public RecordWithMetadataParser(Class<? extends RecordWithMetadata<T>> clazz, int[] metaIndexes,
            IRecordDataParser<T> valueParser, int valueIndex) {
        this.clazz = clazz;
        this.metaIndexes = metaIndexes;
        this.valueParser = valueParser;
        this.valueIndex = valueIndex;
    }

    @Override
    public DataSourceType getDataSourceType() {
        return DataSourceType.RECORDS;
    }

    @Override
    public void configure(Map<String, String> configuration, ARecordType recordType)
            throws HyracksDataException, IOException {
        this.recordType = recordType;
        this.numberOfFields = recordType.getFieldNames().length;
        recBuilder = new RecordBuilder();
        recBuilder.reset(recordType);
        recBuilder.init();
        nameBuffers = new ArrayBackedValueStorage[numberOfFields];
        AMutableString str = new AMutableString(null);
        for (int i = 0; i < numberOfFields; i++) {
            String name = recordType.getFieldNames()[i];
            nameBuffers[i] = new ArrayBackedValueStorage();
            str.setValue(name);
            IDataParser.toBytes(str, nameBuffers[i], stringSerde);
        }
    }

    @Override
    public Class<? extends RecordWithMetadata<T>> getRecordClass() {
        return clazz;
    }

    @Override
    public void parse(IRawRecord<? extends RecordWithMetadata<T>> record, DataOutput out) throws Exception {
        recBuilder.reset(recordType);
        valueBuffer.reset();
        recBuilder.init();
        RecordWithMetadata<T> rwm = record.get();
        for (int i = 0; i < numberOfFields; i++) {
            if (i == valueIndex) {
                valueParser.parse(rwm.getRecord(), valueBuffer.getDataOutput());
                recBuilder.addField(i, valueBuffer);
            } else {
                recBuilder.addField(i, rwm.getMetadata(metaIndexes[i]));
            }
        }
        recBuilder.write(out, true);
    }
}
