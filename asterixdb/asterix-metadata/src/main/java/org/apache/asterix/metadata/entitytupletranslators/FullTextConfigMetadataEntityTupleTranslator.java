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

import static org.apache.asterix.metadata.bootstrap.MetadataRecordTypes.FULL_TEXT_ARECORD_CONFIG_NAME_FIELD_INDEX;
import static org.apache.asterix.metadata.bootstrap.MetadataRecordTypes.FULL_TEXT_ARECORD_CONFIG_TOKENIZER_FIELD_INDEX;
import static org.apache.asterix.metadata.bootstrap.MetadataRecordTypes.FULL_TEXT_ARECORD_DATAVERSE_NAME_FIELD_INDEX;
import static org.apache.asterix.metadata.bootstrap.MetadataRecordTypes.FULL_TEXT_ARECORD_FILTER_PIPELINE_FIELD_INDEX;

import java.util.List;

import org.apache.asterix.builders.OrderedListBuilder;
import org.apache.asterix.common.metadata.DataverseName;
import org.apache.asterix.formats.nontagged.SerializerDeserializerProvider;
import org.apache.asterix.metadata.bootstrap.MetadataPrimaryIndexes;
import org.apache.asterix.metadata.bootstrap.MetadataRecordTypes;
import org.apache.asterix.metadata.entities.FullTextConfigMetadataEntity;
import org.apache.asterix.om.base.AInt8;
import org.apache.asterix.om.base.AOrderedList;
import org.apache.asterix.om.base.ARecord;
import org.apache.asterix.om.base.AString;
import org.apache.asterix.om.base.IACursor;
import org.apache.asterix.om.types.AOrderedListType;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.asterix.runtime.fulltext.FullTextConfigDescriptor;
import org.apache.commons.lang3.EnumUtils;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.api.dataflow.value.ISerializerDeserializer;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.apache.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import org.apache.hyracks.dataflow.common.comm.io.ArrayTupleReference;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;
import org.apache.hyracks.storage.am.lsm.invertedindex.fulltext.TokenizerCategory;

import com.google.common.collect.ImmutableList;

public class FullTextConfigMetadataEntityTupleTranslator extends AbstractTupleTranslator<FullTextConfigMetadataEntity> {

    private static final int FULL_TEXT_CONFIG_PAYLOAD_TUPLE_FIELD_INDEX = 2;
    protected final ArrayTupleReference tuple;
    protected final ISerializerDeserializer<AInt8> int8Serde =
            SerializerDeserializerProvider.INSTANCE.getSerializerDeserializer(BuiltinType.AINT8);

    protected FullTextConfigMetadataEntityTupleTranslator(boolean getTuple) {
        super(getTuple, MetadataPrimaryIndexes.FULL_TEXT_CONFIG_DATASET, FULL_TEXT_CONFIG_PAYLOAD_TUPLE_FIELD_INDEX);
        if (getTuple) {
            tuple = new ArrayTupleReference();
        } else {
            tuple = null;
        }
    }

    @Override
    protected FullTextConfigMetadataEntity createMetadataEntityFromARecord(ARecord aRecord)
            throws HyracksDataException, AlgebricksException {
        DataverseName dataverseName = DataverseName.createFromCanonicalForm(
                ((AString) aRecord.getValueByPos(MetadataRecordTypes.FULL_TEXT_ARECORD_DATAVERSE_NAME_FIELD_INDEX))
                        .getStringValue());

        String name = ((AString) aRecord.getValueByPos(MetadataRecordTypes.FULL_TEXT_ARECORD_CONFIG_NAME_FIELD_INDEX))
                .getStringValue();

        TokenizerCategory tokenizerCategory =
                EnumUtils.getEnumIgnoreCase(TokenizerCategory.class,
                        ((AString) aRecord
                                .getValueByPos(MetadataRecordTypes.FULL_TEXT_ARECORD_CONFIG_TOKENIZER_FIELD_INDEX))
                                        .getStringValue());

        ImmutableList.Builder<String> filterNamesBuilder = ImmutableList.builder();
        IACursor filterNamesCursor = ((AOrderedList) (aRecord
                .getValueByPos(MetadataRecordTypes.FULL_TEXT_ARECORD_FILTER_PIPELINE_FIELD_INDEX))).getCursor();
        while (filterNamesCursor.next()) {
            filterNamesBuilder.add(((AString) filterNamesCursor.get()).getStringValue());
        }

        FullTextConfigDescriptor configDescriptor =
                new FullTextConfigDescriptor(dataverseName, name, tokenizerCategory, filterNamesBuilder.build());
        FullTextConfigMetadataEntity configMetadataEntity = new FullTextConfigMetadataEntity(configDescriptor);
        return configMetadataEntity;
    }

    private void writeIndex(String dataverseName, String configName, ArrayTupleBuilder tupleBuilder)
            throws HyracksDataException {
        aString.setValue(dataverseName);
        stringSerde.serialize(aString, tupleBuilder.getDataOutput());
        tupleBuilder.addFieldEndOffset();

        aString.setValue(configName);
        stringSerde.serialize(aString, tupleBuilder.getDataOutput());
        tupleBuilder.addFieldEndOffset();
    }

    @Override
    public ITupleReference getTupleFromMetadataEntity(FullTextConfigMetadataEntity configMetadataEntity)
            throws HyracksDataException {
        tupleBuilder.reset();

        FullTextConfigDescriptor configDescriptor = configMetadataEntity.getFullTextConfig();

        writeIndex(configDescriptor.getDataverseName().getCanonicalForm(), configDescriptor.getName(), tupleBuilder);

        recordBuilder.reset(MetadataRecordTypes.FULL_TEXT_CONFIG_RECORDTYPE);

        // write dataverse name
        fieldValue.reset();
        aString.setValue(configDescriptor.getDataverseName().getCanonicalForm());
        stringSerde.serialize(aString, fieldValue.getDataOutput());
        recordBuilder.addField(FULL_TEXT_ARECORD_DATAVERSE_NAME_FIELD_INDEX, fieldValue);

        // write name
        fieldValue.reset();
        aString.setValue(configDescriptor.getName());
        stringSerde.serialize(aString, fieldValue.getDataOutput());
        recordBuilder.addField(FULL_TEXT_ARECORD_CONFIG_NAME_FIELD_INDEX, fieldValue);

        // write tokenizer category
        fieldValue.reset();
        aString.setValue(configDescriptor.getTokenizerCategory().name());
        stringSerde.serialize(aString, fieldValue.getDataOutput());
        recordBuilder.addField(FULL_TEXT_ARECORD_CONFIG_TOKENIZER_FIELD_INDEX, fieldValue);

        // set filter pipeline
        List<String> filterNames = configDescriptor.getFilterNames();

        OrderedListBuilder listBuilder = new OrderedListBuilder();
        listBuilder.reset(new AOrderedListType(BuiltinType.ASTRING, null));
        ArrayBackedValueStorage itemValue = new ArrayBackedValueStorage();
        for (String s : filterNames) {
            itemValue.reset();
            aString.setValue(s);
            stringSerde.serialize(aString, itemValue.getDataOutput());
            listBuilder.addItem(itemValue);
        }

        fieldValue.reset();
        listBuilder.write(fieldValue.getDataOutput(), true);
        recordBuilder.addField(FULL_TEXT_ARECORD_FILTER_PIPELINE_FIELD_INDEX, fieldValue);

        recordBuilder.write(tupleBuilder.getDataOutput(), true);
        tupleBuilder.addFieldEndOffset();

        tuple.reset(tupleBuilder.getFieldEndOffsets(), tupleBuilder.getByteArray());
        return tuple;
    }
}
