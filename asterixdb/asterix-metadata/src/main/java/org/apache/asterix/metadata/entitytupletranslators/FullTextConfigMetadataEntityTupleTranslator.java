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

import java.util.List;

import org.apache.asterix.builders.OrderedListBuilder;
import org.apache.asterix.common.metadata.DataverseName;
import org.apache.asterix.common.metadata.MetadataUtil;
import org.apache.asterix.metadata.bootstrap.FullTextConfigEntity;
import org.apache.asterix.metadata.entities.FullTextConfigMetadataEntity;
import org.apache.asterix.om.base.AOrderedList;
import org.apache.asterix.om.base.ARecord;
import org.apache.asterix.om.base.AString;
import org.apache.asterix.om.base.IACursor;
import org.apache.asterix.om.types.AOrderedListType;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.asterix.runtime.fulltext.FullTextConfigDescriptor;
import org.apache.commons.lang3.EnumUtils;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.apache.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import org.apache.hyracks.dataflow.common.comm.io.ArrayTupleReference;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;
import org.apache.hyracks.storage.am.lsm.invertedindex.fulltext.TokenizerCategory;

import com.google.common.collect.ImmutableList;

public class FullTextConfigMetadataEntityTupleTranslator extends AbstractTupleTranslator<FullTextConfigMetadataEntity> {

    private final FullTextConfigEntity fullTextConfigEntity;
    protected final ArrayTupleReference tuple;

    protected FullTextConfigMetadataEntityTupleTranslator(boolean getTuple, FullTextConfigEntity fullTextConfigEntity) {
        super(getTuple, fullTextConfigEntity.getIndex(), fullTextConfigEntity.payloadPosition());
        this.fullTextConfigEntity = fullTextConfigEntity;
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
                ((AString) aRecord.getValueByPos(fullTextConfigEntity.dataverseNameIndex())).getStringValue());
        int databaseNameIndex = fullTextConfigEntity.databaseNameIndex();
        String databaseName;
        if (databaseNameIndex >= 0) {
            databaseName = ((AString) aRecord.getValueByPos(databaseNameIndex)).getStringValue();
        } else {
            databaseName = MetadataUtil.databaseFor(dataverseName);
        }
        String name = ((AString) aRecord.getValueByPos(fullTextConfigEntity.configNameIndex())).getStringValue();

        TokenizerCategory tokenizerCategory = EnumUtils.getEnumIgnoreCase(TokenizerCategory.class,
                ((AString) aRecord.getValueByPos(fullTextConfigEntity.tokenizerIndex())).getStringValue());

        ImmutableList.Builder<String> filterNamesBuilder = ImmutableList.builder();
        IACursor filterNamesCursor =
                ((AOrderedList) (aRecord.getValueByPos(fullTextConfigEntity.filterPipelineIndex()))).getCursor();
        while (filterNamesCursor.next()) {
            filterNamesBuilder.add(((AString) filterNamesCursor.get()).getStringValue());
        }

        FullTextConfigDescriptor configDescriptor = new FullTextConfigDescriptor(databaseName, dataverseName, name,
                tokenizerCategory, filterNamesBuilder.build());
        return new FullTextConfigMetadataEntity(configDescriptor);
    }

    private void writeIndex(String databaseName, String dataverseName, String configName,
            ArrayTupleBuilder tupleBuilder) throws HyracksDataException {
        if (fullTextConfigEntity.databaseNameIndex() >= 0) {
            aString.setValue(databaseName);
            stringSerde.serialize(aString, tupleBuilder.getDataOutput());
            tupleBuilder.addFieldEndOffset();
        }
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

        writeIndex(configDescriptor.getDatabaseName(), configDescriptor.getDataverseName().getCanonicalForm(),
                configDescriptor.getName(), tupleBuilder);

        recordBuilder.reset(fullTextConfigEntity.getRecordType());

        if (fullTextConfigEntity.databaseNameIndex() >= 0) {
            fieldValue.reset();
            aString.setValue(configDescriptor.getDatabaseName());
            stringSerde.serialize(aString, fieldValue.getDataOutput());
            recordBuilder.addField(fullTextConfigEntity.databaseNameIndex(), fieldValue);
        }
        // write dataverse name
        fieldValue.reset();
        aString.setValue(configDescriptor.getDataverseName().getCanonicalForm());
        stringSerde.serialize(aString, fieldValue.getDataOutput());
        recordBuilder.addField(fullTextConfigEntity.dataverseNameIndex(), fieldValue);

        // write name
        fieldValue.reset();
        aString.setValue(configDescriptor.getName());
        stringSerde.serialize(aString, fieldValue.getDataOutput());
        recordBuilder.addField(fullTextConfigEntity.configNameIndex(), fieldValue);

        // write tokenizer category
        fieldValue.reset();
        aString.setValue(configDescriptor.getTokenizerCategory().name());
        stringSerde.serialize(aString, fieldValue.getDataOutput());
        recordBuilder.addField(fullTextConfigEntity.tokenizerIndex(), fieldValue);

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
        recordBuilder.addField(fullTextConfigEntity.filterPipelineIndex(), fieldValue);

        recordBuilder.write(tupleBuilder.getDataOutput(), true);
        tupleBuilder.addFieldEndOffset();

        tuple.reset(tupleBuilder.getFieldEndOffsets(), tupleBuilder.getByteArray());
        return tuple;
    }
}
