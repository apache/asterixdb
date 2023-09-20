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

import static org.apache.asterix.metadata.bootstrap.MetadataRecordTypes.FIELD_NAME_FULL_TEXT_STOPWORD_LIST;

import java.util.List;

import org.apache.asterix.builders.OrderedListBuilder;
import org.apache.asterix.common.exceptions.AsterixException;
import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.common.metadata.DataverseName;
import org.apache.asterix.metadata.bootstrap.FullTextFilterEntity;
import org.apache.asterix.metadata.entities.FullTextFilterMetadataEntity;
import org.apache.asterix.metadata.utils.MetadataUtil;
import org.apache.asterix.om.base.AOrderedList;
import org.apache.asterix.om.base.ARecord;
import org.apache.asterix.om.base.AString;
import org.apache.asterix.om.base.IACursor;
import org.apache.asterix.om.types.AOrderedListType;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.asterix.runtime.fulltext.AbstractFullTextFilterDescriptor;
import org.apache.asterix.runtime.fulltext.StopwordsFullTextFilterDescriptor;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.apache.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import org.apache.hyracks.dataflow.common.comm.io.ArrayTupleReference;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;
import org.apache.hyracks.storage.am.lsm.invertedindex.fulltext.FullTextFilterType;

import com.google.common.collect.ImmutableList;

public class FullTextFilterMetadataEntityTupleTranslator extends AbstractTupleTranslator<FullTextFilterMetadataEntity> {

    private final FullTextFilterEntity fullTextFilterEntity;
    protected final ArrayTupleReference tuple;

    protected FullTextFilterMetadataEntityTupleTranslator(boolean getTuple, FullTextFilterEntity fullTextFilterEntity) {
        super(getTuple, fullTextFilterEntity.getIndex(), fullTextFilterEntity.payloadPosition());
        this.fullTextFilterEntity = fullTextFilterEntity;
        if (getTuple) {
            tuple = new ArrayTupleReference();
        } else {
            tuple = null;
        }
    }

    @Override
    protected FullTextFilterMetadataEntity createMetadataEntityFromARecord(ARecord aRecord) throws AlgebricksException {
        int databaseNameIndex = fullTextFilterEntity.databaseNameIndex();
        String databaseName;
        if (databaseNameIndex >= 0) {
            databaseName = ((AString) aRecord.getValueByPos(databaseNameIndex)).getStringValue();
        }
        AString dataverseName = (AString) aRecord.getValueByPos(fullTextFilterEntity.dataverseNameIndex());
        AString filterName = (AString) aRecord.getValueByPos(fullTextFilterEntity.filterNameIndex());
        AString filterTypeAString = (AString) aRecord.getValueByPos(fullTextFilterEntity.filterTypeIndex());

        FullTextFilterType filterType = FullTextFilterType.getEnumIgnoreCase(filterTypeAString.getStringValue());
        switch (filterType) {
            case STOPWORDS:
                return createStopwordsFilterDescriptorFromARecord(dataverseName, filterName, aRecord);
            case STEMMER:
            case SYNONYM:
            default:
                throw new AsterixException(ErrorCode.METADATA_ERROR, "Not supported yet");
        }
    }

    public FullTextFilterMetadataEntity createStopwordsFilterDescriptorFromARecord(AString dataverseName, AString name,
            ARecord aRecord) throws AlgebricksException {
        ImmutableList.Builder<String> stopwordsBuilder = ImmutableList.<String> builder();
        int fieldIndex = aRecord.getType().getFieldIndex(FIELD_NAME_FULL_TEXT_STOPWORD_LIST);
        if (fieldIndex >= 0) {
            IACursor stopwordsCursor = ((AOrderedList) (aRecord.getValueByPos(fieldIndex))).getCursor();
            while (stopwordsCursor.next()) {
                stopwordsBuilder.add(((AString) stopwordsCursor.get()).getStringValue());
            }
        }

        DataverseName dataverse = DataverseName.createFromCanonicalForm(dataverseName.getStringValue());
        String database = MetadataUtil.databaseFor(dataverse);
        StopwordsFullTextFilterDescriptor filterDescriptor = new StopwordsFullTextFilterDescriptor(database, dataverse,
                name.getStringValue(), stopwordsBuilder.build());
        return new FullTextFilterMetadataEntity(filterDescriptor);
    }

    private void writeKeyAndValue2FieldVariables(String key, String value) throws HyracksDataException {
        fieldName.reset();
        aString.setValue(key);
        stringSerde.serialize(aString, fieldName.getDataOutput());

        fieldValue.reset();
        aString.setValue(value);
        stringSerde.serialize(aString, fieldValue.getDataOutput());
    }

    private void writeOrderedList2RecordBuilder(String strFieldName, List<String> list) throws HyracksDataException {
        fieldName.reset();
        aString.setValue(strFieldName);
        stringSerde.serialize(aString, fieldName.getDataOutput());

        OrderedListBuilder listBuilder = new OrderedListBuilder();
        listBuilder.reset(new AOrderedListType(BuiltinType.ASTRING, null));
        ArrayBackedValueStorage itemValue = new ArrayBackedValueStorage();
        for (String s : list) {
            itemValue.reset();
            aString.setValue(s);
            stringSerde.serialize(aString, itemValue.getDataOutput());
            listBuilder.addItem(itemValue);
        }

        fieldValue.reset();
        listBuilder.write(fieldValue.getDataOutput(), true);

        recordBuilder.addField(fieldName, fieldValue);
    }

    private void writeStopwordFilterDescriptor(StopwordsFullTextFilterDescriptor stopwordsFullTextFilterDescriptor)
            throws HyracksDataException {
        writeOrderedList2RecordBuilder(FIELD_NAME_FULL_TEXT_STOPWORD_LIST,
                stopwordsFullTextFilterDescriptor.getStopwordList());
    }

    private void writeFulltextFilter(AbstractFullTextFilterDescriptor filterDescriptor)
            throws AsterixException, HyracksDataException {
        if (fullTextFilterEntity.databaseNameIndex() >= 0) {
            fieldValue.reset();
            aString.setValue(filterDescriptor.getDatabaseName());
            stringSerde.serialize(aString, fieldValue.getDataOutput());
            recordBuilder.addField(fullTextFilterEntity.databaseNameIndex(), fieldValue);
        }
        fieldValue.reset();
        aString.setValue(filterDescriptor.getDataverseName().getCanonicalForm());
        stringSerde.serialize(aString, fieldValue.getDataOutput());
        recordBuilder.addField(fullTextFilterEntity.dataverseNameIndex(), fieldValue);

        fieldValue.reset();
        aString.setValue(filterDescriptor.getName());
        stringSerde.serialize(aString, fieldValue.getDataOutput());
        recordBuilder.addField(fullTextFilterEntity.filterNameIndex(), fieldValue);

        fieldValue.reset();
        aString.setValue(filterDescriptor.getFilterType().getValue());
        stringSerde.serialize(aString, fieldValue.getDataOutput());
        recordBuilder.addField(fullTextFilterEntity.filterTypeIndex(), fieldValue);

        switch (filterDescriptor.getFilterType()) {
            case STOPWORDS:
                writeStopwordFilterDescriptor((StopwordsFullTextFilterDescriptor) filterDescriptor);
                break;
            case STEMMER:
            case SYNONYM:
            default:
                throw new AsterixException(ErrorCode.METADATA_ERROR, "Not supported yet");
        }
    }

    private void writeIndex(String databaseName, String dataverseName, String filterName,
            ArrayTupleBuilder tupleBuilder) throws HyracksDataException {
        if (fullTextFilterEntity.databaseNameIndex() >= 0) {
            aString.setValue(databaseName);
            stringSerde.serialize(aString, tupleBuilder.getDataOutput());
            tupleBuilder.addFieldEndOffset();
        }
        aString.setValue(dataverseName);
        stringSerde.serialize(aString, tupleBuilder.getDataOutput());
        tupleBuilder.addFieldEndOffset();

        aString.setValue(filterName);
        stringSerde.serialize(aString, tupleBuilder.getDataOutput());
        tupleBuilder.addFieldEndOffset();
    }

    @Override
    public ITupleReference getTupleFromMetadataEntity(FullTextFilterMetadataEntity filterMetadataEntity)
            throws HyracksDataException, AsterixException {
        tupleBuilder.reset();

        writeIndex(filterMetadataEntity.getFullTextFilter().getDatabaseName(),
                filterMetadataEntity.getFullTextFilter().getDataverseName().getCanonicalForm(),
                filterMetadataEntity.getFullTextFilter().getName(), tupleBuilder);

        // Write the record
        recordBuilder.reset(fullTextFilterEntity.getRecordType());

        writeFulltextFilter(filterMetadataEntity.getFullTextFilter());

        recordBuilder.write(tupleBuilder.getDataOutput(), true);
        tupleBuilder.addFieldEndOffset();

        tuple.reset(tupleBuilder.getFieldEndOffsets(), tupleBuilder.getByteArray());
        return tuple;
    }
}
