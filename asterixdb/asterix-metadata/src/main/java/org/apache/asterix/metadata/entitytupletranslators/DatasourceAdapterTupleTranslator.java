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

import java.util.Calendar;

import org.apache.asterix.common.external.IDataSourceAdapter;
import org.apache.asterix.common.metadata.DataverseName;
import org.apache.asterix.external.dataset.adapter.AdapterIdentifier;
import org.apache.asterix.metadata.bootstrap.MetadataPrimaryIndexes;
import org.apache.asterix.metadata.bootstrap.MetadataRecordTypes;
import org.apache.asterix.metadata.entities.DatasourceAdapter;
import org.apache.asterix.om.base.ARecord;
import org.apache.asterix.om.base.AString;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;

public class DatasourceAdapterTupleTranslator extends AbstractTupleTranslator<DatasourceAdapter> {

    // Payload field containing serialized Adapter.
    private static final int ADAPTER_PAYLOAD_TUPLE_FIELD_INDEX = 2;

    protected DatasourceAdapterTupleTranslator(boolean getTuple) {
        super(getTuple, MetadataPrimaryIndexes.DATASOURCE_ADAPTER_DATASET, ADAPTER_PAYLOAD_TUPLE_FIELD_INDEX);
    }

    @Override
    protected DatasourceAdapter createMetadataEntityFromARecord(ARecord adapterRecord) throws AlgebricksException {
        String dataverseCanonicalName = ((AString) adapterRecord
                .getValueByPos(MetadataRecordTypes.DATASOURCE_ADAPTER_ARECORD_DATAVERSENAME_FIELD_INDEX))
                        .getStringValue();
        DataverseName dataverseName = DataverseName.createFromCanonicalForm(dataverseCanonicalName);
        String adapterName =
                ((AString) adapterRecord.getValueByPos(MetadataRecordTypes.DATASOURCE_ADAPTER_ARECORD_NAME_FIELD_INDEX))
                        .getStringValue();
        String classname = ((AString) adapterRecord
                .getValueByPos(MetadataRecordTypes.DATASOURCE_ADAPTER_ARECORD_CLASSNAME_FIELD_INDEX)).getStringValue();
        IDataSourceAdapter.AdapterType adapterType = IDataSourceAdapter.AdapterType.valueOf(
                ((AString) adapterRecord.getValueByPos(MetadataRecordTypes.DATASOURCE_ADAPTER_ARECORD_TYPE_FIELD_INDEX))
                        .getStringValue());

        DataverseName libraryDataverseName = null;
        String libraryName = null;
        int libraryNameIdx = adapterRecord.getType().getFieldIndex(MetadataRecordTypes.FIELD_NAME_LIBRARY_NAME);
        if (libraryNameIdx >= 0) {
            libraryName = ((AString) adapterRecord.getValueByPos(libraryNameIdx)).getStringValue();
            int libraryDataverseNameIdx =
                    adapterRecord.getType().getFieldIndex(MetadataRecordTypes.FIELD_NAME_LIBRARY_DATAVERSE_NAME);
            libraryDataverseName = libraryDataverseNameIdx >= 0
                    ? DataverseName.createFromCanonicalForm(
                            ((AString) adapterRecord.getValueByPos(libraryDataverseNameIdx)).getStringValue())
                    : dataverseName;
        }

        return new DatasourceAdapter(new AdapterIdentifier(dataverseName, adapterName), adapterType, classname,
                libraryDataverseName, libraryName);
    }

    @Override
    public ITupleReference getTupleFromMetadataEntity(DatasourceAdapter adapter) throws HyracksDataException {
        AdapterIdentifier adapterIdentifier = adapter.getAdapterIdentifier();
        String dataverseCanonicalName = adapterIdentifier.getDataverseName().getCanonicalForm();

        // write the key in the first 2 fields of the tuple
        tupleBuilder.reset();

        aString.setValue(dataverseCanonicalName);
        stringSerde.serialize(aString, tupleBuilder.getDataOutput());
        tupleBuilder.addFieldEndOffset();
        aString.setValue(adapterIdentifier.getName());
        stringSerde.serialize(aString, tupleBuilder.getDataOutput());
        tupleBuilder.addFieldEndOffset();

        // write the pay-load in the third field of the tuple

        recordBuilder.reset(MetadataRecordTypes.DATASOURCE_ADAPTER_RECORDTYPE);

        // write field 0
        fieldValue.reset();
        aString.setValue(dataverseCanonicalName);
        stringSerde.serialize(aString, fieldValue.getDataOutput());
        recordBuilder.addField(MetadataRecordTypes.DATASOURCE_ADAPTER_ARECORD_DATAVERSENAME_FIELD_INDEX, fieldValue);

        // write field 1
        fieldValue.reset();
        aString.setValue(adapterIdentifier.getName());
        stringSerde.serialize(aString, fieldValue.getDataOutput());
        recordBuilder.addField(MetadataRecordTypes.DATASOURCE_ADAPTER_ARECORD_NAME_FIELD_INDEX, fieldValue);

        // write field 2
        fieldValue.reset();
        aString.setValue(adapter.getClassname());
        stringSerde.serialize(aString, fieldValue.getDataOutput());
        recordBuilder.addField(MetadataRecordTypes.DATASOURCE_ADAPTER_ARECORD_CLASSNAME_FIELD_INDEX, fieldValue);

        // write field 3
        fieldValue.reset();
        aString.setValue(adapter.getType().name());
        stringSerde.serialize(aString, fieldValue.getDataOutput());
        recordBuilder.addField(MetadataRecordTypes.DATASOURCE_ADAPTER_ARECORD_TYPE_FIELD_INDEX, fieldValue);

        // write field 4
        fieldValue.reset();
        aString.setValue(Calendar.getInstance().getTime().toString());
        stringSerde.serialize(aString, fieldValue.getDataOutput());
        recordBuilder.addField(MetadataRecordTypes.DATASOURCE_ADAPTER_ARECORD_TIMESTAMP_FIELD_INDEX, fieldValue);

        // write open fields
        writeOpenFields(adapter);

        // write record
        recordBuilder.write(tupleBuilder.getDataOutput(), true);
        tupleBuilder.addFieldEndOffset();

        tuple.reset(tupleBuilder.getFieldEndOffsets(), tupleBuilder.getByteArray());
        return tuple;
    }

    void writeOpenFields(DatasourceAdapter adapter) throws HyracksDataException {
        writeLibrary(adapter);
    }

    protected void writeLibrary(DatasourceAdapter adapter) throws HyracksDataException {
        if (adapter.getLibraryName() == null) {
            return;
        }

        fieldName.reset();
        aString.setValue(MetadataRecordTypes.FIELD_NAME_LIBRARY_DATAVERSE_NAME);
        stringSerde.serialize(aString, fieldName.getDataOutput());
        fieldValue.reset();
        aString.setValue(adapter.getLibraryDataverseName().getCanonicalForm());
        stringSerde.serialize(aString, fieldValue.getDataOutput());
        recordBuilder.addField(fieldName, fieldValue);

        fieldName.reset();
        aString.setValue(MetadataRecordTypes.FIELD_NAME_LIBRARY_NAME);
        stringSerde.serialize(aString, fieldName.getDataOutput());
        fieldValue.reset();
        aString.setValue(adapter.getLibraryName());
        stringSerde.serialize(aString, fieldValue.getDataOutput());
        recordBuilder.addField(fieldName, fieldValue);
    }
}
