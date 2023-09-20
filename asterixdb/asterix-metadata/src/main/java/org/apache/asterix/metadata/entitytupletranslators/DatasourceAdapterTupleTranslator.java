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
import org.apache.asterix.metadata.bootstrap.DatasourceAdapterEntity;
import org.apache.asterix.metadata.bootstrap.MetadataRecordTypes;
import org.apache.asterix.metadata.entities.DatasourceAdapter;
import org.apache.asterix.metadata.utils.MetadataUtil;
import org.apache.asterix.om.base.ARecord;
import org.apache.asterix.om.base.AString;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;

public class DatasourceAdapterTupleTranslator extends AbstractTupleTranslator<DatasourceAdapter> {

    private final DatasourceAdapterEntity datasourceAdapterEntity;

    protected DatasourceAdapterTupleTranslator(boolean getTuple, DatasourceAdapterEntity datasourceAdapterEntity) {
        super(getTuple, datasourceAdapterEntity.getIndex(), datasourceAdapterEntity.payloadPosition());
        this.datasourceAdapterEntity = datasourceAdapterEntity;
    }

    @Override
    protected DatasourceAdapter createMetadataEntityFromARecord(ARecord adapterRecord) throws AlgebricksException {
        String dataverseCanonicalName =
                ((AString) adapterRecord.getValueByPos(datasourceAdapterEntity.dataverseNameIndex())).getStringValue();
        DataverseName dataverseName = DataverseName.createFromCanonicalForm(dataverseCanonicalName);
        int databaseNameIndex = datasourceAdapterEntity.databaseNameIndex();
        String databaseName;
        if (databaseNameIndex >= 0) {
            databaseName = ((AString) adapterRecord.getValueByPos(databaseNameIndex)).getStringValue();
        } else {
            databaseName = MetadataUtil.databaseFor(dataverseName);
        }
        String adapterName =
                ((AString) adapterRecord.getValueByPos(datasourceAdapterEntity.adapterNameIndex())).getStringValue();
        String classname =
                ((AString) adapterRecord.getValueByPos(datasourceAdapterEntity.classNameIndex())).getStringValue();
        IDataSourceAdapter.AdapterType adapterType = IDataSourceAdapter.AdapterType
                .valueOf(((AString) adapterRecord.getValueByPos(datasourceAdapterEntity.typeIndex())).getStringValue());

        String libraryDatabase = null;
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
            libraryDatabase = MetadataUtil.resolveDatabase(libraryDatabase, libraryDataverseName);
        }

        return new DatasourceAdapter(new AdapterIdentifier(databaseName, dataverseName, adapterName), adapterType,
                classname, libraryDatabase, libraryDataverseName, libraryName);
    }

    @Override
    public ITupleReference getTupleFromMetadataEntity(DatasourceAdapter adapter) throws HyracksDataException {
        AdapterIdentifier adapterIdentifier = adapter.getAdapterIdentifier();
        String dataverseCanonicalName = adapterIdentifier.getDataverseName().getCanonicalForm();

        // write the key in the first 2 fields of the tuple
        tupleBuilder.reset();

        if (datasourceAdapterEntity.databaseNameIndex() >= 0) {
            aString.setValue(adapterIdentifier.getDatabaseName());
            stringSerde.serialize(aString, tupleBuilder.getDataOutput());
            tupleBuilder.addFieldEndOffset();
        }
        aString.setValue(dataverseCanonicalName);
        stringSerde.serialize(aString, tupleBuilder.getDataOutput());
        tupleBuilder.addFieldEndOffset();
        aString.setValue(adapterIdentifier.getName());
        stringSerde.serialize(aString, tupleBuilder.getDataOutput());
        tupleBuilder.addFieldEndOffset();

        // write the pay-load in the third field of the tuple
        recordBuilder.reset(datasourceAdapterEntity.getRecordType());

        if (datasourceAdapterEntity.databaseNameIndex() >= 0) {
            fieldValue.reset();
            aString.setValue(adapterIdentifier.getDatabaseName());
            stringSerde.serialize(aString, fieldValue.getDataOutput());
            recordBuilder.addField(datasourceAdapterEntity.databaseNameIndex(), fieldValue);
        }
        // write field 0
        fieldValue.reset();
        aString.setValue(dataverseCanonicalName);
        stringSerde.serialize(aString, fieldValue.getDataOutput());
        recordBuilder.addField(datasourceAdapterEntity.dataverseNameIndex(), fieldValue);

        // write field 1
        fieldValue.reset();
        aString.setValue(adapterIdentifier.getName());
        stringSerde.serialize(aString, fieldValue.getDataOutput());
        recordBuilder.addField(datasourceAdapterEntity.adapterNameIndex(), fieldValue);

        // write field 2
        fieldValue.reset();
        aString.setValue(adapter.getClassname());
        stringSerde.serialize(aString, fieldValue.getDataOutput());
        recordBuilder.addField(datasourceAdapterEntity.classNameIndex(), fieldValue);

        // write field 3
        fieldValue.reset();
        aString.setValue(adapter.getType().name());
        stringSerde.serialize(aString, fieldValue.getDataOutput());
        recordBuilder.addField(datasourceAdapterEntity.typeIndex(), fieldValue);

        // write field 4
        fieldValue.reset();
        aString.setValue(Calendar.getInstance().getTime().toString());
        stringSerde.serialize(aString, fieldValue.getDataOutput());
        recordBuilder.addField(datasourceAdapterEntity.timestampIndex(), fieldValue);

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

        //TODO(DB): write library database name
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
