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

import org.apache.asterix.common.metadata.DataverseName;
import org.apache.asterix.common.metadata.MetadataUtil;
import org.apache.asterix.metadata.bootstrap.MetadataRecordTypes;
import org.apache.asterix.metadata.bootstrap.SynonymEntity;
import org.apache.asterix.metadata.entities.Synonym;
import org.apache.asterix.om.base.ARecord;
import org.apache.asterix.om.base.AString;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;

/**
 * Translates a Synonym metadata entity to an ITupleReference and vice versa.
 */
public final class SynonymTupleTranslator extends AbstractTupleTranslator<Synonym> {

    private final SynonymEntity synonymEntity;

    protected SynonymTupleTranslator(boolean getTuple, SynonymEntity synonymEntity) {
        super(getTuple, synonymEntity.getIndex(), synonymEntity.payloadPosition());
        this.synonymEntity = synonymEntity;
    }

    @Override
    protected Synonym createMetadataEntityFromARecord(ARecord synonymRecord) throws AlgebricksException {
        String dataverseCanonicalName =
                ((AString) synonymRecord.getValueByPos(synonymEntity.dataverseNameIndex())).getStringValue();
        DataverseName dataverseName = DataverseName.createFromCanonicalForm(dataverseCanonicalName);
        int databaseNameIndex = synonymEntity.databaseNameIndex();
        String databaseName;
        if (databaseNameIndex >= 0) {
            databaseName = ((AString) synonymRecord.getValueByPos(databaseNameIndex)).getStringValue();
        } else {
            databaseName = MetadataUtil.databaseFor(dataverseName);
        }
        String synonymName = ((AString) synonymRecord.getValueByPos(synonymEntity.synonymNameIndex())).getStringValue();

        String objectDataverseCanonicalName =
                ((AString) synonymRecord.getValueByPos(synonymEntity.objectDataverseNameIndex())).getStringValue();
        DataverseName objectDataverseName = DataverseName.createFromCanonicalForm(objectDataverseCanonicalName);

        String objectDatabaseName;
        int objectDatabaseIdx =
                synonymRecord.getType().getFieldIndex(MetadataRecordTypes.FIELD_NAME_OBJECT_DATABASE_NAME);
        if (objectDatabaseIdx >= 0) {
            objectDatabaseName = ((AString) synonymRecord.getValueByPos(objectDatabaseIdx)).getStringValue();
        } else {
            objectDatabaseName = MetadataUtil.databaseFor(objectDataverseName);
        }

        String objectName = ((AString) synonymRecord.getValueByPos(synonymEntity.objectNameIndex())).getStringValue();

        return new Synonym(databaseName, dataverseName, synonymName, objectDatabaseName, objectDataverseName,
                objectName);
    }

    @Override
    public ITupleReference getTupleFromMetadataEntity(Synonym synonym) throws HyracksDataException {
        String dataverseCanonicalName = synonym.getDataverseName().getCanonicalForm();

        // write the key in the first 2 fields of the tuple
        tupleBuilder.reset();

        if (synonymEntity.databaseNameIndex() >= 0) {
            aString.setValue(synonym.getDatabaseName());
            stringSerde.serialize(aString, tupleBuilder.getDataOutput());
            tupleBuilder.addFieldEndOffset();
        }
        aString.setValue(dataverseCanonicalName);
        stringSerde.serialize(aString, tupleBuilder.getDataOutput());
        tupleBuilder.addFieldEndOffset();
        aString.setValue(synonym.getSynonymName());
        stringSerde.serialize(aString, tupleBuilder.getDataOutput());
        tupleBuilder.addFieldEndOffset();

        // write the pay-load in the third field of the tuple
        recordBuilder.reset(synonymEntity.getRecordType());

        if (synonymEntity.databaseNameIndex() >= 0) {
            fieldValue.reset();
            aString.setValue(synonym.getDatabaseName());
            stringSerde.serialize(aString, fieldValue.getDataOutput());
            recordBuilder.addField(synonymEntity.databaseNameIndex(), fieldValue);
        }
        // write field 0
        fieldValue.reset();
        aString.setValue(dataverseCanonicalName);
        stringSerde.serialize(aString, fieldValue.getDataOutput());
        recordBuilder.addField(synonymEntity.dataverseNameIndex(), fieldValue);

        // write field 1
        fieldValue.reset();
        aString.setValue(synonym.getSynonymName());
        stringSerde.serialize(aString, fieldValue.getDataOutput());
        recordBuilder.addField(synonymEntity.synonymNameIndex(), fieldValue);

        // write field 2
        fieldValue.reset();
        aString.setValue(synonym.getObjectDataverseName().getCanonicalForm());
        stringSerde.serialize(aString, fieldValue.getDataOutput());
        recordBuilder.addField(synonymEntity.objectDataverseNameIndex(), fieldValue);

        // write field 3
        fieldValue.reset();
        aString.setValue(synonym.getObjectName());
        stringSerde.serialize(aString, fieldValue.getDataOutput());
        recordBuilder.addField(synonymEntity.objectNameIndex(), fieldValue);

        // write open fields
        writeOpenFields(synonym);

        // write record
        recordBuilder.write(tupleBuilder.getDataOutput(), true);
        tupleBuilder.addFieldEndOffset();

        tuple.reset(tupleBuilder.getFieldEndOffsets(), tupleBuilder.getByteArray());
        return tuple;
    }

    private void writeOpenFields(Synonym synonym) throws HyracksDataException {
        //TODO(DB): should use something better
        if (synonymEntity.databaseNameIndex() >= 0) {
            fieldName.reset();
            aString.setValue(MetadataRecordTypes.FIELD_NAME_OBJECT_DATABASE_NAME);
            stringSerde.serialize(aString, fieldName.getDataOutput());
            fieldValue.reset();
            aString.setValue(synonym.getObjectDatabaseName());
            stringSerde.serialize(aString, fieldValue.getDataOutput());
            recordBuilder.addField(fieldName, fieldValue);
        }
    }
}
