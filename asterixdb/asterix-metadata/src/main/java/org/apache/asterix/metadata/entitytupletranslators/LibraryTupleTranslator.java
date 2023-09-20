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

import static org.apache.asterix.metadata.bootstrap.MetadataRecordTypes.FIELD_NAME_HASH;
import static org.apache.asterix.metadata.bootstrap.MetadataRecordTypes.FIELD_NAME_LANGUAGE;
import static org.apache.asterix.metadata.bootstrap.MetadataRecordTypes.FIELD_NAME_PENDING_OP;

import java.util.Calendar;

import org.apache.asterix.common.functions.ExternalFunctionLanguage;
import org.apache.asterix.common.metadata.DataverseName;
import org.apache.asterix.metadata.bootstrap.LibraryEntity;
import org.apache.asterix.metadata.entities.Library;
import org.apache.asterix.metadata.utils.MetadataUtil;
import org.apache.asterix.om.base.AInt32;
import org.apache.asterix.om.base.ARecord;
import org.apache.asterix.om.base.AString;
import org.apache.asterix.om.types.ARecordType;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;

/**
 * Translates a Library metadata entity to an ITupleReference and vice versa.
 */
public class LibraryTupleTranslator extends AbstractTupleTranslator<Library> {

    private final LibraryEntity libraryEntity;

    protected LibraryTupleTranslator(boolean getTuple, LibraryEntity libraryEntity) {
        super(getTuple, libraryEntity.getIndex(), libraryEntity.payloadPosition());
        this.libraryEntity = libraryEntity;
    }

    @Override
    protected Library createMetadataEntityFromARecord(ARecord libraryRecord) throws AlgebricksException {
        String dataverseCanonicalName =
                ((AString) libraryRecord.getValueByPos(libraryEntity.dataverseNameIndex())).getStringValue();
        DataverseName dataverseName = DataverseName.createFromCanonicalForm(dataverseCanonicalName);
        int databaseNameIndex = libraryEntity.databaseNameIndex();
        String databaseName;
        if (databaseNameIndex >= 0) {
            databaseName = ((AString) libraryRecord.getValueByPos(databaseNameIndex)).getStringValue();
        } else {
            databaseName = MetadataUtil.databaseFor(dataverseName);
        }
        String libraryName = ((AString) libraryRecord.getValueByPos(libraryEntity.libraryNameIndex())).getStringValue();

        ARecordType libraryRecordType = libraryRecord.getType();
        int pendingOpIdx = libraryRecordType.getFieldIndex(FIELD_NAME_PENDING_OP);
        int pendingOp = pendingOpIdx >= 0 ? ((AInt32) libraryRecord.getValueByPos(pendingOpIdx)).getIntegerValue()
                : MetadataUtil.PENDING_NO_OP;

        int languageIdx = libraryRecordType.getFieldIndex(FIELD_NAME_LANGUAGE);
        String language = languageIdx >= 0 ? ((AString) libraryRecord.getValueByPos(languageIdx)).getStringValue()
                : ExternalFunctionLanguage.JAVA.name();

        int hashIdx = libraryRecordType.getFieldIndex(FIELD_NAME_HASH);
        String hash = hashIdx >= 0 ? ((AString) libraryRecord.getValueByPos(hashIdx)).getStringValue() : null;

        return new Library(databaseName, dataverseName, libraryName, language, hash, pendingOp);
    }

    @Override
    public ITupleReference getTupleFromMetadataEntity(Library library) throws HyracksDataException {
        String dataverseCanonicalName = library.getDataverseName().getCanonicalForm();

        // write the key in the first 2 fields of the tuple
        if (libraryEntity.databaseNameIndex() >= 0) {
            aString.setValue(library.getDatabaseName());
            stringSerde.serialize(aString, tupleBuilder.getDataOutput());
            tupleBuilder.addFieldEndOffset();
        }
        tupleBuilder.reset();
        aString.setValue(dataverseCanonicalName);
        stringSerde.serialize(aString, tupleBuilder.getDataOutput());
        tupleBuilder.addFieldEndOffset();
        aString.setValue(library.getName());
        stringSerde.serialize(aString, tupleBuilder.getDataOutput());
        tupleBuilder.addFieldEndOffset();

        // write the pay-load in the third field of the tuple
        recordBuilder.reset(libraryEntity.getRecordType());

        if (libraryEntity.databaseNameIndex() >= 0) {
            fieldValue.reset();
            aString.setValue(library.getDatabaseName());
            stringSerde.serialize(aString, fieldValue.getDataOutput());
            recordBuilder.addField(libraryEntity.databaseNameIndex(), fieldValue);
        }
        // write field 0
        fieldValue.reset();
        aString.setValue(dataverseCanonicalName);
        stringSerde.serialize(aString, fieldValue.getDataOutput());
        recordBuilder.addField(libraryEntity.dataverseNameIndex(), fieldValue);

        // write field 1
        fieldValue.reset();
        aString.setValue(library.getName());
        stringSerde.serialize(aString, fieldValue.getDataOutput());
        recordBuilder.addField(libraryEntity.libraryNameIndex(), fieldValue);

        // write field 2
        fieldValue.reset();
        aString.setValue(Calendar.getInstance().getTime().toString());
        stringSerde.serialize(aString, fieldValue.getDataOutput());
        recordBuilder.addField(libraryEntity.timestampIndex(), fieldValue);

        writeOpenFields(library);

        // write record
        recordBuilder.write(tupleBuilder.getDataOutput(), true);
        tupleBuilder.addFieldEndOffset();

        tuple.reset(tupleBuilder.getFieldEndOffsets(), tupleBuilder.getByteArray());
        return tuple;
    }

    protected void writeOpenFields(Library library) throws HyracksDataException {
        writeLanguage(library);
        writePendingOp(library);
        writeHash(library);
    }

    private void writeLanguage(Library library) throws HyracksDataException {
        String language = library.getLanguage();

        fieldName.reset();
        aString.setValue(FIELD_NAME_LANGUAGE);
        stringSerde.serialize(aString, fieldName.getDataOutput());
        fieldValue.reset();
        aString.setValue(language);
        stringSerde.serialize(aString, fieldValue.getDataOutput());
        recordBuilder.addField(fieldName, fieldValue);
    }

    private void writeHash(Library library) throws HyracksDataException {
        String hash = library.getHash();

        fieldName.reset();
        aString.setValue(FIELD_NAME_HASH);
        stringSerde.serialize(aString, fieldName.getDataOutput());
        fieldValue.reset();
        aString.setValue(hash);
        stringSerde.serialize(aString, fieldValue.getDataOutput());
        recordBuilder.addField(fieldName, fieldValue);
    }

    private void writePendingOp(Library library) throws HyracksDataException {
        int pendingOp = library.getPendingOp();

        if (pendingOp != MetadataUtil.PENDING_NO_OP) {
            fieldName.reset();
            aString.setValue(FIELD_NAME_PENDING_OP);
            stringSerde.serialize(aString, fieldName.getDataOutput());
            fieldValue.reset();
            int32Serde.serialize(new AInt32(pendingOp), fieldValue.getDataOutput());
            recordBuilder.addField(fieldName, fieldValue);
        }
    }
}
