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

import java.util.Date;

import org.apache.asterix.common.config.DatasetConfig.ExternalFilePendingOp;
import org.apache.asterix.common.metadata.DataverseName;
import org.apache.asterix.external.indexing.ExternalFile;
import org.apache.asterix.formats.nontagged.SerializerDeserializerProvider;
import org.apache.asterix.metadata.bootstrap.ExternalFileEntity;
import org.apache.asterix.metadata.utils.MetadataUtil;
import org.apache.asterix.om.base.ADateTime;
import org.apache.asterix.om.base.AInt32;
import org.apache.asterix.om.base.AInt64;
import org.apache.asterix.om.base.AMutableDateTime;
import org.apache.asterix.om.base.AMutableInt32;
import org.apache.asterix.om.base.AMutableInt64;
import org.apache.asterix.om.base.ARecord;
import org.apache.asterix.om.base.AString;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.api.dataflow.value.ISerializerDeserializer;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;

public class ExternalFileTupleTranslator extends AbstractTupleTranslator<ExternalFile> {

    private final ExternalFileEntity externalFileEntity;
    protected AMutableInt32 aInt32;
    protected AMutableInt64 aInt64;
    protected AMutableDateTime aDateTime;
    protected ISerializerDeserializer<ADateTime> dateTimeSerde;

    @SuppressWarnings("unchecked")
    protected ExternalFileTupleTranslator(boolean getTuple, ExternalFileEntity externalFileEntity) {
        super(getTuple, externalFileEntity.getIndex(), externalFileEntity.payloadPosition());
        this.externalFileEntity = externalFileEntity;
        if (getTuple) {
            aInt32 = new AMutableInt32(0);
            aInt64 = new AMutableInt64(0);
            aDateTime = new AMutableDateTime(0);
            dateTimeSerde = SerializerDeserializerProvider.INSTANCE.getSerializerDeserializer(BuiltinType.ADATETIME);
        }
    }

    @Override
    protected ExternalFile createMetadataEntityFromARecord(ARecord externalFileRecord) throws AlgebricksException {
        String dataverseCanonicalName =
                ((AString) externalFileRecord.getValueByPos(externalFileEntity.dataverseNameIndex())).getStringValue();
        DataverseName dataverseName = DataverseName.createFromCanonicalForm(dataverseCanonicalName);
        int databaseNameIndex = externalFileEntity.databaseNameIndex();
        String databaseName;
        if (databaseNameIndex >= 0) {
            databaseName = ((AString) externalFileRecord.getValueByPos(databaseNameIndex)).getStringValue();
        } else {
            databaseName = MetadataUtil.databaseFor(dataverseName);
        }
        String datasetName =
                ((AString) externalFileRecord.getValueByPos(externalFileEntity.datasetNameIndex())).getStringValue();
        int fileNumber =
                ((AInt32) externalFileRecord.getValueByPos(externalFileEntity.fileNumberIndex())).getIntegerValue();
        String fileName =
                ((AString) externalFileRecord.getValueByPos(externalFileEntity.fileNameIndex())).getStringValue();
        long fileSize = ((AInt64) externalFileRecord.getValueByPos(externalFileEntity.fileSizeIndex())).getLongValue();
        Date lastMoDifiedDate = new Date(
                ((ADateTime) externalFileRecord.getValueByPos(externalFileEntity.fileModDateIndex())).getChrononTime());
        ExternalFilePendingOp pendingOp = ExternalFilePendingOp
                .values()[((AInt32) externalFileRecord.getValueByPos(externalFileEntity.pendingOpIndex()))
                        .getIntegerValue()];
        return new ExternalFile(databaseName, dataverseName, datasetName, fileNumber, fileName, lastMoDifiedDate,
                fileSize, pendingOp);
    }

    @Override
    public ITupleReference getTupleFromMetadataEntity(ExternalFile externalFile) throws HyracksDataException {
        String dataverseCanonicalName = externalFile.getDataverseName().getCanonicalForm();

        // write the key in the first 3 fields of the tuple
        tupleBuilder.reset();
        if (externalFileEntity.databaseNameIndex() >= 0) {
            aString.setValue(externalFile.getDatabaseName());
            stringSerde.serialize(aString, tupleBuilder.getDataOutput());
            tupleBuilder.addFieldEndOffset();
        }
        // dataverse name
        aString.setValue(dataverseCanonicalName);
        stringSerde.serialize(aString, tupleBuilder.getDataOutput());
        tupleBuilder.addFieldEndOffset();
        // dataset name
        aString.setValue(externalFile.getDatasetName());
        stringSerde.serialize(aString, tupleBuilder.getDataOutput());
        tupleBuilder.addFieldEndOffset();
        // file number
        aInt32.setValue(externalFile.getFileNumber());
        int32Serde.serialize(aInt32, tupleBuilder.getDataOutput());
        tupleBuilder.addFieldEndOffset();

        // write the pay-load in the fourth field of the tuple
        recordBuilder.reset(externalFileEntity.getRecordType());

        if (externalFileEntity.databaseNameIndex() >= 0) {
            fieldValue.reset();
            aString.setValue(externalFile.getDatabaseName());
            stringSerde.serialize(aString, fieldValue.getDataOutput());
            recordBuilder.addField(externalFileEntity.databaseNameIndex(), fieldValue);
        }
        // write field 0
        fieldValue.reset();
        aString.setValue(dataverseCanonicalName);
        stringSerde.serialize(aString, fieldValue.getDataOutput());
        recordBuilder.addField(externalFileEntity.dataverseNameIndex(), fieldValue);

        // write field 1
        fieldValue.reset();
        aString.setValue(externalFile.getDatasetName());
        stringSerde.serialize(aString, fieldValue.getDataOutput());
        recordBuilder.addField(externalFileEntity.datasetNameIndex(), fieldValue);

        // write field 2
        fieldValue.reset();
        aInt32.setValue(externalFile.getFileNumber());
        int32Serde.serialize(aInt32, fieldValue.getDataOutput());
        recordBuilder.addField(externalFileEntity.fileNumberIndex(), fieldValue);

        // write field 3
        fieldValue.reset();
        aString.setValue(externalFile.getFileName());
        stringSerde.serialize(aString, fieldValue.getDataOutput());
        recordBuilder.addField(externalFileEntity.fileNameIndex(), fieldValue);

        // write field 4
        fieldValue.reset();
        aInt64.setValue(externalFile.getSize());
        int64Serde.serialize(aInt64, fieldValue.getDataOutput());
        recordBuilder.addField(externalFileEntity.fileSizeIndex(), fieldValue);

        // write field 5
        fieldValue.reset();
        aDateTime.setValue(externalFile.getLastModefiedTime().getTime());
        dateTimeSerde.serialize(aDateTime, fieldValue.getDataOutput());
        recordBuilder.addField(externalFileEntity.fileModDateIndex(), fieldValue);

        // write field 6
        fieldValue.reset();
        aInt32.setValue(externalFile.getPendingOp().ordinal());
        int32Serde.serialize(aInt32, fieldValue.getDataOutput());
        recordBuilder.addField(externalFileEntity.pendingOpIndex(), fieldValue);

        // write record
        recordBuilder.write(tupleBuilder.getDataOutput(), true);
        tupleBuilder.addFieldEndOffset();

        tuple.reset(tupleBuilder.getFieldEndOffsets(), tupleBuilder.getByteArray());
        return tuple;
    }
}
