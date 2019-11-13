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
import org.apache.asterix.metadata.bootstrap.MetadataPrimaryIndexes;
import org.apache.asterix.metadata.bootstrap.MetadataRecordTypes;
import org.apache.asterix.om.base.ADateTime;
import org.apache.asterix.om.base.AInt32;
import org.apache.asterix.om.base.AInt64;
import org.apache.asterix.om.base.AMutableDateTime;
import org.apache.asterix.om.base.AMutableInt32;
import org.apache.asterix.om.base.AMutableInt64;
import org.apache.asterix.om.base.ARecord;
import org.apache.asterix.om.base.AString;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.hyracks.api.dataflow.value.ISerializerDeserializer;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;

public class ExternalFileTupleTranslator extends AbstractTupleTranslator<ExternalFile> {

    // Payload field containing serialized ExternalFile.
    private static final int EXTERNAL_FILE_PAYLOAD_TUPLE_FIELD_INDEX = 3;

    protected AMutableInt32 aInt32;
    protected AMutableInt64 aInt64;
    protected AMutableDateTime aDateTime;
    protected ISerializerDeserializer<ADateTime> dateTimeSerde;

    @SuppressWarnings("unchecked")
    protected ExternalFileTupleTranslator(boolean getTuple) {
        super(getTuple, MetadataPrimaryIndexes.EXTERNAL_FILE_DATASET, EXTERNAL_FILE_PAYLOAD_TUPLE_FIELD_INDEX);
        if (getTuple) {
            aInt32 = new AMutableInt32(0);
            aInt64 = new AMutableInt64(0);
            aDateTime = new AMutableDateTime(0);
            dateTimeSerde = SerializerDeserializerProvider.INSTANCE.getSerializerDeserializer(BuiltinType.ADATETIME);
        }
    }

    @Override
    protected ExternalFile createMetadataEntityFromARecord(ARecord externalFileRecord) {
        String dataverseCanonicalName = ((AString) externalFileRecord
                .getValueByPos(MetadataRecordTypes.EXTERNAL_FILE_ARECORD_DATAVERSENAME_FIELD_INDEX)).getStringValue();
        DataverseName dataverseName = DataverseName.createFromCanonicalForm(dataverseCanonicalName);
        String datasetName = ((AString) externalFileRecord
                .getValueByPos(MetadataRecordTypes.EXTERNAL_FILE_ARECORD_DATASET_NAME_FIELD_INDEX)).getStringValue();
        int fileNumber = ((AInt32) externalFileRecord
                .getValueByPos(MetadataRecordTypes.EXTERNAL_FILE_ARECORD_FILE_NUMBER_FIELD_INDEX)).getIntegerValue();
        String fileName = ((AString) externalFileRecord
                .getValueByPos(MetadataRecordTypes.EXTERNAL_FILE_ARECORD_FILE_NAME_FIELD_INDEX)).getStringValue();
        long fileSize = ((AInt64) externalFileRecord
                .getValueByPos(MetadataRecordTypes.EXTERNAL_FILE_ARECORD_FILE_SIZE_FIELD_INDEX)).getLongValue();
        Date lastMoDifiedDate = new Date(((ADateTime) externalFileRecord
                .getValueByPos(MetadataRecordTypes.EXTERNAL_FILE_ARECORD_FILE_MOD_DATE_FIELD_INDEX)).getChrononTime());
        ExternalFilePendingOp pendingOp = ExternalFilePendingOp.values()[((AInt32) externalFileRecord
                .getValueByPos(MetadataRecordTypes.EXTERNAL_FILE_ARECORD_FILE_PENDING_OP_FIELD_INDEX))
                        .getIntegerValue()];
        return new ExternalFile(dataverseName, datasetName, fileNumber, fileName, lastMoDifiedDate, fileSize,
                pendingOp);
    }

    @Override
    public ITupleReference getTupleFromMetadataEntity(ExternalFile externalFile) throws HyracksDataException {
        String dataverseCanonicalName = externalFile.getDataverseName().getCanonicalForm();

        // write the key in the first 3 fields of the tuple
        tupleBuilder.reset();
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
        recordBuilder.reset(MetadataRecordTypes.EXTERNAL_FILE_RECORDTYPE);

        // write field 0
        fieldValue.reset();
        aString.setValue(dataverseCanonicalName);
        stringSerde.serialize(aString, fieldValue.getDataOutput());
        recordBuilder.addField(MetadataRecordTypes.EXTERNAL_FILE_ARECORD_DATAVERSENAME_FIELD_INDEX, fieldValue);

        // write field 1
        fieldValue.reset();
        aString.setValue(externalFile.getDatasetName());
        stringSerde.serialize(aString, fieldValue.getDataOutput());
        recordBuilder.addField(MetadataRecordTypes.EXTERNAL_FILE_ARECORD_DATASET_NAME_FIELD_INDEX, fieldValue);

        // write field 2
        fieldValue.reset();
        aInt32.setValue(externalFile.getFileNumber());
        int32Serde.serialize(aInt32, fieldValue.getDataOutput());
        recordBuilder.addField(MetadataRecordTypes.EXTERNAL_FILE_ARECORD_FILE_NUMBER_FIELD_INDEX, fieldValue);

        // write field 3
        fieldValue.reset();
        aString.setValue(externalFile.getFileName());
        stringSerde.serialize(aString, fieldValue.getDataOutput());
        recordBuilder.addField(MetadataRecordTypes.EXTERNAL_FILE_ARECORD_FILE_NAME_FIELD_INDEX, fieldValue);

        // write field 4
        fieldValue.reset();
        aInt64.setValue(externalFile.getSize());
        int64Serde.serialize(aInt64, fieldValue.getDataOutput());
        recordBuilder.addField(MetadataRecordTypes.EXTERNAL_FILE_ARECORD_FILE_SIZE_FIELD_INDEX, fieldValue);

        // write field 5
        fieldValue.reset();
        aDateTime.setValue(externalFile.getLastModefiedTime().getTime());
        dateTimeSerde.serialize(aDateTime, fieldValue.getDataOutput());
        recordBuilder.addField(MetadataRecordTypes.EXTERNAL_FILE_ARECORD_FILE_MOD_DATE_FIELD_INDEX, fieldValue);

        // write field 6
        fieldValue.reset();
        aInt32.setValue(externalFile.getPendingOp().ordinal());
        int32Serde.serialize(aInt32, fieldValue.getDataOutput());
        recordBuilder.addField(MetadataRecordTypes.EXTERNAL_FILE_ARECORD_FILE_PENDING_OP_FIELD_INDEX, fieldValue);

        // write record
        recordBuilder.write(tupleBuilder.getDataOutput(), true);
        tupleBuilder.addFieldEndOffset();

        tuple.reset(tupleBuilder.getFieldEndOffsets(), tupleBuilder.getByteArray());
        return tuple;
    }
}
