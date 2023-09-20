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

import org.apache.asterix.common.metadata.DataverseName;
import org.apache.asterix.metadata.bootstrap.DataverseEntity;
import org.apache.asterix.metadata.entities.Dataverse;
import org.apache.asterix.metadata.utils.MetadataUtil;
import org.apache.asterix.om.base.AInt32;
import org.apache.asterix.om.base.AMutableInt32;
import org.apache.asterix.om.base.ARecord;
import org.apache.asterix.om.base.AString;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;

/**
 * Translates a Dataverse metadata entity to an ITupleReference and vice versa.
 */
public class DataverseTupleTranslator extends AbstractTupleTranslator<Dataverse> {

    private final DataverseEntity dataverseEntity;
    private AMutableInt32 aInt32;

    protected DataverseTupleTranslator(boolean getTuple, DataverseEntity dataverseEntity) {
        super(getTuple, dataverseEntity.getIndex(), dataverseEntity.payloadPosition());
        this.dataverseEntity = dataverseEntity;
        if (getTuple) {
            aInt32 = new AMutableInt32(-1);
        }
    }

    @Override
    protected Dataverse createMetadataEntityFromARecord(ARecord dataverseRecord) throws AlgebricksException {
        String dataverseCanonicalName =
                ((AString) dataverseRecord.getValueByPos(dataverseEntity.dataverseNameIndex())).getStringValue();
        DataverseName dataverseName = DataverseName.createFromCanonicalForm(dataverseCanonicalName);
        int databaseNameIndex = dataverseEntity.databaseNameIndex();
        String databaseName;
        if (databaseNameIndex >= 0) {
            databaseName = ((AString) dataverseRecord.getValueByPos(databaseNameIndex)).getStringValue();
        } else {
            databaseName = MetadataUtil.databaseFor(dataverseName);
        }
        String format = ((AString) dataverseRecord.getValueByPos(dataverseEntity.dataFormatIndex())).getStringValue();
        int pendingOp = ((AInt32) dataverseRecord.getValueByPos(dataverseEntity.pendingOpIndex())).getIntegerValue();

        return new Dataverse(databaseName, dataverseName, format, pendingOp);
    }

    @Override
    public ITupleReference getTupleFromMetadataEntity(Dataverse dataverse) throws HyracksDataException {
        String dataverseCanonicalName = dataverse.getDataverseName().getCanonicalForm();

        // write the key in the first field of the tuple
        tupleBuilder.reset();
        if (dataverseEntity.databaseNameIndex() >= 0) {
            aString.setValue(dataverse.getDatabaseName());
            stringSerde.serialize(aString, tupleBuilder.getDataOutput());
            tupleBuilder.addFieldEndOffset();
        }
        aString.setValue(dataverseCanonicalName);
        stringSerde.serialize(aString, tupleBuilder.getDataOutput());
        tupleBuilder.addFieldEndOffset();

        // write the payload in the second field of the tuple
        recordBuilder.reset(dataverseEntity.getRecordType());

        if (dataverseEntity.databaseNameIndex() >= 0) {
            fieldValue.reset();
            aString.setValue(dataverse.getDatabaseName());
            stringSerde.serialize(aString, fieldValue.getDataOutput());
            recordBuilder.addField(dataverseEntity.databaseNameIndex(), fieldValue);
        }
        // write field 0
        fieldValue.reset();
        aString.setValue(dataverseCanonicalName);
        stringSerde.serialize(aString, fieldValue.getDataOutput());
        recordBuilder.addField(dataverseEntity.dataverseNameIndex(), fieldValue);

        // write field 1
        fieldValue.reset();
        aString.setValue(dataverse.getDataFormat());
        stringSerde.serialize(aString, fieldValue.getDataOutput());
        recordBuilder.addField(dataverseEntity.dataFormatIndex(), fieldValue);

        // write field 2
        fieldValue.reset();
        aString.setValue(Calendar.getInstance().getTime().toString());
        stringSerde.serialize(aString, fieldValue.getDataOutput());
        recordBuilder.addField(dataverseEntity.timestampIndex(), fieldValue);

        // write field 3
        fieldValue.reset();
        aInt32.setValue(dataverse.getPendingOp());
        int32Serde.serialize(aInt32, fieldValue.getDataOutput());
        recordBuilder.addField(dataverseEntity.pendingOpIndex(), fieldValue);

        // write record
        recordBuilder.write(tupleBuilder.getDataOutput(), true);
        tupleBuilder.addFieldEndOffset();

        tuple.reset(tupleBuilder.getFieldEndOffsets(), tupleBuilder.getByteArray());
        return tuple;
    }
}
