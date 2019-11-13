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
import org.apache.asterix.metadata.bootstrap.MetadataPrimaryIndexes;
import org.apache.asterix.metadata.bootstrap.MetadataRecordTypes;
import org.apache.asterix.metadata.entities.Dataverse;
import org.apache.asterix.om.base.AInt32;
import org.apache.asterix.om.base.AMutableInt32;
import org.apache.asterix.om.base.ARecord;
import org.apache.asterix.om.base.AString;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;

/**
 * Translates a Dataverse metadata entity to an ITupleReference and vice versa.
 */
public class DataverseTupleTranslator extends AbstractTupleTranslator<Dataverse> {

    // Payload field containing serialized Dataverse.
    private static final int DATAVERSE_PAYLOAD_TUPLE_FIELD_INDEX = 1;

    protected AMutableInt32 aInt32;

    protected DataverseTupleTranslator(boolean getTuple) {
        super(getTuple, MetadataPrimaryIndexes.DATAVERSE_DATASET, DATAVERSE_PAYLOAD_TUPLE_FIELD_INDEX);
        if (getTuple) {
            aInt32 = new AMutableInt32(-1);
        }
    }

    @Override
    protected Dataverse createMetadataEntityFromARecord(ARecord dataverseRecord) {
        String dataverseCanonicalName = ((AString) dataverseRecord.getValueByPos(0)).getStringValue();
        DataverseName dataverseName = DataverseName.createFromCanonicalForm(dataverseCanonicalName);
        String format = ((AString) dataverseRecord.getValueByPos(1)).getStringValue();
        int pendingOp = ((AInt32) dataverseRecord.getValueByPos(3)).getIntegerValue();

        return new Dataverse(dataverseName, format, pendingOp);
    }

    @Override
    public ITupleReference getTupleFromMetadataEntity(Dataverse dataverse) throws HyracksDataException {
        String dataverseCanonicalName = dataverse.getDataverseName().getCanonicalForm();

        // write the key in the first field of the tuple
        tupleBuilder.reset();
        aString.setValue(dataverseCanonicalName);
        stringSerde.serialize(aString, tupleBuilder.getDataOutput());
        tupleBuilder.addFieldEndOffset();

        // write the payload in the second field of the tuple
        recordBuilder.reset(MetadataRecordTypes.DATAVERSE_RECORDTYPE);
        // write field 0
        fieldValue.reset();
        aString.setValue(dataverseCanonicalName);
        stringSerde.serialize(aString, fieldValue.getDataOutput());
        recordBuilder.addField(MetadataRecordTypes.DATAVERSE_ARECORD_NAME_FIELD_INDEX, fieldValue);

        // write field 1
        fieldValue.reset();
        aString.setValue(dataverse.getDataFormat());
        stringSerde.serialize(aString, fieldValue.getDataOutput());
        recordBuilder.addField(MetadataRecordTypes.DATAVERSE_ARECORD_FORMAT_FIELD_INDEX, fieldValue);

        // write field 2
        fieldValue.reset();
        aString.setValue(Calendar.getInstance().getTime().toString());
        stringSerde.serialize(aString, fieldValue.getDataOutput());
        recordBuilder.addField(MetadataRecordTypes.DATAVERSE_ARECORD_TIMESTAMP_FIELD_INDEX, fieldValue);

        // write field 3
        fieldValue.reset();
        aInt32.setValue(dataverse.getPendingOp());
        int32Serde.serialize(aInt32, fieldValue.getDataOutput());
        recordBuilder.addField(MetadataRecordTypes.DATAVERSE_ARECORD_PENDINGOP_FIELD_INDEX, fieldValue);

        // write record
        recordBuilder.write(tupleBuilder.getDataOutput(), true);
        tupleBuilder.addFieldEndOffset();

        tuple.reset(tupleBuilder.getFieldEndOffsets(), tupleBuilder.getByteArray());
        return tuple;
    }
}
