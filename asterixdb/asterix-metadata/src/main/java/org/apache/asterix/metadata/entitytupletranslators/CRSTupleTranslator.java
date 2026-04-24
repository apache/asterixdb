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
import org.apache.asterix.metadata.bootstrap.CRSEntity;
import org.apache.asterix.metadata.entities.CoordinateReferenceSystem;
import org.apache.asterix.om.base.AInt32;
import org.apache.asterix.om.base.AMutableInt32;
import org.apache.asterix.om.base.ARecord;
import org.apache.asterix.om.base.AString;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;

/**
 * Translates a CoordinateReferenceSystem metadata entity to an ITupleReference and vice versa.
 */
public final class CRSTupleTranslator extends AbstractTupleTranslator<CoordinateReferenceSystem> {

    private final CRSEntity crsEntity;
    private AMutableInt32 aInt32;

    CRSTupleTranslator(boolean getTuple, CRSEntity crsEntity) {
        super(getTuple, crsEntity.getIndex(), crsEntity.payloadPosition());
        this.crsEntity = crsEntity;
        if (getTuple) {
            aInt32 = new AMutableInt32(-1);
        }
    }

    @Override
    protected CoordinateReferenceSystem createMetadataEntityFromARecord(ARecord crsRecord) throws AlgebricksException {
        String dataverseCanonicalName =
                ((AString) crsRecord.getValueByPos(crsEntity.dataverseNameIndex())).getStringValue();
        DataverseName dataverseName = DataverseName.createFromCanonicalForm(dataverseCanonicalName);
        int databaseNameIndex = crsEntity.databaseNameIndex();
        String databaseName;
        if (databaseNameIndex >= 0) {
            databaseName = ((AString) crsRecord.getValueByPos(databaseNameIndex)).getStringValue();
        } else {
            databaseName = MetadataUtil.databaseFor(dataverseName);
        }
        int srid = ((AInt32) crsRecord.getValueByPos(crsEntity.getSridIndex())).getIntegerValue();
        String crsName = ((AString) crsRecord.getValueByPos(crsEntity.getCrsNameIndex())).getStringValue();
        String crsWkt = ((AString) crsRecord.getValueByPos(crsEntity.getCrsWktIndex())).getStringValue();
        return new CoordinateReferenceSystem(databaseName, dataverseName, srid, crsName, crsWkt);
    }

    @Override
    public ITupleReference getTupleFromMetadataEntity(CoordinateReferenceSystem crs) throws HyracksDataException {
        String dataverseCanonicalName = crs.getDataverseName().getCanonicalForm();

        tupleBuilder.reset();

        // write key fields
        if (crsEntity.databaseNameIndex() >= 0) {
            aString.setValue(crs.getDatabaseName());
            stringSerde.serialize(aString, tupleBuilder.getDataOutput());
            tupleBuilder.addFieldEndOffset();
        }
        aString.setValue(dataverseCanonicalName);
        stringSerde.serialize(aString, tupleBuilder.getDataOutput());
        tupleBuilder.addFieldEndOffset();
        aInt32.setValue(crs.getSrid());
        int32Serde.serialize(aInt32, tupleBuilder.getDataOutput());
        tupleBuilder.addFieldEndOffset();

        // write the payload record
        recordBuilder.reset(crsEntity.getRecordType());

        if (crsEntity.databaseNameIndex() >= 0) {
            fieldValue.reset();
            aString.setValue(crs.getDatabaseName());
            stringSerde.serialize(aString, fieldValue.getDataOutput());
            recordBuilder.addField(crsEntity.databaseNameIndex(), fieldValue);
        }

        // write DataverseName
        fieldValue.reset();
        aString.setValue(dataverseCanonicalName);
        stringSerde.serialize(aString, fieldValue.getDataOutput());
        recordBuilder.addField(crsEntity.dataverseNameIndex(), fieldValue);

        // write SRID
        fieldValue.reset();
        aInt32.setValue(crs.getSrid());
        int32Serde.serialize(aInt32, fieldValue.getDataOutput());
        recordBuilder.addField(crsEntity.getSridIndex(), fieldValue);

        // write CRSName
        fieldValue.reset();
        aString.setValue(crs.getCrsName());
        stringSerde.serialize(aString, fieldValue.getDataOutput());
        recordBuilder.addField(crsEntity.getCrsNameIndex(), fieldValue);

        // write CrsWKT
        fieldValue.reset();
        aString.setValue(crs.getCrsWkt());
        stringSerde.serialize(aString, fieldValue.getDataOutput());
        recordBuilder.addField(crsEntity.getCrsWktIndex(), fieldValue);

        // write the payload record to the tuple
        recordBuilder.write(tupleBuilder.getDataOutput(), true);
        tupleBuilder.addFieldEndOffset();

        tuple.reset(tupleBuilder.getFieldEndOffsets(), tupleBuilder.getByteArray());
        return tuple;
    }
}
