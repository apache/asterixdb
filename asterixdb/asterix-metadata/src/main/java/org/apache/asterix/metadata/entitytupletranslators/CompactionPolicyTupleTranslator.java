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
import org.apache.asterix.metadata.bootstrap.CompactionPolicyEntity;
import org.apache.asterix.metadata.entities.CompactionPolicy;
import org.apache.asterix.metadata.utils.MetadataUtil;
import org.apache.asterix.om.base.ARecord;
import org.apache.asterix.om.base.AString;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;

/**
 * Translates a CompactionPolicy metadata entity to an ITupleReference and vice versa.
 */
public class CompactionPolicyTupleTranslator extends AbstractTupleTranslator<CompactionPolicy> {

    private final CompactionPolicyEntity compactionPolicyEntity;

    protected CompactionPolicyTupleTranslator(boolean getTuple, CompactionPolicyEntity compactionPolicyEntity) {
        super(getTuple, compactionPolicyEntity.getIndex(), compactionPolicyEntity.payloadPosition());
        this.compactionPolicyEntity = compactionPolicyEntity;
    }

    @Override
    protected CompactionPolicy createMetadataEntityFromARecord(ARecord compactionPolicyRecord)
            throws AlgebricksException {
        String dataverseCanonicalName =
                ((AString) compactionPolicyRecord.getValueByPos(compactionPolicyEntity.dataverseNameIndex()))
                        .getStringValue();
        DataverseName dataverseName = DataverseName.createFromCanonicalForm(dataverseCanonicalName);
        String databaseName;
        int databaseNameIndex = compactionPolicyEntity.databaseNameIndex();
        if (databaseNameIndex >= 0) {
            databaseName = ((AString) compactionPolicyRecord.getValueByPos(databaseNameIndex)).getStringValue();
        } else {
            databaseName = MetadataUtil.databaseFor(dataverseName);
        }
        String policyName = ((AString) compactionPolicyRecord.getValueByPos(compactionPolicyEntity.policyNameIndex()))
                .getStringValue();
        String className = ((AString) compactionPolicyRecord.getValueByPos(compactionPolicyEntity.classNameIndex()))
                .getStringValue();

        return new CompactionPolicy(databaseName, dataverseName, policyName, className);
    }

    @Override
    public ITupleReference getTupleFromMetadataEntity(CompactionPolicy compactionPolicy) throws HyracksDataException {
        String dataverseCanonicalName = compactionPolicy.getDataverseName().getCanonicalForm();

        tupleBuilder.reset();
        if (compactionPolicyEntity.databaseNameIndex() >= 0) {
            aString.setValue(compactionPolicy.getDatabaseName());
            stringSerde.serialize(aString, tupleBuilder.getDataOutput());
            tupleBuilder.addFieldEndOffset();
        }
        aString.setValue(dataverseCanonicalName);
        stringSerde.serialize(aString, tupleBuilder.getDataOutput());
        tupleBuilder.addFieldEndOffset();

        aString.setValue(compactionPolicy.getPolicyName());
        stringSerde.serialize(aString, tupleBuilder.getDataOutput());
        tupleBuilder.addFieldEndOffset();

        recordBuilder.reset(compactionPolicyEntity.getRecordType());

        if (compactionPolicyEntity.databaseNameIndex() >= 0) {
            fieldValue.reset();
            aString.setValue(compactionPolicy.getDatabaseName());
            stringSerde.serialize(aString, fieldValue.getDataOutput());
            recordBuilder.addField(compactionPolicyEntity.databaseNameIndex(), fieldValue);
        }
        // write field 0
        fieldValue.reset();
        aString.setValue(dataverseCanonicalName);
        stringSerde.serialize(aString, fieldValue.getDataOutput());
        recordBuilder.addField(compactionPolicyEntity.dataverseNameIndex(), fieldValue);

        // write field 1
        fieldValue.reset();
        aString.setValue(compactionPolicy.getPolicyName());
        stringSerde.serialize(aString, fieldValue.getDataOutput());
        recordBuilder.addField(compactionPolicyEntity.policyNameIndex(), fieldValue);

        // write field 2
        fieldValue.reset();
        aString.setValue(compactionPolicy.getClassName());
        stringSerde.serialize(aString, fieldValue.getDataOutput());
        recordBuilder.addField(compactionPolicyEntity.classNameIndex(), fieldValue);

        // write record
        recordBuilder.write(tupleBuilder.getDataOutput(), true);
        tupleBuilder.addFieldEndOffset();

        tuple.reset(tupleBuilder.getFieldEndOffsets(), tupleBuilder.getByteArray());
        return tuple;
    }
}
