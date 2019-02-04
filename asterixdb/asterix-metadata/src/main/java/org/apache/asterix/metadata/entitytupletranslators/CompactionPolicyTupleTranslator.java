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

import java.io.ByteArrayInputStream;
import java.io.DataInput;
import java.io.DataInputStream;

import org.apache.asterix.formats.nontagged.SerializerDeserializerProvider;
import org.apache.asterix.metadata.bootstrap.MetadataPrimaryIndexes;
import org.apache.asterix.metadata.bootstrap.MetadataRecordTypes;
import org.apache.asterix.metadata.entities.CompactionPolicy;
import org.apache.asterix.om.base.ARecord;
import org.apache.asterix.om.base.AString;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.api.dataflow.value.ISerializerDeserializer;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;

/**
 * Translates a Dataset metadata entity to an ITupleReference and vice versa.
 */
public class CompactionPolicyTupleTranslator extends AbstractTupleTranslator<CompactionPolicy> {
    private static final long serialVersionUID = 5291424952240239023L;

    // Field indexes of serialized CompactionPolicy in a tuple.
    // Key field.
    public static final int COMPACTION_POLICY_DATAVERSE_NAME_FIELD_INDEX = 0;

    public static final int COMPACTION_POLICY_NAME_FIELD_INDEX = 1;

    // Payload field containing serialized compactionPolicy.
    public static final int COMPACTION_POLICY_PAYLOAD_TUPLE_FIELD_INDEX = 2;

    @SuppressWarnings("unchecked")
    private ISerializerDeserializer<ARecord> recordSerDes = SerializerDeserializerProvider.INSTANCE
            .getSerializerDeserializer(MetadataRecordTypes.COMPACTION_POLICY_RECORDTYPE);

    protected CompactionPolicyTupleTranslator(boolean getTuple) {
        super(getTuple, MetadataPrimaryIndexes.COMPACTION_POLICY_DATASET.getFieldCount());
    }

    @Override
    public CompactionPolicy getMetadataEntityFromTuple(ITupleReference tuple) throws HyracksDataException {
        byte[] serRecord = tuple.getFieldData(COMPACTION_POLICY_PAYLOAD_TUPLE_FIELD_INDEX);
        int recordStartOffset = tuple.getFieldStart(COMPACTION_POLICY_PAYLOAD_TUPLE_FIELD_INDEX);
        int recordLength = tuple.getFieldLength(COMPACTION_POLICY_PAYLOAD_TUPLE_FIELD_INDEX);
        ByteArrayInputStream stream = new ByteArrayInputStream(serRecord, recordStartOffset, recordLength);
        DataInput in = new DataInputStream(stream);
        ARecord compactionPolicyRecord = recordSerDes.deserialize(in);
        return createCompactionPolicyFromARecord(compactionPolicyRecord);
    }

    private CompactionPolicy createCompactionPolicyFromARecord(ARecord compactionPolicyRecord) {
        CompactionPolicy compactionPolicy = null;
        String dataverseName = ((AString) compactionPolicyRecord
                .getValueByPos(MetadataRecordTypes.COMPACTION_POLICY_ARECORD_DATAVERSE_NAME_FIELD_INDEX))
                        .getStringValue();
        String policyName = ((AString) compactionPolicyRecord
                .getValueByPos(MetadataRecordTypes.COMPACTION_POLICY_ARECORD_POLICY_NAME_FIELD_INDEX)).getStringValue();
        String className = ((AString) compactionPolicyRecord
                .getValueByPos(MetadataRecordTypes.COMPACTION_POLICY_ARECORD_CLASSNAME_FIELD_INDEX)).getStringValue();

        compactionPolicy = new CompactionPolicy(dataverseName, policyName, className);
        return compactionPolicy;
    }

    @Override
    public ITupleReference getTupleFromMetadataEntity(CompactionPolicy compactionPolicy)
            throws HyracksDataException, AlgebricksException {

        tupleBuilder.reset();
        aString.setValue(compactionPolicy.getDataverseName());
        stringSerde.serialize(aString, tupleBuilder.getDataOutput());
        tupleBuilder.addFieldEndOffset();

        aString.setValue(compactionPolicy.getPolicyName());
        stringSerde.serialize(aString, tupleBuilder.getDataOutput());
        tupleBuilder.addFieldEndOffset();

        recordBuilder.reset(MetadataRecordTypes.COMPACTION_POLICY_RECORDTYPE);

        // write field 0
        fieldValue.reset();
        aString.setValue(compactionPolicy.getDataverseName());
        stringSerde.serialize(aString, fieldValue.getDataOutput());
        recordBuilder.addField(MetadataRecordTypes.COMPACTION_POLICY_ARECORD_DATAVERSE_NAME_FIELD_INDEX, fieldValue);

        // write field 1
        fieldValue.reset();
        aString.setValue(compactionPolicy.getPolicyName());
        stringSerde.serialize(aString, fieldValue.getDataOutput());
        recordBuilder.addField(MetadataRecordTypes.COMPACTION_POLICY_ARECORD_POLICY_NAME_FIELD_INDEX, fieldValue);

        // write field 2
        fieldValue.reset();
        aString.setValue(compactionPolicy.getClassName());
        stringSerde.serialize(aString, fieldValue.getDataOutput());
        recordBuilder.addField(MetadataRecordTypes.COMPACTION_POLICY_ARECORD_CLASSNAME_FIELD_INDEX, fieldValue);

        // write record
        recordBuilder.write(tupleBuilder.getDataOutput(), true);
        tupleBuilder.addFieldEndOffset();

        tuple.reset(tupleBuilder.getFieldEndOffsets(), tupleBuilder.getByteArray());
        return tuple;
    }
}
