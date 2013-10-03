/*
 * Copyright 2009-2013 by The Regents of the University of California
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * you may obtain a copy of the License from
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package edu.uci.ics.asterix.metadata.entitytupletranslators;

import java.io.ByteArrayInputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.IOException;

import edu.uci.ics.asterix.common.exceptions.AsterixException;
import edu.uci.ics.asterix.formats.nontagged.AqlSerializerDeserializerProvider;
import edu.uci.ics.asterix.metadata.MetadataException;
import edu.uci.ics.asterix.metadata.bootstrap.MetadataPrimaryIndexes;
import edu.uci.ics.asterix.metadata.bootstrap.MetadataRecordTypes;
import edu.uci.ics.asterix.metadata.entities.CompactionPolicy;
import edu.uci.ics.asterix.om.base.ARecord;
import edu.uci.ics.asterix.om.base.AString;
import edu.uci.ics.hyracks.api.dataflow.value.ISerializerDeserializer;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.ITupleReference;

/**
 * Translates a Dataset metadata entity to an ITupleReference and vice versa.
 */
public class CompactionPolicyTupleTranslator extends AbstractTupleTranslator<CompactionPolicy> {
    // Field indexes of serialized CompactionPolicy in a tuple.
    // Key field.
    public static final int COMPACTION_POLICY_DATAVERSE_NAME_FIELD_INDEX = 0;

    public static final int COMPACTION_POLICY_NAME_FIELD_INDEX = 1;

    // Payload field containing serialized compactionPolicy.
    public static final int COMPACTION_POLICY_PAYLOAD_TUPLE_FIELD_INDEX = 2;

    @SuppressWarnings("unchecked")
    private ISerializerDeserializer<ARecord> recordSerDes = AqlSerializerDeserializerProvider.INSTANCE
            .getSerializerDeserializer(MetadataRecordTypes.COMPACTION_POLICY_RECORDTYPE);

    public CompactionPolicyTupleTranslator(boolean getTuple) {
        super(getTuple, MetadataPrimaryIndexes.COMPACTION_POLICY_DATASET.getFieldCount());
    }

    @Override
    public CompactionPolicy getMetadataEntytiFromTuple(ITupleReference frameTuple) throws IOException {
        byte[] serRecord = frameTuple.getFieldData(COMPACTION_POLICY_PAYLOAD_TUPLE_FIELD_INDEX);
        int recordStartOffset = frameTuple.getFieldStart(COMPACTION_POLICY_PAYLOAD_TUPLE_FIELD_INDEX);
        int recordLength = frameTuple.getFieldLength(COMPACTION_POLICY_PAYLOAD_TUPLE_FIELD_INDEX);
        ByteArrayInputStream stream = new ByteArrayInputStream(serRecord, recordStartOffset, recordLength);
        DataInput in = new DataInputStream(stream);
        ARecord compactionPolicyRecord = (ARecord) recordSerDes.deserialize(in);
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
    public ITupleReference getTupleFromMetadataEntity(CompactionPolicy compactionPolicy) throws IOException,
            MetadataException {

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
        try {
            recordBuilder.write(tupleBuilder.getDataOutput(), true);
        } catch (AsterixException e) {
            throw new MetadataException(e);
        }
        tupleBuilder.addFieldEndOffset();

        tuple.reset(tupleBuilder.getFieldEndOffsets(), tupleBuilder.getByteArray());
        return tuple;
    }
}
