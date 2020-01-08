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
import org.apache.asterix.metadata.bootstrap.MetadataPrimaryIndexes;
import org.apache.asterix.metadata.bootstrap.MetadataRecordTypes;
import org.apache.asterix.metadata.entities.Synonym;
import org.apache.asterix.om.base.ARecord;
import org.apache.asterix.om.base.AString;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;

/**
 * Translates a Synonym metadata entity to an ITupleReference and vice versa.
 */
public final class SynonymTupleTranslator extends AbstractTupleTranslator<Synonym> {

    // Payload field containing serialized Synonym.

    private static final int SYNONYM_PAYLOAD_TUPLE_FIELD_INDEX = 2;

    protected SynonymTupleTranslator(boolean getTuple) {
        super(getTuple, MetadataPrimaryIndexes.SYNONYM_DATASET, SYNONYM_PAYLOAD_TUPLE_FIELD_INDEX);
    }

    @Override
    protected Synonym createMetadataEntityFromARecord(ARecord synonymRecord) {
        String dataverseCanonicalName =
                ((AString) synonymRecord.getValueByPos(MetadataRecordTypes.SYNONYM_ARECORD_DATAVERSENAME_FIELD_INDEX))
                        .getStringValue();
        DataverseName dataverseName = DataverseName.createFromCanonicalForm(dataverseCanonicalName);

        String synonymName =
                ((AString) synonymRecord.getValueByPos(MetadataRecordTypes.SYNONYM_ARECORD_SYNONYMNAME_FIELD_INDEX))
                        .getStringValue();

        String objectDataverseCanonicalName = ((AString) synonymRecord
                .getValueByPos(MetadataRecordTypes.SYNONYM_ARECORD_OBJECTDATAVERSENAME_FIELD_INDEX)).getStringValue();
        DataverseName objectDataverseName = DataverseName.createFromCanonicalForm(objectDataverseCanonicalName);

        String objectName =
                ((AString) synonymRecord.getValueByPos(MetadataRecordTypes.SYNONYM_ARECORD_OBJECTNAME_FIELD_INDEX))
                        .getStringValue();

        return new Synonym(dataverseName, synonymName, objectDataverseName, objectName);
    }

    @Override
    public ITupleReference getTupleFromMetadataEntity(Synonym synonym) throws HyracksDataException {
        String dataverseCanonicalName = synonym.getDataverseName().getCanonicalForm();

        // write the key in the first 2 fields of the tuple
        tupleBuilder.reset();

        aString.setValue(dataverseCanonicalName);
        stringSerde.serialize(aString, tupleBuilder.getDataOutput());
        tupleBuilder.addFieldEndOffset();
        aString.setValue(synonym.getSynonymName());
        stringSerde.serialize(aString, tupleBuilder.getDataOutput());
        tupleBuilder.addFieldEndOffset();

        // write the pay-load in the third field of the tuple

        recordBuilder.reset(MetadataRecordTypes.SYNONYM_RECORDTYPE);

        // write field 0
        fieldValue.reset();
        aString.setValue(dataverseCanonicalName);
        stringSerde.serialize(aString, fieldValue.getDataOutput());
        recordBuilder.addField(MetadataRecordTypes.SYNONYM_ARECORD_DATAVERSENAME_FIELD_INDEX, fieldValue);

        // write field 1
        fieldValue.reset();
        aString.setValue(synonym.getSynonymName());
        stringSerde.serialize(aString, fieldValue.getDataOutput());
        recordBuilder.addField(MetadataRecordTypes.SYNONYM_ARECORD_SYNONYMNAME_FIELD_INDEX, fieldValue);

        // write field 2
        fieldValue.reset();
        aString.setValue(synonym.getObjectDataverseName().getCanonicalForm());
        stringSerde.serialize(aString, fieldValue.getDataOutput());
        recordBuilder.addField(MetadataRecordTypes.SYNONYM_ARECORD_OBJECTDATAVERSENAME_FIELD_INDEX, fieldValue);

        // write field 3
        fieldValue.reset();
        aString.setValue(synonym.getObjectName());
        stringSerde.serialize(aString, fieldValue.getDataOutput());
        recordBuilder.addField(MetadataRecordTypes.SYNONYM_ARECORD_OBJECTNAME_FIELD_INDEX, fieldValue);

        // write record
        recordBuilder.write(tupleBuilder.getDataOutput(), true);
        tupleBuilder.addFieldEndOffset();

        tuple.reset(tupleBuilder.getFieldEndOffsets(), tupleBuilder.getByteArray());
        return tuple;
    }
}