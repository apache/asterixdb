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

import java.util.ArrayList;
import java.util.List;

import org.apache.asterix.builders.IARecordBuilder;
import org.apache.asterix.builders.UnorderedListBuilder;
import org.apache.asterix.common.functions.FunctionSignature;
import org.apache.asterix.common.metadata.DataverseName;
import org.apache.asterix.metadata.bootstrap.MetadataPrimaryIndexes;
import org.apache.asterix.metadata.bootstrap.MetadataRecordTypes;
import org.apache.asterix.metadata.entities.FeedConnection;
import org.apache.asterix.om.base.AMissing;
import org.apache.asterix.om.base.ANull;
import org.apache.asterix.om.base.ARecord;
import org.apache.asterix.om.base.AString;
import org.apache.asterix.om.base.AUnorderedList;
import org.apache.asterix.om.base.IACursor;
import org.apache.asterix.om.types.AUnorderedListType;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;

public class FeedConnectionTupleTranslator extends AbstractTupleTranslator<FeedConnection> {

    // Payload field containing serialized ExternalFile.
    private static final int FEED_CONNECTION_PAYLOAD_TUPLE_FIELD_INDEX = 3;

    public FeedConnectionTupleTranslator(boolean getTuple) {
        super(getTuple, MetadataPrimaryIndexes.FEED_CONNECTION_DATASET, FEED_CONNECTION_PAYLOAD_TUPLE_FIELD_INDEX);
    }

    @Override
    protected FeedConnection createMetadataEntityFromARecord(ARecord feedConnectionRecord) {
        String dataverseCanonicalName =
                ((AString) feedConnectionRecord.getValueByPos(MetadataRecordTypes.FEED_CONN_DATAVERSE_NAME_FIELD_INDEX))
                        .getStringValue();
        DataverseName dataverseName = DataverseName.createFromCanonicalForm(dataverseCanonicalName);
        String feedName =
                ((AString) feedConnectionRecord.getValueByPos(MetadataRecordTypes.FEED_CONN_FEED_NAME_FIELD_INDEX))
                        .getStringValue();
        String datasetName =
                ((AString) feedConnectionRecord.getValueByPos(MetadataRecordTypes.FEED_CONN_DATASET_NAME_FIELD_INDEX))
                        .getStringValue();
        String outputType =
                ((AString) feedConnectionRecord.getValueByPos(MetadataRecordTypes.FEED_CONN_OUTPUT_TYPE_INDEX))
                        .getStringValue();
        String policyName =
                ((AString) feedConnectionRecord.getValueByPos(MetadataRecordTypes.FEED_CONN_POLICY_FIELD_INDEX))
                        .getStringValue();
        List<FunctionSignature> appliedFunctions = null;
        Object o = feedConnectionRecord.getValueByPos(MetadataRecordTypes.FEED_CONN_APPLIED_FUNCTIONS_FIELD_INDEX);
        IACursor cursor;

        if (!(o instanceof ANull) && !(o instanceof AMissing)) {
            appliedFunctions = new ArrayList<>();
            AUnorderedList afList = (AUnorderedList) feedConnectionRecord
                    .getValueByPos(MetadataRecordTypes.FEED_CONN_APPLIED_FUNCTIONS_FIELD_INDEX);
            cursor = afList.getCursor();
            while (cursor.next()) {
                String afValue = ((AString) cursor.get()).getStringValue();
                int pos = afValue.lastIndexOf('.'); //TODO(MULTI_PART_DATAVERSE_NAME):REVISIT
                String afDataverseCanonicalName = afValue.substring(0, pos);
                String afName = afValue.substring(pos + 1);
                DataverseName afDataverseName = DataverseName.createFromCanonicalForm(afDataverseCanonicalName);
                FunctionSignature functionSignature = new FunctionSignature(afDataverseName, afName, 1);
                appliedFunctions.add(functionSignature);
            }
        }

        int whereClauseIdx = feedConnectionRecord.getType().getFieldIndex(MetadataRecordTypes.FIELD_NAME_WHERE_CLAUSE);
        String whereClauseBody = whereClauseIdx >= 0
                ? ((AString) feedConnectionRecord.getValueByPos(whereClauseIdx)).getStringValue() : "";

        return new FeedConnection(dataverseName, feedName, datasetName, appliedFunctions, policyName, whereClauseBody,
                outputType);
    }

    @Override
    public ITupleReference getTupleFromMetadataEntity(FeedConnection feedConnection) throws HyracksDataException {
        String dataverseCanonicalName = feedConnection.getDataverseName().getCanonicalForm();

        tupleBuilder.reset();

        // key: dataverse
        aString.setValue(dataverseCanonicalName);
        stringSerde.serialize(aString, tupleBuilder.getDataOutput());
        tupleBuilder.addFieldEndOffset();

        // key: feedName
        aString.setValue(feedConnection.getFeedName());
        stringSerde.serialize(aString, tupleBuilder.getDataOutput());
        tupleBuilder.addFieldEndOffset();

        // key: dataset
        aString.setValue(feedConnection.getDatasetName());
        stringSerde.serialize(aString, tupleBuilder.getDataOutput());
        tupleBuilder.addFieldEndOffset();

        recordBuilder.reset(MetadataRecordTypes.FEED_CONNECTION_RECORDTYPE);

        // field dataverse
        fieldValue.reset();
        aString.setValue(dataverseCanonicalName);
        stringSerde.serialize(aString, fieldValue.getDataOutput());
        recordBuilder.addField(MetadataRecordTypes.FEED_CONN_DATAVERSE_NAME_FIELD_INDEX, fieldValue);

        // field: feedId
        fieldValue.reset();
        aString.setValue(feedConnection.getFeedName());
        stringSerde.serialize(aString, fieldValue.getDataOutput());
        recordBuilder.addField(MetadataRecordTypes.FEED_CONN_FEED_NAME_FIELD_INDEX, fieldValue);

        // field: dataset
        fieldValue.reset();
        aString.setValue(feedConnection.getDatasetName());
        stringSerde.serialize(aString, fieldValue.getDataOutput());
        recordBuilder.addField(MetadataRecordTypes.FEED_CONN_DATASET_NAME_FIELD_INDEX, fieldValue);

        // field: outputType
        fieldValue.reset();
        aString.setValue(feedConnection.getOutputType());
        stringSerde.serialize(aString, fieldValue.getDataOutput());
        recordBuilder.addField(MetadataRecordTypes.FEED_CONN_OUTPUT_TYPE_INDEX, fieldValue);

        // field: appliedFunctions
        fieldValue.reset();
        writeAppliedFunctionsField(recordBuilder, feedConnection, fieldValue);

        // field: policyName
        fieldValue.reset();
        aString.setValue(feedConnection.getPolicyName());
        stringSerde.serialize(aString, fieldValue.getDataOutput());
        recordBuilder.addField(MetadataRecordTypes.FEED_CONN_POLICY_FIELD_INDEX, fieldValue);

        // write open fields
        writeOpenFields(feedConnection);

        recordBuilder.write(tupleBuilder.getDataOutput(), true);
        tupleBuilder.addFieldEndOffset();

        tuple.reset(tupleBuilder.getFieldEndOffsets(), tupleBuilder.getByteArray());
        return tuple;
    }

    protected void writeOpenFields(FeedConnection feedConnection) throws HyracksDataException {
        writeWhereClauseBody(feedConnection);
    }

    private void writeWhereClauseBody(FeedConnection feedConnection) throws HyracksDataException {
        // field: whereClauseBody
        if (feedConnection.getWhereClauseBody() != null && feedConnection.getWhereClauseBody().length() > 0) {
            fieldName.reset();
            aString.setValue(MetadataRecordTypes.FIELD_NAME_WHERE_CLAUSE);
            stringSerde.serialize(aString, fieldName.getDataOutput());
            fieldValue.reset();
            aString.setValue(feedConnection.getWhereClauseBody());
            stringSerde.serialize(aString, fieldValue.getDataOutput());
            recordBuilder.addField(fieldName, fieldValue);
        }
    }

    private void writeAppliedFunctionsField(IARecordBuilder rb, FeedConnection fc, ArrayBackedValueStorage buffer)
            throws HyracksDataException {
        UnorderedListBuilder listBuilder = new UnorderedListBuilder();
        ArrayBackedValueStorage listEleBuffer = new ArrayBackedValueStorage();

        listBuilder.reset((AUnorderedListType) MetadataRecordTypes.FEED_CONNECTION_RECORDTYPE
                .getFieldTypes()[MetadataRecordTypes.FEED_CONN_APPLIED_FUNCTIONS_FIELD_INDEX]);
        if (fc.getAppliedFunctions() != null) {
            List<FunctionSignature> appliedFunctions = fc.getAppliedFunctions();
            for (FunctionSignature af : appliedFunctions) {
                listEleBuffer.reset();
                aString.setValue(af.getDataverseName().getCanonicalForm() + '.' + af.getName()); //TODO(MULTI_PART_DATAVERSE_NAME):REVISIT
                stringSerde.serialize(aString, listEleBuffer.getDataOutput());
                listBuilder.addItem(listEleBuffer);
            }
        }
        listBuilder.write(buffer.getDataOutput(), true);
        rb.addField(MetadataRecordTypes.FEED_CONN_APPLIED_FUNCTIONS_FIELD_INDEX, buffer);
    }
}
