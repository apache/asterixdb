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
import org.apache.asterix.common.metadata.MetadataUtil;
import org.apache.asterix.metadata.bootstrap.FeedConnectionEntity;
import org.apache.asterix.metadata.bootstrap.MetadataRecordTypes;
import org.apache.asterix.metadata.entities.FeedConnection;
import org.apache.asterix.om.base.AMissing;
import org.apache.asterix.om.base.ANull;
import org.apache.asterix.om.base.ARecord;
import org.apache.asterix.om.base.AString;
import org.apache.asterix.om.base.AUnorderedList;
import org.apache.asterix.om.base.IACursor;
import org.apache.asterix.om.types.AUnorderedListType;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;

public class FeedConnectionTupleTranslator extends AbstractTupleTranslator<FeedConnection> {

    private final FeedConnectionEntity feedConnectionEntity;

    public FeedConnectionTupleTranslator(boolean getTuple, FeedConnectionEntity feedConnectionEntity) {
        super(getTuple, feedConnectionEntity.getIndex(), feedConnectionEntity.payloadPosition());
        this.feedConnectionEntity = feedConnectionEntity;
    }

    @Override
    protected FeedConnection createMetadataEntityFromARecord(ARecord feedConnectionRecord) throws AlgebricksException {
        String dataverseCanonicalName =
                ((AString) feedConnectionRecord.getValueByPos(feedConnectionEntity.dataverseNameIndex()))
                        .getStringValue();
        DataverseName dataverseName = DataverseName.createFromCanonicalForm(dataverseCanonicalName);
        int databaseNameIndex = feedConnectionEntity.databaseNameIndex();
        String databaseName;
        if (databaseNameIndex >= 0) {
            databaseName = ((AString) feedConnectionRecord.getValueByPos(databaseNameIndex)).getStringValue();
        } else {
            databaseName = MetadataUtil.databaseFor(dataverseName);
        }
        String feedName =
                ((AString) feedConnectionRecord.getValueByPos(feedConnectionEntity.feedNameIndex())).getStringValue();
        String datasetName = ((AString) feedConnectionRecord.getValueByPos(feedConnectionEntity.datasetNameIndex()))
                .getStringValue();
        String outputType =
                ((AString) feedConnectionRecord.getValueByPos(feedConnectionEntity.outputTypeIndex())).getStringValue();
        String policyName =
                ((AString) feedConnectionRecord.getValueByPos(feedConnectionEntity.policyIndex())).getStringValue();
        List<FunctionSignature> appliedFunctions = null;
        Object o = feedConnectionRecord.getValueByPos(feedConnectionEntity.appliedFunctionsIndex());
        IACursor cursor;

        if (!(o instanceof ANull) && !(o instanceof AMissing)) {
            appliedFunctions = new ArrayList<>();
            AUnorderedList afList =
                    (AUnorderedList) feedConnectionRecord.getValueByPos(feedConnectionEntity.appliedFunctionsIndex());
            cursor = afList.getCursor();
            while (cursor.next()) {
                //TODO(DB): deal with database
                String afValue = ((AString) cursor.get()).getStringValue();
                int pos = afValue.lastIndexOf('.'); //TODO(MULTI_PART_DATAVERSE_NAME):REVISIT
                String afDataverseCanonicalName = afValue.substring(0, pos);
                String afName = afValue.substring(pos + 1);
                DataverseName afDataverseName = DataverseName.createFromCanonicalForm(afDataverseCanonicalName);
                String afDatabaseName = MetadataUtil.databaseFor(afDataverseName);
                FunctionSignature functionSignature = new FunctionSignature(afDatabaseName, afDataverseName, afName, 1);
                appliedFunctions.add(functionSignature);
            }
        }

        int whereClauseIdx = feedConnectionRecord.getType().getFieldIndex(MetadataRecordTypes.FIELD_NAME_WHERE_CLAUSE);
        String whereClauseBody = whereClauseIdx >= 0
                ? ((AString) feedConnectionRecord.getValueByPos(whereClauseIdx)).getStringValue() : "";

        return new FeedConnection(databaseName, dataverseName, feedName, datasetName, appliedFunctions, policyName,
                whereClauseBody, outputType);
    }

    @Override
    public ITupleReference getTupleFromMetadataEntity(FeedConnection feedConnection) throws HyracksDataException {
        String dataverseCanonicalName = feedConnection.getDataverseName().getCanonicalForm();

        tupleBuilder.reset();

        if (feedConnectionEntity.databaseNameIndex() >= 0) {
            aString.setValue(feedConnection.getDatabaseName());
            stringSerde.serialize(aString, tupleBuilder.getDataOutput());
            tupleBuilder.addFieldEndOffset();
        }
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

        recordBuilder.reset(feedConnectionEntity.getRecordType());

        if (feedConnectionEntity.databaseNameIndex() >= 0) {
            fieldValue.reset();
            aString.setValue(feedConnection.getDatabaseName());
            stringSerde.serialize(aString, fieldValue.getDataOutput());
            recordBuilder.addField(feedConnectionEntity.databaseNameIndex(), fieldValue);
        }

        // field dataverse
        fieldValue.reset();
        aString.setValue(dataverseCanonicalName);
        stringSerde.serialize(aString, fieldValue.getDataOutput());
        recordBuilder.addField(feedConnectionEntity.dataverseNameIndex(), fieldValue);

        // field: feedId
        fieldValue.reset();
        aString.setValue(feedConnection.getFeedName());
        stringSerde.serialize(aString, fieldValue.getDataOutput());
        recordBuilder.addField(feedConnectionEntity.feedNameIndex(), fieldValue);

        // field: dataset
        fieldValue.reset();
        aString.setValue(feedConnection.getDatasetName());
        stringSerde.serialize(aString, fieldValue.getDataOutput());
        recordBuilder.addField(feedConnectionEntity.datasetNameIndex(), fieldValue);

        // field: outputType
        fieldValue.reset();
        aString.setValue(feedConnection.getOutputType());
        stringSerde.serialize(aString, fieldValue.getDataOutput());
        recordBuilder.addField(feedConnectionEntity.outputTypeIndex(), fieldValue);

        // field: appliedFunctions
        fieldValue.reset();
        writeAppliedFunctionsField(recordBuilder, feedConnection, fieldValue);

        // field: policyName
        fieldValue.reset();
        aString.setValue(feedConnection.getPolicyName());
        stringSerde.serialize(aString, fieldValue.getDataOutput());
        recordBuilder.addField(feedConnectionEntity.policyIndex(), fieldValue);

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

        listBuilder.reset((AUnorderedListType) feedConnectionEntity.getRecordType().getFieldTypes()[feedConnectionEntity
                .appliedFunctionsIndex()]);
        if (fc.getAppliedFunctions() != null) {
            List<FunctionSignature> appliedFunctions = fc.getAppliedFunctions();
            for (FunctionSignature af : appliedFunctions) {
                //TODO(DB): deal with database
                listEleBuffer.reset();
                aString.setValue(af.getDataverseName().getCanonicalForm() + '.' + af.getName()); //TODO(MULTI_PART_DATAVERSE_NAME):REVISIT
                stringSerde.serialize(aString, listEleBuffer.getDataOutput());
                listBuilder.addItem(listEleBuffer);
            }
        }
        listBuilder.write(buffer.getDataOutput(), true);
        rb.addField(feedConnectionEntity.appliedFunctionsIndex(), buffer);
    }
}
