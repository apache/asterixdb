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
import java.util.ArrayList;
import java.util.List;

import org.apache.asterix.builders.IARecordBuilder;
import org.apache.asterix.builders.UnorderedListBuilder;
import org.apache.asterix.common.functions.FunctionSignature;
import org.apache.asterix.formats.nontagged.SerializerDeserializerProvider;
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
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.api.dataflow.value.ISerializerDeserializer;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;

public class FeedConnectionTupleTranslator extends AbstractTupleTranslator<FeedConnection> {
    private static final long serialVersionUID = -1798961999812829511L;

    public static final int FEED_CONN_DATAVERSE_NAME_FIELD_INDEX = 0;
    public static final int FEED_CONN_FEED_NAME_FIELD_INDEX = 1;
    public static final int FEED_CONN_DATASET_NAME_FIELD_INDEX = 2;

    public static final int FEED_CONN_PAYLOAD_TUPLE_FIELD_INDEX = 3;

    protected final transient ArrayBackedValueStorage fieldName = new ArrayBackedValueStorage();

    private ISerializerDeserializer<ARecord> recordSerDes = SerializerDeserializerProvider.INSTANCE
            .getSerializerDeserializer(MetadataRecordTypes.FEED_CONNECTION_RECORDTYPE);

    public FeedConnectionTupleTranslator(boolean getTuple) {
        super(getTuple, MetadataPrimaryIndexes.FEED_CONNECTION_DATASET.getFieldCount());
    }

    @Override
    public FeedConnection getMetadataEntityFromTuple(ITupleReference frameTuple)
            throws AlgebricksException, HyracksDataException {
        byte[] serRecord = frameTuple.getFieldData(FEED_CONN_PAYLOAD_TUPLE_FIELD_INDEX);
        int recordStartOffset = frameTuple.getFieldStart(FEED_CONN_PAYLOAD_TUPLE_FIELD_INDEX);
        int recordLength = frameTuple.getFieldLength(FEED_CONN_PAYLOAD_TUPLE_FIELD_INDEX);
        ByteArrayInputStream stream = new ByteArrayInputStream(serRecord, recordStartOffset, recordLength);
        DataInput in = new DataInputStream(stream);
        ARecord feedConnRecord = recordSerDes.deserialize(in);
        return createFeedConnFromRecord(feedConnRecord);
    }

    private FeedConnection createFeedConnFromRecord(ARecord feedConnRecord) {
        String dataverseName =
                ((AString) feedConnRecord.getValueByPos(MetadataRecordTypes.FEED_CONN_DATAVERSE_NAME_FIELD_INDEX))
                        .getStringValue();
        String feedName = ((AString) feedConnRecord.getValueByPos(MetadataRecordTypes.FEED_CONN_FEED_NAME_FIELD_INDEX))
                .getStringValue();
        String datasetName =
                ((AString) feedConnRecord.getValueByPos(MetadataRecordTypes.FEED_CONN_DATASET_NAME_FIELD_INDEX))
                        .getStringValue();
        String outputType = ((AString) feedConnRecord.getValueByPos(MetadataRecordTypes.FEED_CONN_OUTPUT_TYPE_INDEX))
                .getStringValue();
        String policyName = ((AString) feedConnRecord.getValueByPos(MetadataRecordTypes.FEED_CONN_POLICY_FIELD_INDEX))
                .getStringValue();
        ArrayList<FunctionSignature> appliedFunctions = null;
        Object o = feedConnRecord.getValueByPos(MetadataRecordTypes.FEED_CONN_APPLIED_FUNCTIONS_FIELD_INDEX);
        IACursor cursor;

        if (!(o instanceof ANull) && !(o instanceof AMissing)) {
            appliedFunctions = new ArrayList<>();
            FunctionSignature functionSignature;
            cursor = ((AUnorderedList) feedConnRecord
                    .getValueByPos(MetadataRecordTypes.FEED_CONN_APPLIED_FUNCTIONS_FIELD_INDEX)).getCursor();
            while (cursor.next()) {
                String[] functionFullName = ((AString) cursor.get()).getStringValue().split("\\.");
                functionSignature = new FunctionSignature(functionFullName[0], functionFullName[1], 1);
                appliedFunctions.add(functionSignature);
            }
        }

        int whereClauseIdx = feedConnRecord.getType().getFieldIndex(MetadataRecordTypes.FIELD_NAME_WHERE_CLAUSE);
        String whereClauseBody =
                whereClauseIdx >= 0 ? ((AString) feedConnRecord.getValueByPos(whereClauseIdx)).getStringValue() : "";

        return new FeedConnection(dataverseName, feedName, datasetName, appliedFunctions, policyName, whereClauseBody,
                outputType);
    }

    @Override
    public ITupleReference getTupleFromMetadataEntity(FeedConnection me)
            throws AlgebricksException, HyracksDataException {
        tupleBuilder.reset();

        // key: dataverse
        aString.setValue(me.getDataverseName());
        stringSerde.serialize(aString, tupleBuilder.getDataOutput());
        tupleBuilder.addFieldEndOffset();

        // key: feedName
        aString.setValue(me.getFeedName());
        stringSerde.serialize(aString, tupleBuilder.getDataOutput());
        tupleBuilder.addFieldEndOffset();

        // key: dataset
        aString.setValue(me.getDatasetName());
        stringSerde.serialize(aString, tupleBuilder.getDataOutput());
        tupleBuilder.addFieldEndOffset();

        recordBuilder.reset(MetadataRecordTypes.FEED_CONNECTION_RECORDTYPE);
        // field dataverse
        fieldValue.reset();
        aString.setValue(me.getDataverseName());
        stringSerde.serialize(aString, fieldValue.getDataOutput());
        recordBuilder.addField(MetadataRecordTypes.FEED_CONN_DATAVERSE_NAME_FIELD_INDEX, fieldValue);

        // field: feedId
        fieldValue.reset();
        aString.setValue(me.getFeedName());
        stringSerde.serialize(aString, fieldValue.getDataOutput());
        recordBuilder.addField(MetadataRecordTypes.FEED_CONN_FEED_NAME_FIELD_INDEX, fieldValue);

        // field: dataset
        fieldValue.reset();
        aString.setValue(me.getDatasetName());
        stringSerde.serialize(aString, fieldValue.getDataOutput());
        recordBuilder.addField(MetadataRecordTypes.FEED_CONN_DATASET_NAME_FIELD_INDEX, fieldValue);

        // field: outputType
        fieldValue.reset();
        aString.setValue(me.getOutputType());
        stringSerde.serialize(aString, fieldValue.getDataOutput());
        recordBuilder.addField(MetadataRecordTypes.FEED_CONN_OUTPUT_TYPE_INDEX, fieldValue);

        // field: appliedFunctions
        fieldValue.reset();
        writeAppliedFunctionsField(recordBuilder, me, fieldValue);

        // field: policyName
        fieldValue.reset();
        aString.setValue(me.getPolicyName());
        stringSerde.serialize(aString, fieldValue.getDataOutput());
        recordBuilder.addField(MetadataRecordTypes.FEED_CONN_POLICY_FIELD_INDEX, fieldValue);

        // field: whereClauseBody
        writeOpenPart(me);

        recordBuilder.write(tupleBuilder.getDataOutput(), true);
        tupleBuilder.addFieldEndOffset();

        tuple.reset(tupleBuilder.getFieldEndOffsets(), tupleBuilder.getByteArray());
        return tuple;
    }

    protected void writeOpenPart(FeedConnection fc) throws HyracksDataException {
        if (fc.getWhereClauseBody() != null && fc.getWhereClauseBody().length() > 0) {
            fieldName.reset();
            aString.setValue(MetadataRecordTypes.FIELD_NAME_WHERE_CLAUSE);
            stringSerde.serialize(aString, fieldName.getDataOutput());
            fieldValue.reset();
            aString.setValue(fc.getWhereClauseBody());
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
                aString.setValue(af.getNamespace() + "." + af.getName());
                stringSerde.serialize(aString, listEleBuffer.getDataOutput());
                listBuilder.addItem(listEleBuffer);
            }
        }
        listBuilder.write(buffer.getDataOutput(), true);
        rb.addField(MetadataRecordTypes.FEED_CONN_APPLIED_FUNCTIONS_FIELD_INDEX, buffer);
    }
}
