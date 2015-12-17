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
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.asterix.builders.IARecordBuilder;
import org.apache.asterix.builders.OrderedListBuilder;
import org.apache.asterix.builders.RecordBuilder;
import org.apache.asterix.builders.UnorderedListBuilder;
import org.apache.asterix.common.config.DatasetConfig.DatasetType;
import org.apache.asterix.common.config.DatasetConfig.ExternalDatasetTransactionState;
import org.apache.asterix.common.exceptions.AsterixException;
import org.apache.asterix.formats.nontagged.AqlSerializerDeserializerProvider;
import org.apache.asterix.metadata.IDatasetDetails;
import org.apache.asterix.metadata.MetadataException;
import org.apache.asterix.metadata.bootstrap.MetadataPrimaryIndexes;
import org.apache.asterix.metadata.bootstrap.MetadataRecordTypes;
import org.apache.asterix.metadata.entities.Dataset;
import org.apache.asterix.metadata.entities.ExternalDatasetDetails;
import org.apache.asterix.metadata.entities.InternalDatasetDetails;
import org.apache.asterix.metadata.entities.InternalDatasetDetails.FileStructure;
import org.apache.asterix.metadata.entities.InternalDatasetDetails.PartitioningStrategy;
import org.apache.asterix.metadata.utils.DatasetUtils;
import org.apache.asterix.om.base.ABoolean;
import org.apache.asterix.om.base.ADateTime;
import org.apache.asterix.om.base.AInt32;
import org.apache.asterix.om.base.AMutableInt32;
import org.apache.asterix.om.base.AMutableString;
import org.apache.asterix.om.base.AOrderedList;
import org.apache.asterix.om.base.ARecord;
import org.apache.asterix.om.base.AString;
import org.apache.asterix.om.base.AUnorderedList;
import org.apache.asterix.om.base.IACursor;
import org.apache.asterix.om.types.AOrderedListType;
import org.apache.asterix.om.types.AUnorderedListType;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.asterix.om.types.IAType;
import org.apache.hyracks.api.dataflow.value.ISerializerDeserializer;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;

/**
 * Translates a Dataset metadata entity to an ITupleReference and vice versa.
 */
public class DatasetTupleTranslator extends AbstractTupleTranslator<Dataset> {
    // Field indexes of serialized Dataset in a tuple.
    // First key field.
    public static final int DATASET_DATAVERSENAME_TUPLE_FIELD_INDEX = 0;
    // Second key field.
    public static final int DATASET_DATASETNAME_TUPLE_FIELD_INDEX = 1;
    // Payload field containing serialized Dataset.
    public static final int DATASET_PAYLOAD_TUPLE_FIELD_INDEX = 2;

    @SuppressWarnings("unchecked")
    private final ISerializerDeserializer<ARecord> recordSerDes = AqlSerializerDeserializerProvider.INSTANCE
            .getSerializerDeserializer(MetadataRecordTypes.DATASET_RECORDTYPE);
    private final AMutableInt32 aInt32;
    protected ISerializerDeserializer<AInt32> aInt32Serde;

    @SuppressWarnings("unchecked")
    public DatasetTupleTranslator(boolean getTuple) {
        super(getTuple, MetadataPrimaryIndexes.DATASET_DATASET.getFieldCount());
        aInt32 = new AMutableInt32(-1);
        aInt32Serde = AqlSerializerDeserializerProvider.INSTANCE.getSerializerDeserializer(BuiltinType.AINT32);
    }

    @Override
    public Dataset getMetadataEntityFromTuple(ITupleReference frameTuple) throws IOException {
        byte[] serRecord = frameTuple.getFieldData(DATASET_PAYLOAD_TUPLE_FIELD_INDEX);
        int recordStartOffset = frameTuple.getFieldStart(DATASET_PAYLOAD_TUPLE_FIELD_INDEX);
        int recordLength = frameTuple.getFieldLength(DATASET_PAYLOAD_TUPLE_FIELD_INDEX);
        ByteArrayInputStream stream = new ByteArrayInputStream(serRecord, recordStartOffset, recordLength);
        DataInput in = new DataInputStream(stream);
        ARecord datasetRecord = recordSerDes.deserialize(in);
        return createDatasetFromARecord(datasetRecord);
    }

    private Dataset createDatasetFromARecord(ARecord datasetRecord) throws IOException {

        String dataverseName = ((AString) datasetRecord
                .getValueByPos(MetadataRecordTypes.DATASET_ARECORD_DATAVERSENAME_FIELD_INDEX)).getStringValue();
        String datasetName = ((AString) datasetRecord
                .getValueByPos(MetadataRecordTypes.DATASET_ARECORD_DATASETNAME_FIELD_INDEX)).getStringValue();
        String typeName = ((AString) datasetRecord
                .getValueByPos(MetadataRecordTypes.DATASET_ARECORD_DATATYPENAME_FIELD_INDEX)).getStringValue();
        String typeDataverseName = ((AString) datasetRecord
                .getValueByPos(MetadataRecordTypes.DATASET_ARECORD_DATATYPEDATAVERSENAME_FIELD_INDEX)).getStringValue();
        DatasetType datasetType = DatasetType.valueOf(
                ((AString) datasetRecord.getValueByPos(MetadataRecordTypes.DATASET_ARECORD_DATASETTYPE_FIELD_INDEX))
                        .getStringValue());
        IDatasetDetails datasetDetails = null;
        int datasetId = ((AInt32) datasetRecord
                .getValueByPos(MetadataRecordTypes.DATASET_ARECORD_DATASETID_FIELD_INDEX)).getIntegerValue();
        int pendingOp = ((AInt32) datasetRecord
                .getValueByPos(MetadataRecordTypes.DATASET_ARECORD_PENDINGOP_FIELD_INDEX)).getIntegerValue();
        String nodeGroupName = ((AString) datasetRecord
                .getValueByPos(MetadataRecordTypes.DATASET_ARECORD_GROUPNAME_FIELD_INDEX)).getStringValue();
        String compactionPolicy = ((AString) datasetRecord
                .getValueByPos(MetadataRecordTypes.DATASET_ARECORD_COMPACTION_POLICY_FIELD_INDEX)).getStringValue();
        IACursor cursor = ((AOrderedList) datasetRecord
                .getValueByPos(MetadataRecordTypes.DATASET_ARECORD_COMPACTION_POLICY_PROPERTIES_FIELD_INDEX))
                        .getCursor();
        Map<String, String> compactionPolicyProperties = new LinkedHashMap<String, String>();
        String key;
        String value;
        while (cursor.next()) {
            ARecord field = (ARecord) cursor.get();
            key = ((AString) field.getValueByPos(MetadataRecordTypes.PROPERTIES_NAME_FIELD_INDEX)).getStringValue();
            value = ((AString) field.getValueByPos(MetadataRecordTypes.PROPERTIES_VALUE_FIELD_INDEX)).getStringValue();
            compactionPolicyProperties.put(key, value);
        }
        switch (datasetType) {
            case INTERNAL: {
                ARecord datasetDetailsRecord = (ARecord) datasetRecord
                        .getValueByPos(MetadataRecordTypes.DATASET_ARECORD_INTERNALDETAILS_FIELD_INDEX);
                FileStructure fileStructure = FileStructure.valueOf(((AString) datasetDetailsRecord
                        .getValueByPos(MetadataRecordTypes.INTERNAL_DETAILS_ARECORD_FILESTRUCTURE_FIELD_INDEX))
                                .getStringValue());
                PartitioningStrategy partitioningStrategy = PartitioningStrategy.valueOf(((AString) datasetDetailsRecord
                        .getValueByPos(MetadataRecordTypes.INTERNAL_DETAILS_ARECORD_PARTITIONSTRATEGY_FIELD_INDEX))
                                .getStringValue());
                cursor = ((AOrderedList) datasetDetailsRecord
                        .getValueByPos(MetadataRecordTypes.INTERNAL_DETAILS_ARECORD_PARTITIONKEY_FIELD_INDEX))
                                .getCursor();
                List<List<String>> partitioningKey = new ArrayList<List<String>>();
                List<IAType> partitioningKeyType = new ArrayList<IAType>();

                AOrderedList fieldNameList;
                while (cursor.next()) {
                    fieldNameList = (AOrderedList) cursor.get();
                    IACursor nestedFieldNameCursor = (fieldNameList.getCursor());
                    List<String> nestedFieldName = new ArrayList<String>();
                    while (nestedFieldNameCursor.next()) {
                        nestedFieldName.add(((AString) nestedFieldNameCursor.get()).getStringValue());
                    }
                    partitioningKey.add(nestedFieldName);
                    partitioningKeyType.add(BuiltinType.ASTRING);
                }

                boolean autogenerated = ((ABoolean) datasetDetailsRecord
                        .getValueByPos(MetadataRecordTypes.INTERNAL_DETAILS_ARECORD_AUTOGENERATED_FIELD_INDEX))
                                .getBoolean();

                // Check if there is a filter field.
                List<String> filterField = null;
                int filterFieldPos = datasetDetailsRecord.getType()
                        .getFieldIndex(InternalDatasetDetails.FILTER_FIELD_NAME);
                if (filterFieldPos >= 0) {
                    filterField = new ArrayList<String>();
                    cursor = ((AOrderedList) datasetDetailsRecord.getValueByPos(filterFieldPos)).getCursor();
                    while (cursor.next()) {
                        filterField.add(((AString) cursor.get()).getStringValue());
                    }
                }

                // Temporary dataset only lives in the compiler therefore the temp field is false.
                //  DatasetTupleTranslator always read from the metadata node, so the temp flag should be always false.
                datasetDetails = new InternalDatasetDetails(fileStructure, partitioningStrategy, partitioningKey,
                        partitioningKey, partitioningKeyType, autogenerated, filterField, false);
                break;
            }

            case EXTERNAL:
                ARecord datasetDetailsRecord = (ARecord) datasetRecord
                        .getValueByPos(MetadataRecordTypes.DATASET_ARECORD_EXTERNALDETAILS_FIELD_INDEX);
                String adapter = ((AString) datasetDetailsRecord
                        .getValueByPos(MetadataRecordTypes.EXTERNAL_DETAILS_ARECORD_DATASOURCE_ADAPTER_FIELD_INDEX))
                                .getStringValue();
                cursor = ((AOrderedList) datasetDetailsRecord
                        .getValueByPos(MetadataRecordTypes.EXTERNAL_DETAILS_ARECORD_PROPERTIES_FIELD_INDEX))
                                .getCursor();
                Map<String, String> properties = new HashMap<String, String>();
                while (cursor.next()) {
                    ARecord field = (ARecord) cursor.get();
                    key = ((AString) field.getValueByPos(MetadataRecordTypes.PROPERTIES_NAME_FIELD_INDEX))
                            .getStringValue();
                    value = ((AString) field.getValueByPos(MetadataRecordTypes.PROPERTIES_VALUE_FIELD_INDEX))
                            .getStringValue();
                    properties.put(key, value);
                }

                // Timestamp
                Date timestamp = new Date((((ADateTime) datasetDetailsRecord
                        .getValueByPos(MetadataRecordTypes.EXTERNAL_DETAILS_ARECORD_LAST_REFRESH_TIME_FIELD_INDEX)))
                                .getChrononTime());
                // State
                ExternalDatasetTransactionState state = ExternalDatasetTransactionState
                        .values()[((AInt32) datasetDetailsRecord.getValueByPos(
                                MetadataRecordTypes.EXTERNAL_DETAILS_ARECORD_TRANSACTION_STATE_FIELD_INDEX))
                                        .getIntegerValue()];

                datasetDetails = new ExternalDatasetDetails(adapter, properties, timestamp, state);
        }

        Map<String, String> hints = getDatasetHints(datasetRecord);

        return new Dataset(dataverseName, datasetName, typeDataverseName, typeName, nodeGroupName, compactionPolicy,
                compactionPolicyProperties, datasetDetails, hints, datasetType, datasetId, pendingOp);
    }

    @Override
    public ITupleReference getTupleFromMetadataEntity(Dataset dataset) throws IOException, MetadataException {
        OrderedListBuilder listBuilder = new OrderedListBuilder();
        ArrayBackedValueStorage itemValue = new ArrayBackedValueStorage();
        // write the key in the first 2 fields of the tuple
        tupleBuilder.reset();
        aString.setValue(dataset.getDataverseName());
        stringSerde.serialize(aString, tupleBuilder.getDataOutput());
        tupleBuilder.addFieldEndOffset();
        aString.setValue(dataset.getDatasetName());
        stringSerde.serialize(aString, tupleBuilder.getDataOutput());
        tupleBuilder.addFieldEndOffset();

        // write the pay-load in the third field of the tuple

        recordBuilder.reset(MetadataRecordTypes.DATASET_RECORDTYPE);

        // write field 0
        fieldValue.reset();
        aString.setValue(dataset.getDataverseName());
        stringSerde.serialize(aString, fieldValue.getDataOutput());
        recordBuilder.addField(MetadataRecordTypes.DATASET_ARECORD_DATAVERSENAME_FIELD_INDEX, fieldValue);

        // write field 1
        fieldValue.reset();
        aString.setValue(dataset.getDatasetName());
        stringSerde.serialize(aString, fieldValue.getDataOutput());
        recordBuilder.addField(MetadataRecordTypes.DATASET_ARECORD_DATASETNAME_FIELD_INDEX, fieldValue);

        // write field 2
        fieldValue.reset();
        aString.setValue(dataset.getItemTypeDataverseName());
        stringSerde.serialize(aString, fieldValue.getDataOutput());
        recordBuilder.addField(MetadataRecordTypes.DATASET_ARECORD_DATATYPEDATAVERSENAME_FIELD_INDEX, fieldValue);

        // write field 3
        fieldValue.reset();
        aString.setValue(dataset.getItemTypeName());
        stringSerde.serialize(aString, fieldValue.getDataOutput());
        recordBuilder.addField(MetadataRecordTypes.DATASET_ARECORD_DATATYPENAME_FIELD_INDEX, fieldValue);

        // write field 4
        fieldValue.reset();
        aString.setValue(dataset.getDatasetType().toString());
        stringSerde.serialize(aString, fieldValue.getDataOutput());
        recordBuilder.addField(MetadataRecordTypes.DATASET_ARECORD_DATASETTYPE_FIELD_INDEX, fieldValue);

        // write field 5
        fieldValue.reset();
        aString.setValue(dataset.getNodeGroupName());
        stringSerde.serialize(aString, fieldValue.getDataOutput());
        recordBuilder.addField(MetadataRecordTypes.DATASET_ARECORD_GROUPNAME_FIELD_INDEX, fieldValue);

        // write field 6
        fieldValue.reset();
        aString.setValue(dataset.getCompactionPolicy());
        stringSerde.serialize(aString, fieldValue.getDataOutput());
        recordBuilder.addField(MetadataRecordTypes.DATASET_ARECORD_COMPACTION_POLICY_FIELD_INDEX, fieldValue);

        // write field 7
        listBuilder.reset((AOrderedListType) MetadataRecordTypes.DATASET_RECORDTYPE
                .getFieldTypes()[MetadataRecordTypes.DATASET_ARECORD_COMPACTION_POLICY_PROPERTIES_FIELD_INDEX]);
        if (dataset.getCompactionPolicyProperties() != null) {
            for (Map.Entry<String, String> property : dataset.getCompactionPolicyProperties().entrySet()) {
                String name = property.getKey();
                String value = property.getValue();
                itemValue.reset();
                DatasetUtils.writePropertyTypeRecord(name, value, itemValue.getDataOutput(),
                        MetadataRecordTypes.COMPACTION_POLICY_PROPERTIES_RECORDTYPE);
                listBuilder.addItem(itemValue);
            }
        }
        fieldValue.reset();
        listBuilder.write(fieldValue.getDataOutput(), true);
        recordBuilder.addField(MetadataRecordTypes.DATASET_ARECORD_COMPACTION_POLICY_PROPERTIES_FIELD_INDEX,
                fieldValue);

        // write field 8/9
        fieldValue.reset();
        writeDatasetDetailsRecordType(recordBuilder, dataset, fieldValue.getDataOutput());

        // write field 10
        UnorderedListBuilder uListBuilder = new UnorderedListBuilder();
        uListBuilder.reset((AUnorderedListType) MetadataRecordTypes.DATASET_RECORDTYPE
                .getFieldTypes()[MetadataRecordTypes.DATASET_ARECORD_HINTS_FIELD_INDEX]);
        for (Map.Entry<String, String> property : dataset.getHints().entrySet()) {
            String name = property.getKey();
            String value = property.getValue();
            itemValue.reset();
            writeDatasetHintRecord(name, value, itemValue.getDataOutput());
            uListBuilder.addItem(itemValue);
        }
        fieldValue.reset();
        uListBuilder.write(fieldValue.getDataOutput(), true);
        recordBuilder.addField(MetadataRecordTypes.DATASET_ARECORD_HINTS_FIELD_INDEX, fieldValue);

        // write field 11
        fieldValue.reset();
        aString.setValue(Calendar.getInstance().getTime().toString());
        stringSerde.serialize(aString, fieldValue.getDataOutput());
        recordBuilder.addField(MetadataRecordTypes.DATASET_ARECORD_TIMESTAMP_FIELD_INDEX, fieldValue);

        // write field 12
        fieldValue.reset();
        aInt32.setValue(dataset.getDatasetId());
        aInt32Serde.serialize(aInt32, fieldValue.getDataOutput());
        recordBuilder.addField(MetadataRecordTypes.DATASET_ARECORD_DATASETID_FIELD_INDEX, fieldValue);

        // write field 13
        fieldValue.reset();
        aInt32.setValue(dataset.getPendingOp());
        aInt32Serde.serialize(aInt32, fieldValue.getDataOutput());
        recordBuilder.addField(MetadataRecordTypes.DATASET_ARECORD_PENDINGOP_FIELD_INDEX, fieldValue);

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

    private void writeDatasetDetailsRecordType(IARecordBuilder recordBuilder, Dataset dataset, DataOutput dataOutput)
            throws HyracksDataException {

        dataset.getDatasetDetails().writeDatasetDetailsRecordType(fieldValue.getDataOutput());
        switch (dataset.getDatasetType()) {
            case INTERNAL:
                recordBuilder.addField(MetadataRecordTypes.DATASET_ARECORD_INTERNALDETAILS_FIELD_INDEX, fieldValue);
                break;
            case EXTERNAL:
                recordBuilder.addField(MetadataRecordTypes.DATASET_ARECORD_EXTERNALDETAILS_FIELD_INDEX, fieldValue);
                break;
        }

    }

    private Map<String, String> getDatasetHints(ARecord datasetRecord) {
        Map<String, String> hints = new HashMap<String, String>();
        String key;
        String value;
        AUnorderedList list = (AUnorderedList) datasetRecord
                .getValueByPos(MetadataRecordTypes.DATASET_ARECORD_HINTS_FIELD_INDEX);
        IACursor cursor = list.getCursor();
        while (cursor.next()) {
            ARecord field = (ARecord) cursor.get();
            key = ((AString) field.getValueByPos(MetadataRecordTypes.PROPERTIES_NAME_FIELD_INDEX)).getStringValue();
            value = ((AString) field.getValueByPos(MetadataRecordTypes.PROPERTIES_VALUE_FIELD_INDEX)).getStringValue();
            hints.put(key, value);
        }
        return hints;
    }

    @SuppressWarnings("unchecked")
    private void writeDatasetHintRecord(String name, String value, DataOutput out) throws HyracksDataException {
        IARecordBuilder propertyRecordBuilder = new RecordBuilder();
        ArrayBackedValueStorage fieldValue = new ArrayBackedValueStorage();
        propertyRecordBuilder.reset(MetadataRecordTypes.DATASET_HINTS_RECORDTYPE);
        AMutableString aString = new AMutableString("");
        ISerializerDeserializer<AString> stringSerde = AqlSerializerDeserializerProvider.INSTANCE
                .getSerializerDeserializer(BuiltinType.ASTRING);

        // write field 0
        fieldValue.reset();
        aString.setValue(name);
        stringSerde.serialize(aString, fieldValue.getDataOutput());
        propertyRecordBuilder.addField(0, fieldValue);

        // write field 1
        fieldValue.reset();
        aString.setValue(value);
        stringSerde.serialize(aString, fieldValue.getDataOutput());
        propertyRecordBuilder.addField(1, fieldValue);

        try {
            propertyRecordBuilder.write(out, true);
        } catch (IOException | AsterixException ioe) {
            throw new HyracksDataException(ioe);
        }
    }

}
