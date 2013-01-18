/*
 * Copyright 2009-2010 by The Regents of the University of California
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
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import edu.uci.ics.asterix.builders.IARecordBuilder;
import edu.uci.ics.asterix.builders.RecordBuilder;
import edu.uci.ics.asterix.common.config.DatasetConfig;
import edu.uci.ics.asterix.common.config.DatasetConfig.DatasetType;
import edu.uci.ics.asterix.common.functions.FunctionSignature;
import edu.uci.ics.asterix.formats.nontagged.AqlSerializerDeserializerProvider;
import edu.uci.ics.asterix.metadata.IDatasetDetails;
import edu.uci.ics.asterix.metadata.bootstrap.MetadataPrimaryIndexes;
import edu.uci.ics.asterix.metadata.bootstrap.MetadataRecordTypes;
import edu.uci.ics.asterix.metadata.entities.Dataset;
import edu.uci.ics.asterix.metadata.entities.ExternalDatasetDetails;
import edu.uci.ics.asterix.metadata.entities.FeedDatasetDetails;
import edu.uci.ics.asterix.metadata.entities.InternalDatasetDetails;
import edu.uci.ics.asterix.metadata.entities.InternalDatasetDetails.FileStructure;
import edu.uci.ics.asterix.metadata.entities.InternalDatasetDetails.PartitioningStrategy;
import edu.uci.ics.asterix.om.base.AInt32;
import edu.uci.ics.asterix.om.base.AMutableInt32;
import edu.uci.ics.asterix.om.base.AMutableString;
import edu.uci.ics.asterix.om.base.AOrderedList;
import edu.uci.ics.asterix.om.base.ARecord;
import edu.uci.ics.asterix.om.base.AString;
import edu.uci.ics.asterix.om.base.IACursor;
import edu.uci.ics.asterix.om.types.BuiltinType;
import edu.uci.ics.hyracks.api.dataflow.value.ISerializerDeserializer;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.data.std.util.ArrayBackedValueStorage;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.ITupleReference;
import edu.uci.ics.hyracks.dataflow.std.file.FileSplit;

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

    private FileSplit[] splits;
    private List<String> partitioningKey;
    private List<String> primaryKey;
    @SuppressWarnings("unchecked")
    private ISerializerDeserializer<ARecord> recordSerDes = AqlSerializerDeserializerProvider.INSTANCE
            .getSerializerDeserializer(MetadataRecordTypes.DATASET_RECORDTYPE);
    private AMutableInt32 aInt32;
    protected ISerializerDeserializer<AInt32> aInt32Serde;

    @SuppressWarnings("unchecked")
    public DatasetTupleTranslator(boolean getTuple) {
        super(getTuple, MetadataPrimaryIndexes.DATASET_DATASET.getFieldCount());
        aInt32 = new AMutableInt32(-1);
        aInt32Serde = AqlSerializerDeserializerProvider.INSTANCE.getSerializerDeserializer(BuiltinType.AINT32);
    }

    @Override
    public Dataset getMetadataEntytiFromTuple(ITupleReference frameTuple) throws IOException {
        byte[] serRecord = frameTuple.getFieldData(DATASET_PAYLOAD_TUPLE_FIELD_INDEX);
        int recordStartOffset = frameTuple.getFieldStart(DATASET_PAYLOAD_TUPLE_FIELD_INDEX);
        int recordLength = frameTuple.getFieldLength(DATASET_PAYLOAD_TUPLE_FIELD_INDEX);
        ByteArrayInputStream stream = new ByteArrayInputStream(serRecord, recordStartOffset, recordLength);
        DataInput in = new DataInputStream(stream);
        ARecord datasetRecord = (ARecord) recordSerDes.deserialize(in);
        return createDatasetFromARecord(datasetRecord);
    }

    private Dataset createDatasetFromARecord(ARecord datasetRecord) {

        String dataverseName = ((AString) datasetRecord
                .getValueByPos(MetadataRecordTypes.DATASET_ARECORD_DATAVERSENAME_FIELD_INDEX)).getStringValue();
        String datasetName = ((AString) datasetRecord
                .getValueByPos(MetadataRecordTypes.DATASET_ARECORD_DATASETNAME_FIELD_INDEX)).getStringValue();
        String typeName = ((AString) datasetRecord
                .getValueByPos(MetadataRecordTypes.DATASET_ARECORD_DATATYPENAME_FIELD_INDEX)).getStringValue();
        DatasetType datasetType = DatasetType.valueOf(((AString) datasetRecord.getValueByPos(3)).getStringValue());
        IDatasetDetails datasetDetails = null;
        int datasetId = ((AInt32) datasetRecord
                .getValueByPos(MetadataRecordTypes.DATASET_ARECORD_DATASETID_FIELD_INDEX)).getIntegerValue();
        int pendingOp = ((AInt32) datasetRecord
                .getValueByPos(MetadataRecordTypes.DATASET_ARECORD_PENDINGOP_FIELD_INDEX)).getIntegerValue();
        switch (datasetType) {
            case FEED:
            case INTERNAL: {
                ARecord datasetDetailsRecord = (ARecord) datasetRecord
                        .getValueByPos(MetadataRecordTypes.DATASET_ARECORD_INTERNALDETAILS_FIELD_INDEX);
                FileStructure fileStructure = FileStructure.valueOf(((AString) datasetDetailsRecord
                        .getValueByPos(MetadataRecordTypes.INTERNAL_DETAILS_ARECORD_FILESTRUCTURE_FIELD_INDEX))
                        .getStringValue());
                PartitioningStrategy partitioningStrategy = PartitioningStrategy
                        .valueOf(((AString) datasetDetailsRecord
                                .getValueByPos(MetadataRecordTypes.INTERNAL_DETAILS_ARECORD_PARTITIONSTRATEGY_FIELD_INDEX))
                                .getStringValue());
                IACursor cursor = ((AOrderedList) datasetDetailsRecord
                        .getValueByPos(MetadataRecordTypes.INTERNAL_DETAILS_ARECORD_PARTITIONKEY_FIELD_INDEX))
                        .getCursor();
                List<String> partitioningKey = new ArrayList<String>();
                while (cursor.next())
                    partitioningKey.add(((AString) cursor.get()).getStringValue());
                String groupName = ((AString) datasetDetailsRecord
                        .getValueByPos(MetadataRecordTypes.INTERNAL_DETAILS_ARECORD_GROUPNAME_FIELD_INDEX))
                        .getStringValue();

                if (datasetType == DatasetConfig.DatasetType.INTERNAL) {
                    datasetDetails = new InternalDatasetDetails(fileStructure, partitioningStrategy, partitioningKey,
                            partitioningKey, groupName);
                } else {
                    String adapter = ((AString) datasetDetailsRecord
                            .getValueByPos(MetadataRecordTypes.FEED_DETAILS_ARECORD_DATASOURCE_ADAPTER_FIELD_INDEX))
                            .getStringValue();
                    cursor = ((AOrderedList) datasetDetailsRecord
                            .getValueByPos(MetadataRecordTypes.FEED_DETAILS_ARECORD_PROPERTIES_FIELD_INDEX))
                            .getCursor();
                    Map<String, String> properties = new HashMap<String, String>();
                    String key;
                    String value;
                    while (cursor.next()) {
                        ARecord field = (ARecord) cursor.get();
                        key = ((AString) field
                                .getValueByPos(MetadataRecordTypes.DATASOURCE_ADAPTER_PROPERTIES_ARECORD_NAME_FIELD_INDEX))
                                .getStringValue();
                        value = ((AString) field
                                .getValueByPos(MetadataRecordTypes.DATASOURCE_ADAPTER_PROPERTIES_ARECORD_VALUE_FIELD_INDEX))
                                .getStringValue();
                        properties.put(key, value);
                    }

                    String functionIdentifier = ((AString) datasetDetailsRecord
                            .getValueByPos(MetadataRecordTypes.FEED_DETAILS_ARECORD_FUNCTION_FIELD_INDEX))
                            .getStringValue();
                    String[] nameComponents1 = functionIdentifier.split(".");
                    String functionDataverse = nameComponents1[0];
                    String[] nameComponents2 = nameComponents1[1].split("@");
                    String functionName = nameComponents2[0];
                    FunctionSignature signature = new FunctionSignature(functionDataverse, functionName,
                            Integer.parseInt(nameComponents2[1]));

                    String feedState = ((AString) datasetDetailsRecord
                            .getValueByPos(MetadataRecordTypes.FEED_DETAILS_ARECORD_STATE_FIELD_INDEX))
                            .getStringValue();

                    datasetDetails = new FeedDatasetDetails(fileStructure, partitioningStrategy, partitioningKey,
                            partitioningKey, groupName, adapter, properties, signature, feedState);

                }
                break;
            }

            case EXTERNAL:
                ARecord datasetDetailsRecord = (ARecord) datasetRecord
                        .getValueByPos(MetadataRecordTypes.DATASET_ARECORD_EXTERNALDETAILS_FIELD_INDEX);
                String adapter = ((AString) datasetDetailsRecord
                        .getValueByPos(MetadataRecordTypes.EXTERNAL_DETAILS_ARECORD_DATASOURCE_ADAPTER_FIELD_INDEX))
                        .getStringValue();
                IACursor cursor = ((AOrderedList) datasetDetailsRecord
                        .getValueByPos(MetadataRecordTypes.EXTERNAL_DETAILS_ARECORD_PROPERTIES_FIELD_INDEX))
                        .getCursor();
                Map<String, String> properties = new HashMap<String, String>();
                String key;
                String value;
                while (cursor.next()) {
                    ARecord field = (ARecord) cursor.get();
                    key = ((AString) field
                            .getValueByPos(MetadataRecordTypes.DATASOURCE_ADAPTER_PROPERTIES_ARECORD_NAME_FIELD_INDEX))
                            .getStringValue();
                    value = ((AString) field
                            .getValueByPos(MetadataRecordTypes.DATASOURCE_ADAPTER_PROPERTIES_ARECORD_VALUE_FIELD_INDEX))
                            .getStringValue();
                    properties.put(key, value);
                }
                datasetDetails = new ExternalDatasetDetails(adapter, properties);
        }
        return new Dataset(dataverseName, datasetName, typeName, datasetDetails, datasetType, datasetId, pendingOp);
    }

    @Override
    public ITupleReference getTupleFromMetadataEntity(Dataset dataset) throws IOException {
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
        aString.setValue(dataset.getItemTypeName());
        stringSerde.serialize(aString, fieldValue.getDataOutput());
        recordBuilder.addField(MetadataRecordTypes.DATASET_ARECORD_DATATYPENAME_FIELD_INDEX, fieldValue);

        // write field 3
        fieldValue.reset();
        aString.setValue(dataset.getDatasetType().toString());
        stringSerde.serialize(aString, fieldValue.getDataOutput());
        recordBuilder.addField(MetadataRecordTypes.DATASET_ARECORD_DATASETTYPE_FIELD_INDEX, fieldValue);

        // write field 4/5/6
        fieldValue.reset();
        writeDatasetDetailsRecordType(recordBuilder, dataset, fieldValue.getDataOutput());

        // write field 7
        fieldValue.reset();
        aString.setValue(Calendar.getInstance().getTime().toString());
        stringSerde.serialize(aString, fieldValue.getDataOutput());
        recordBuilder.addField(MetadataRecordTypes.DATASET_ARECORD_TIMESTAMP_FIELD_INDEX, fieldValue);

        // write field 8
        fieldValue.reset();
        aInt32.setValue(dataset.getDatasetId());
        aInt32Serde.serialize(aInt32, fieldValue.getDataOutput());
        recordBuilder.addField(MetadataRecordTypes.DATASET_ARECORD_DATASETID_FIELD_INDEX, fieldValue);

        // write field 9
        fieldValue.reset();
        aInt32.setValue(dataset.getPendingOp());
        aInt32Serde.serialize(aInt32, fieldValue.getDataOutput());
        recordBuilder.addField(MetadataRecordTypes.DATASET_ARECORD_PENDINGOP_FIELD_INDEX, fieldValue);

        // write record
        recordBuilder.write(tupleBuilder.getDataOutput(), true);
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
            case FEED:
                recordBuilder.addField(MetadataRecordTypes.DATASET_ARECORD_FEEDDETAILS_FIELD_INDEX, fieldValue);
                break;
        }

    }

    public void writePropertyTypeRecord(String name, String value, DataOutput out) throws IOException {
        IARecordBuilder propertyRecordBuilder = new RecordBuilder();
        ArrayBackedValueStorage fieldValue = new ArrayBackedValueStorage();
        propertyRecordBuilder.reset(MetadataRecordTypes.DATASOURCE_ADAPTER_PROPERTIES_RECORDTYPE);

        // write field 0
        fieldValue.reset();
        aString.setValue(name);
        stringSerde.serialize(aString, fieldValue.getDataOutput());
        propertyRecordBuilder.addField(MetadataRecordTypes.DATASOURCE_ADAPTER_PROPERTIES_ARECORD_NAME_FIELD_INDEX,
                fieldValue);

        // write field 1
        fieldValue.reset();
        aString.setValue(value);
        stringSerde.serialize(aString, fieldValue.getDataOutput());
        propertyRecordBuilder.addField(MetadataRecordTypes.DATASOURCE_ADAPTER_PROPERTIES_ARECORD_VALUE_FIELD_INDEX,
                fieldValue);

        propertyRecordBuilder.write(out, true);
    }
}