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

import java.io.DataOutput;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.asterix.builders.IARecordBuilder;
import org.apache.asterix.builders.OrderedListBuilder;
import org.apache.asterix.builders.RecordBuilder;
import org.apache.asterix.builders.UnorderedListBuilder;
import org.apache.asterix.common.config.DatasetConfig;
import org.apache.asterix.common.config.DatasetConfig.DatasetType;
import org.apache.asterix.common.config.DatasetConfig.TransactionState;
import org.apache.asterix.common.exceptions.AsterixException;
import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.common.metadata.DatasetFullyQualifiedName;
import org.apache.asterix.common.metadata.DataverseName;
import org.apache.asterix.common.metadata.DependencyFullyQualifiedName;
import org.apache.asterix.common.metadata.MetadataUtil;
import org.apache.asterix.metadata.IDatasetDetails;
import org.apache.asterix.metadata.bootstrap.DatasetEntity;
import org.apache.asterix.metadata.bootstrap.MetadataRecordTypes;
import org.apache.asterix.metadata.dataset.DatasetFormatInfo;
import org.apache.asterix.metadata.entities.Dataset;
import org.apache.asterix.metadata.entities.ExternalDatasetDetails;
import org.apache.asterix.metadata.entities.InternalDatasetDetails;
import org.apache.asterix.metadata.entities.InternalDatasetDetails.FileStructure;
import org.apache.asterix.metadata.entities.InternalDatasetDetails.PartitioningStrategy;
import org.apache.asterix.metadata.entities.ViewDetails;
import org.apache.asterix.metadata.utils.DatasetUtil;
import org.apache.asterix.om.base.ABoolean;
import org.apache.asterix.om.base.ADateTime;
import org.apache.asterix.om.base.ADouble;
import org.apache.asterix.om.base.AInt32;
import org.apache.asterix.om.base.AInt64;
import org.apache.asterix.om.base.AInt8;
import org.apache.asterix.om.base.AMutableDouble;
import org.apache.asterix.om.base.AMutableInt32;
import org.apache.asterix.om.base.AMutableInt64;
import org.apache.asterix.om.base.AMutableString;
import org.apache.asterix.om.base.AOrderedList;
import org.apache.asterix.om.base.ARecord;
import org.apache.asterix.om.base.AString;
import org.apache.asterix.om.base.AUnorderedList;
import org.apache.asterix.om.base.IACursor;
import org.apache.asterix.om.base.IAObject;
import org.apache.asterix.om.pointables.base.DefaultOpenFieldType;
import org.apache.asterix.om.types.AOrderedListType;
import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.AUnorderedListType;
import org.apache.asterix.om.types.BuiltinTypeMap;
import org.apache.asterix.om.types.IAType;
import org.apache.asterix.runtime.compression.CompressionManager;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.common.utils.Pair;
import org.apache.hyracks.algebricks.common.utils.Triple;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;

/**
 * Translates a Dataset metadata entity to an ITupleReference and vice versa.
 */
public class DatasetTupleTranslator extends AbstractTupleTranslator<Dataset> {

    protected final DatasetEntity datasetEntity;
    protected AMutableInt32 aInt32;
    protected AMutableInt64 aInt64;
    protected AMutableDouble aDouble;

    protected DatasetTupleTranslator(boolean getTuple, DatasetEntity datasetEntity) {
        super(getTuple, datasetEntity.getIndex(), datasetEntity.payloadPosition());
        this.datasetEntity = datasetEntity;
        if (getTuple) {
            aInt32 = new AMutableInt32(-1);
            aInt64 = new AMutableInt64(-1);
            aDouble = new AMutableDouble(0.0);
        }
    }

    @Override
    protected Dataset createMetadataEntityFromARecord(ARecord datasetRecord) throws AlgebricksException {
        ARecordType recType = datasetRecord.getType();

        String dataverseCanonicalName =
                ((AString) datasetRecord.getValueByPos(datasetEntity.dataverseNameIndex())).getStringValue();
        DataverseName dataverseName = DataverseName.createFromCanonicalForm(dataverseCanonicalName);
        int databaseNameIndex = datasetEntity.databaseNameIndex();
        String databaseName;
        if (databaseNameIndex >= 0) {
            databaseName = ((AString) datasetRecord.getValueByPos(databaseNameIndex)).getStringValue();
        } else {
            databaseName = MetadataUtil.databaseFor(dataverseName);
        }
        String datasetName = ((AString) datasetRecord.getValueByPos(datasetEntity.datasetNameIndex())).getStringValue();
        String typeName = ((AString) datasetRecord.getValueByPos(datasetEntity.datatypeNameIndex())).getStringValue();
        String typeDataverseCanonicalName =
                ((AString) datasetRecord.getValueByPos(datasetEntity.datatypeDataverseNameIndex())).getStringValue();
        DataverseName typeDataverseName = DataverseName.createFromCanonicalForm(typeDataverseCanonicalName);
        String itemTypeDatabaseName;
        int typeDatabaseIdx = recType.getFieldIndex(MetadataRecordTypes.FIELD_NAME_DATATYPE_DATABASE_NAME);
        if (typeDatabaseIdx >= 0) {
            itemTypeDatabaseName = ((AString) datasetRecord.getValueByPos(typeDatabaseIdx)).getStringValue();
        } else {
            itemTypeDatabaseName = MetadataUtil.databaseFor(typeDataverseName);
        }
        DatasetType datasetType = DatasetType
                .valueOf(((AString) datasetRecord.getValueByPos(datasetEntity.datasetTypeIndex())).getStringValue());
        IDatasetDetails datasetDetails = null;
        int datasetId = ((AInt32) datasetRecord.getValueByPos(datasetEntity.datasetIdIndex())).getIntegerValue();
        int pendingOp = ((AInt32) datasetRecord.getValueByPos(datasetEntity.pendingOpIndex())).getIntegerValue();
        String nodeGroupName = ((AString) datasetRecord.getValueByPos(datasetEntity.groupNameIndex())).getStringValue();

        Pair<String, Map<String, String>> compactionPolicy = readCompactionPolicy(datasetType, datasetRecord);

        switch (datasetType) {
            case INTERNAL: {
                ARecord datasetDetailsRecord =
                        (ARecord) datasetRecord.getValueByPos(datasetEntity.internalDetailsIndex());
                FileStructure fileStructure = FileStructure.valueOf(((AString) datasetDetailsRecord
                        .getValueByPos(MetadataRecordTypes.INTERNAL_DETAILS_ARECORD_FILESTRUCTURE_FIELD_INDEX))
                                .getStringValue());
                PartitioningStrategy partitioningStrategy = PartitioningStrategy.valueOf(((AString) datasetDetailsRecord
                        .getValueByPos(MetadataRecordTypes.INTERNAL_DETAILS_ARECORD_PARTITIONSTRATEGY_FIELD_INDEX))
                                .getStringValue());
                IACursor cursor = ((AOrderedList) datasetDetailsRecord
                        .getValueByPos(MetadataRecordTypes.INTERNAL_DETAILS_ARECORD_PARTITIONKEY_FIELD_INDEX))
                                .getCursor();
                List<List<String>> partitioningKey = new ArrayList<>();

                while (cursor.next()) {
                    AOrderedList fieldNameList = (AOrderedList) cursor.get();
                    IACursor nestedFieldNameCursor = (fieldNameList.getCursor());
                    List<String> nestedFieldName = new ArrayList<>();
                    while (nestedFieldNameCursor.next()) {
                        nestedFieldName.add(((AString) nestedFieldNameCursor.get()).getStringValue());
                    }
                    partitioningKey.add(nestedFieldName);
                }

                // Check if there is a primary key types field
                List<IAType> primaryKeyTypes = null;
                int primaryKeyTypesPos = datasetDetailsRecord.getType()
                        .getFieldIndex(InternalDatasetDetails.PRIMARY_KEY_TYPES_FIELD_NAME);
                if (primaryKeyTypesPos >= 0) {
                    cursor = ((AOrderedList) datasetDetailsRecord.getValueByPos(primaryKeyTypesPos)).getCursor();
                    primaryKeyTypes = new ArrayList<>();
                    while (cursor.next()) {
                        String primaryKeyTypeName = ((AString) cursor.get()).getStringValue();
                        IAType primaryKeyType = BuiltinTypeMap.getBuiltinType(primaryKeyTypeName);
                        primaryKeyTypes.add(primaryKeyType);
                    }
                }

                boolean autogenerated = ((ABoolean) datasetDetailsRecord
                        .getValueByPos(MetadataRecordTypes.INTERNAL_DETAILS_ARECORD_AUTOGENERATED_FIELD_INDEX))
                                .getBoolean();

                // check if there is a filter source indicator
                Integer filterSourceIndicator = null;
                int filterSourceIndicatorPos = datasetDetailsRecord.getType()
                        .getFieldIndex(InternalDatasetDetails.FILTER_SOURCE_INDICATOR_FIELD_NAME);
                if (filterSourceIndicatorPos >= 0) {
                    filterSourceIndicator =
                            (int) ((AInt8) datasetDetailsRecord.getValueByPos(filterSourceIndicatorPos)).getByteValue();
                }

                // Check if there is a filter field.
                List<String> filterField = null;
                int filterFieldPos =
                        datasetDetailsRecord.getType().getFieldIndex(InternalDatasetDetails.FILTER_FIELD_NAME);
                if (filterFieldPos >= 0) {
                    // backward compatibility, if a dataset contains filter field but no filter source indicator
                    // we set the indicator to 0 by default.
                    if (filterSourceIndicator == null) {
                        filterSourceIndicator = 0;
                    }
                    filterField = new ArrayList<>();
                    cursor = ((AOrderedList) datasetDetailsRecord.getValueByPos(filterFieldPos)).getCursor();
                    while (cursor.next()) {
                        filterField.add(((AString) cursor.get()).getStringValue());
                    }
                }

                // Read a field-source-indicator field.
                List<Integer> keyFieldSourceIndicator = new ArrayList<>();
                int keyFieldSourceIndicatorIndex = datasetDetailsRecord.getType()
                        .getFieldIndex(InternalDatasetDetails.KEY_FILD_SOURCE_INDICATOR_FIELD_NAME);
                if (keyFieldSourceIndicatorIndex >= 0) {
                    cursor = ((AOrderedList) datasetDetailsRecord.getValueByPos(keyFieldSourceIndicatorIndex))
                            .getCursor();
                    while (cursor.next()) {
                        keyFieldSourceIndicator.add((int) ((AInt8) cursor.get()).getByteValue());
                    }
                } else {
                    for (int index = 0; index < partitioningKey.size(); ++index) {
                        keyFieldSourceIndicator.add(0);
                    }
                }

                boolean isDatasetWithoutTypeSpec = primaryKeyTypes != null && !primaryKeyTypes.isEmpty();
                datasetDetails = new InternalDatasetDetails(fileStructure, partitioningStrategy, partitioningKey,
                        partitioningKey, keyFieldSourceIndicator, primaryKeyTypes, autogenerated, filterSourceIndicator,
                        filterField, isDatasetWithoutTypeSpec);
                break;
            }

            case EXTERNAL: {
                ARecord datasetDetailsRecord =
                        (ARecord) datasetRecord.getValueByPos(datasetEntity.externalDetailsIndex());
                String adapter = ((AString) datasetDetailsRecord
                        .getValueByPos(MetadataRecordTypes.EXTERNAL_DETAILS_ARECORD_DATASOURCE_ADAPTER_FIELD_INDEX))
                                .getStringValue();
                IACursor cursor = ((AOrderedList) datasetDetailsRecord
                        .getValueByPos(MetadataRecordTypes.EXTERNAL_DETAILS_ARECORD_PROPERTIES_FIELD_INDEX))
                                .getCursor();
                Map<String, String> properties = new HashMap<>();
                while (cursor.next()) {
                    ARecord field = (ARecord) cursor.get();
                    String key = ((AString) field.getValueByPos(MetadataRecordTypes.PROPERTIES_NAME_FIELD_INDEX))
                            .getStringValue();
                    String value = ((AString) field.getValueByPos(MetadataRecordTypes.PROPERTIES_VALUE_FIELD_INDEX))
                            .getStringValue();
                    properties.put(key, value);
                }

                // Timestamp
                Date timestamp = new Date((((ADateTime) datasetDetailsRecord
                        .getValueByPos(MetadataRecordTypes.EXTERNAL_DETAILS_ARECORD_LAST_REFRESH_TIME_FIELD_INDEX)))
                                .getChrononTime());
                // State
                TransactionState state = TransactionState.values()[((AInt32) datasetDetailsRecord
                        .getValueByPos(MetadataRecordTypes.EXTERNAL_DETAILS_ARECORD_TRANSACTION_STATE_FIELD_INDEX))
                                .getIntegerValue()];

                datasetDetails = new ExternalDatasetDetails(adapter, properties, timestamp, state);
                break;
            }
            case VIEW: {
                int datasetDetailsFieldPos = recType.getFieldIndex(MetadataRecordTypes.FIELD_NAME_VIEW_DETAILS);
                ARecord datasetDetailsRecord = (ARecord) datasetRecord.getValueByPos(datasetDetailsFieldPos);

                // Definition
                int definitionFieldPos =
                        datasetDetailsRecord.getType().getFieldIndex(MetadataRecordTypes.FIELD_NAME_DEFINITION);
                String definition = ((AString) datasetDetailsRecord.getValueByPos(definitionFieldPos)).getStringValue();

                // Dependencies
                List<List<DependencyFullyQualifiedName>> dependencies = Collections.emptyList();
                int dependenciesFieldPos =
                        datasetDetailsRecord.getType().getFieldIndex(MetadataRecordTypes.FIELD_NAME_DEPENDENCIES);
                if (dependenciesFieldPos >= 0) {
                    dependencies = new ArrayList<>();
                    IACursor dependenciesCursor =
                            ((AOrderedList) datasetDetailsRecord.getValueByPos(dependenciesFieldPos)).getCursor();
                    while (dependenciesCursor.next()) {
                        List<DependencyFullyQualifiedName> dependencyList = new ArrayList<>();
                        IACursor qualifiedDependencyCursor = ((AOrderedList) dependenciesCursor.get()).getCursor();
                        while (qualifiedDependencyCursor.next()) {
                            DependencyFullyQualifiedName dependency =
                                    getDependency((AOrderedList) qualifiedDependencyCursor.get());
                            dependencyList.add(dependency);
                        }
                        dependencies.add(dependencyList);
                    }
                }

                // Default Null
                Boolean defaultNull = null;
                int defaultFieldPos =
                        datasetDetailsRecord.getType().getFieldIndex(MetadataRecordTypes.FIELD_NAME_DEFAULT);
                if (defaultFieldPos >= 0) {
                    IAObject defaultValue = datasetDetailsRecord.getValueByPos(defaultFieldPos);
                    defaultNull = defaultValue.getType().getTypeTag() == ATypeTag.NULL;
                }

                // Primary Key
                List<String> primaryKeyFields = null;
                int primaryKeyFieldPos =
                        datasetDetailsRecord.getType().getFieldIndex(MetadataRecordTypes.FIELD_NAME_PRIMARY_KEY);
                if (primaryKeyFieldPos >= 0) {
                    AOrderedList primaryKeyFieldList =
                            ((AOrderedList) datasetDetailsRecord.getValueByPos(primaryKeyFieldPos));
                    int n = primaryKeyFieldList.size();
                    primaryKeyFields = new ArrayList<>(n);
                    for (int i = 0; i < n; i++) {
                        AOrderedList list = (AOrderedList) primaryKeyFieldList.getItem(i);
                        if (list.size() != 1) {
                            throw new AsterixException(ErrorCode.METADATA_ERROR, list.toJSON());
                        }
                        AString str = (AString) list.getItem(0);
                        primaryKeyFields.add(str.getStringValue());
                    }
                }

                // Foreign Keys
                List<ViewDetails.ForeignKey> foreignKeys = null;
                int foreignKeysFieldPos =
                        datasetDetailsRecord.getType().getFieldIndex(MetadataRecordTypes.FIELD_NAME_FOREIGN_KEYS);
                if (foreignKeysFieldPos >= 0) {
                    AOrderedList foreignKeyRecordsList =
                            ((AOrderedList) datasetDetailsRecord.getValueByPos(foreignKeysFieldPos));
                    int nForeignKeys = foreignKeyRecordsList.size();
                    foreignKeys = new ArrayList<>(nForeignKeys);
                    for (int i = 0; i < nForeignKeys; i++) {
                        ARecord foreignKeyRecord = (ARecord) foreignKeyRecordsList.getItem(i);
                        // 'ForeignKey'
                        int foreignKeyFieldPos =
                                foreignKeyRecord.getType().getFieldIndex(MetadataRecordTypes.FIELD_NAME_FOREIGN_KEY);
                        AOrderedList foreignKeyFieldList =
                                ((AOrderedList) foreignKeyRecord.getValueByPos(foreignKeyFieldPos));
                        int nForeignKeyFields = foreignKeyFieldList.size();
                        List<String> foreignKeyFields = new ArrayList<>(nForeignKeyFields);
                        for (int j = 0; j < nForeignKeyFields; j++) {
                            AOrderedList list = (AOrderedList) foreignKeyFieldList.getItem(j);
                            if (list.size() != 1) {
                                throw new AsterixException(ErrorCode.METADATA_ERROR, list.toJSON());
                            }
                            AString str = (AString) list.getItem(0);
                            foreignKeyFields.add(str.getStringValue());
                        }

                        // 'RefDataverseName'
                        int refDataverseNameFieldPos = foreignKeyRecord.getType()
                                .getFieldIndex(MetadataRecordTypes.FIELD_NAME_REF_DATAVERSE_NAME);
                        String refDataverseCanonicalName =
                                ((AString) foreignKeyRecord.getValueByPos(refDataverseNameFieldPos)).getStringValue();
                        DataverseName refDataverseName =
                                DataverseName.createFromCanonicalForm(refDataverseCanonicalName);

                        // 'RefDatabaseName'
                        String refDatabase;
                        int refDatabaseNameFieldPos = foreignKeyRecord.getType()
                                .getFieldIndex(MetadataRecordTypes.FIELD_NAME_REF_DATABASE_NAME);
                        if (refDatabaseNameFieldPos >= 0) {
                            refDatabase = ((AString) foreignKeyRecord.getValueByPos(refDatabaseNameFieldPos))
                                    .getStringValue();
                        } else {
                            refDatabase = MetadataUtil.databaseFor(refDataverseName);
                        }

                        // 'RefDatasetName'
                        int refDatasetNameFieldPos = foreignKeyRecord.getType()
                                .getFieldIndex(MetadataRecordTypes.FIELD_NAME_REF_DATASET_NAME);
                        String refDatasetName =
                                ((AString) foreignKeyRecord.getValueByPos(refDatasetNameFieldPos)).getStringValue();

                        foreignKeys.add(new ViewDetails.ForeignKey(foreignKeyFields,
                                new DatasetFullyQualifiedName(refDatabase, refDataverseName, refDatasetName)));
                    }
                }

                // Format fields
                Triple<String, String, String> dateTimeFormats = getDateTimeFormats(datasetDetailsRecord);
                String datetimeFormat = dateTimeFormats.first;
                String dateFormat = dateTimeFormats.second;
                String timeFormat = dateTimeFormats.third;
                datasetDetails = new ViewDetails(definition, dependencies, defaultNull, primaryKeyFields, foreignKeys,
                        datetimeFormat, dateFormat, timeFormat);
                break;
            }
        }

        Map<String, String> hints = getDatasetHints(datasetRecord);

        String metaItemTypeDatabaseName = null;
        DataverseName metaTypeDataverseName = null;
        String metaTypeName = null;
        int metaTypeDataverseNameIndex = recType.getFieldIndex(MetadataRecordTypes.FIELD_NAME_METATYPE_DATAVERSE_NAME);
        if (metaTypeDataverseNameIndex >= 0) {
            String metaTypeDataverseCanonicalName =
                    ((AString) datasetRecord.getValueByPos(metaTypeDataverseNameIndex)).getStringValue();
            metaTypeDataverseName = DataverseName.createFromCanonicalForm(metaTypeDataverseCanonicalName);
            int metaTypeNameIndex = recType.getFieldIndex(MetadataRecordTypes.FIELD_NAME_METATYPE_NAME);
            metaTypeName = ((AString) datasetRecord.getValueByPos(metaTypeNameIndex)).getStringValue();

            int metaTypeDatabaseIdx = recType.getFieldIndex(MetadataRecordTypes.FIELD_NAME_METATYPE_DATABASE_NAME);
            if (metaTypeDatabaseIdx >= 0) {
                metaItemTypeDatabaseName =
                        ((AString) datasetRecord.getValueByPos(metaTypeDatabaseIdx)).getStringValue();
            } else {
                metaItemTypeDatabaseName = MetadataUtil.databaseFor(metaTypeDataverseName);
            }
        }

        long rebalanceCount = getRebalanceCount(datasetRecord);
        String compressionScheme = getCompressionScheme(datasetRecord);
        DatasetFormatInfo datasetFormatInfo = getDatasetFormatInfo(datasetRecord);

        return new Dataset(databaseName, dataverseName, datasetName, itemTypeDatabaseName, typeDataverseName, typeName,
                metaItemTypeDatabaseName, metaTypeDataverseName, metaTypeName, nodeGroupName, compactionPolicy.first,
                compactionPolicy.second, datasetDetails, hints, datasetType, datasetId, pendingOp, rebalanceCount,
                compressionScheme, datasetFormatInfo);
    }

    protected Pair<String, Map<String, String>> readCompactionPolicy(DatasetType datasetType, ARecord datasetRecord) {

        String compactionPolicy =
                ((AString) datasetRecord.getValueByPos(datasetEntity.compactionPolicyIndex())).getStringValue();
        AOrderedList compactionPolicyPropertiesList =
                ((AOrderedList) datasetRecord.getValueByPos(datasetEntity.compactionPolicyPropertiesIndex()));

        Map<String, String> compactionPolicyProperties;
        if (compactionPolicyPropertiesList.size() > 0) {
            compactionPolicyProperties = new LinkedHashMap<>();
            for (IACursor cursor = compactionPolicyPropertiesList.getCursor(); cursor.next();) {
                ARecord field = (ARecord) cursor.get();
                String key = ((AString) field.getValueByPos(MetadataRecordTypes.PROPERTIES_NAME_FIELD_INDEX))
                        .getStringValue();
                String value = ((AString) field.getValueByPos(MetadataRecordTypes.PROPERTIES_VALUE_FIELD_INDEX))
                        .getStringValue();
                compactionPolicyProperties.put(key, value);
            }
        } else {
            compactionPolicyProperties = Collections.emptyMap();
        }
        return new Pair<>(compactionPolicy, compactionPolicyProperties);
    }

    private long getRebalanceCount(ARecord datasetRecord) {
        // Read the rebalance count if there is one.
        int rebalanceCountIndex =
                datasetRecord.getType().getFieldIndex(MetadataRecordTypes.DATASET_ARECORD_REBALANCE_FIELD_NAME);
        return rebalanceCountIndex >= 0 ? ((AInt64) datasetRecord.getValueByPos(rebalanceCountIndex)).getLongValue()
                : 0;
    }

    private String getCompressionScheme(ARecord datasetRecord) {
        final ARecordType datasetType = datasetRecord.getType();
        final int compressionIndex = datasetType
                .getFieldIndex(MetadataRecordTypes.DATASET_ARECORD_BLOCK_LEVEL_STORAGE_COMPRESSION_FIELD_NAME);
        if (compressionIndex >= 0) {
            final ARecordType compressionType = (ARecordType) datasetType.getFieldTypes()[compressionIndex];
            final int schemeIndex = compressionType
                    .getFieldIndex(MetadataRecordTypes.DATASET_ARECORD_DATASET_COMPRESSION_SCHEME_FIELD_NAME);
            final ARecord compressionRecord = (ARecord) datasetRecord.getValueByPos(compressionIndex);
            return ((AString) compressionRecord.getValueByPos(schemeIndex)).getStringValue();
        }
        return CompressionManager.NONE;
    }

    private DatasetFormatInfo getDatasetFormatInfo(ARecord datasetRecord) {
        ARecordType datasetType = datasetRecord.getType();
        int datasetFormatIndex =
                datasetType.getFieldIndex(MetadataRecordTypes.DATASET_ARECORD_DATASET_FORMAT_FIELD_NAME);
        if (datasetFormatIndex < 0) {
            return DatasetFormatInfo.SYSTEM_DEFAULT;
        }

        // Record that holds format information
        ARecordType datasetFormatType = (ARecordType) datasetType.getFieldTypes()[datasetFormatIndex];
        ARecord datasetFormatRecord = (ARecord) datasetRecord.getValueByPos(datasetFormatIndex);

        // Format
        int formatIndex =
                datasetFormatType.getFieldIndex(MetadataRecordTypes.DATASET_ARECORD_DATASET_FORMAT_FORMAT_FIELD_NAME);
        AString formatString = (AString) datasetFormatRecord.getValueByPos(formatIndex);
        DatasetConfig.DatasetFormat format = DatasetConfig.DatasetFormat.valueOf(formatString.getStringValue());

        if (format == DatasetConfig.DatasetFormat.ROW) {
            // Return system default (row) instance
            return DatasetFormatInfo.SYSTEM_DEFAULT;
        }

        // MaxTupleCount
        int maxTupleCountIndex =
                datasetFormatType.getFieldIndex(MetadataRecordTypes.DATASET_ARECORD_DATASET_MAX_TUPLE_COUNT_FIELD_NAME);
        AInt64 maxTupleCountInt = (AInt64) datasetFormatRecord.getValueByPos(maxTupleCountIndex);
        int maxTupleCount = (int) maxTupleCountInt.getLongValue();

        // FreeSpaceTolerance
        int freeSpaceToleranceIndex = datasetFormatType
                .getFieldIndex(MetadataRecordTypes.DATASET_ARECORD_DATASET_FREE_SPACE_TOLERANCE_FIELD_NAME);
        ADouble freeSpaceToleranceDouble = (ADouble) datasetFormatRecord.getValueByPos(freeSpaceToleranceIndex);
        double freeSpaceTolerance = freeSpaceToleranceDouble.getDoubleValue();

        // MaxTupleCount
        int maxLeafNodeSizeIndex = datasetFormatType
                .getFieldIndex(MetadataRecordTypes.DATASET_ARECORD_DATASET_MAX_LEAF_NODE_SIZE_FIELD_NAME);
        AInt64 maxLeafNodeSizeInt = (AInt64) datasetFormatRecord.getValueByPos(maxLeafNodeSizeIndex);
        int maxLeafNodeSize = (int) maxLeafNodeSizeInt.getLongValue();

        // Columnar
        return new DatasetFormatInfo(format, maxTupleCount, freeSpaceTolerance, maxLeafNodeSize);
    }

    @Override
    public ITupleReference getTupleFromMetadataEntity(Dataset dataset) throws HyracksDataException {
        OrderedListBuilder listBuilder = new OrderedListBuilder();
        ArrayBackedValueStorage itemValue = new ArrayBackedValueStorage();
        String databaseName = dataset.getDatabaseName();
        String dataverseCanonicalName = dataset.getDataverseName().getCanonicalForm();
        String datasetName = dataset.getDatasetName();

        // write the key in the first 2 fields of the tuple
        tupleBuilder.reset();
        if (datasetEntity.databaseNameIndex() >= 0) {
            aString.setValue(databaseName);
            stringSerde.serialize(aString, tupleBuilder.getDataOutput());
            tupleBuilder.addFieldEndOffset();
        }
        aString.setValue(dataverseCanonicalName);
        stringSerde.serialize(aString, tupleBuilder.getDataOutput());
        tupleBuilder.addFieldEndOffset();
        aString.setValue(datasetName);
        stringSerde.serialize(aString, tupleBuilder.getDataOutput());
        tupleBuilder.addFieldEndOffset();

        // write the pay-load in the third field of the tuple

        recordBuilder.reset(datasetEntity.getRecordType());

        // write field 0
        if (datasetEntity.databaseNameIndex() >= 0) {
            fieldValue.reset();
            aString.setValue(databaseName);
            stringSerde.serialize(aString, fieldValue.getDataOutput());
            recordBuilder.addField(datasetEntity.databaseNameIndex(), fieldValue);
        }
        fieldValue.reset();
        aString.setValue(dataverseCanonicalName);
        stringSerde.serialize(aString, fieldValue.getDataOutput());
        recordBuilder.addField(datasetEntity.dataverseNameIndex(), fieldValue);

        // write field 1
        fieldValue.reset();
        aString.setValue(datasetName);
        stringSerde.serialize(aString, fieldValue.getDataOutput());
        recordBuilder.addField(datasetEntity.datasetNameIndex(), fieldValue);

        // itemTypeDatabaseName is written later by {@code writeOpenFields()}

        // write field 2
        fieldValue.reset();
        aString.setValue(dataset.getItemTypeDataverseName().getCanonicalForm());
        stringSerde.serialize(aString, fieldValue.getDataOutput());
        recordBuilder.addField(datasetEntity.datatypeDataverseNameIndex(), fieldValue);

        // write field 3
        fieldValue.reset();
        aString.setValue(dataset.getItemTypeName());
        stringSerde.serialize(aString, fieldValue.getDataOutput());
        recordBuilder.addField(datasetEntity.datatypeNameIndex(), fieldValue);

        // write field 4
        fieldValue.reset();
        aString.setValue(dataset.getDatasetType().toString());
        stringSerde.serialize(aString, fieldValue.getDataOutput());
        recordBuilder.addField(datasetEntity.datasetTypeIndex(), fieldValue);

        // write field 5
        fieldValue.reset();
        aString.setValue(dataset.getNodeGroupName());
        stringSerde.serialize(aString, fieldValue.getDataOutput());
        recordBuilder.addField(datasetEntity.groupNameIndex(), fieldValue);

        // write field 6/7
        writeCompactionPolicy(dataset.getDatasetType(), dataset.getCompactionPolicy(),
                dataset.getCompactionPolicyProperties(), listBuilder, itemValue);

        // write field 8/9
        switch (dataset.getDatasetType()) {
            case INTERNAL:
                fieldValue.reset();
                dataset.getDatasetDetails().writeDatasetDetailsRecordType(fieldValue.getDataOutput(), datasetEntity);
                recordBuilder.addField(datasetEntity.internalDetailsIndex(), fieldValue);
                break;
            case EXTERNAL:
                fieldValue.reset();
                dataset.getDatasetDetails().writeDatasetDetailsRecordType(fieldValue.getDataOutput(), datasetEntity);
                recordBuilder.addField(datasetEntity.externalDetailsIndex(), fieldValue);
                break;
            // VIEW details are written later by {@code writeOpenFields()}
        }

        // write field 10
        UnorderedListBuilder uListBuilder = new UnorderedListBuilder();
        uListBuilder
                .reset((AUnorderedListType) datasetEntity.getRecordType().getFieldTypes()[datasetEntity.hintsIndex()]);
        for (Map.Entry<String, String> property : dataset.getHints().entrySet()) {
            String name = property.getKey();
            String value = property.getValue();
            itemValue.reset();
            writeDatasetHintRecord(name, value, itemValue.getDataOutput());
            uListBuilder.addItem(itemValue);
        }
        fieldValue.reset();
        uListBuilder.write(fieldValue.getDataOutput(), true);
        recordBuilder.addField(datasetEntity.hintsIndex(), fieldValue);

        // write field 11
        fieldValue.reset();
        aString.setValue(Calendar.getInstance().getTime().toString());
        stringSerde.serialize(aString, fieldValue.getDataOutput());
        recordBuilder.addField(datasetEntity.timestampIndex(), fieldValue);

        // write field 12
        fieldValue.reset();
        aInt32.setValue(dataset.getDatasetId());
        int32Serde.serialize(aInt32, fieldValue.getDataOutput());
        recordBuilder.addField(datasetEntity.datasetIdIndex(), fieldValue);

        // write field 13
        fieldValue.reset();
        aInt32.setValue(dataset.getPendingOp());
        int32Serde.serialize(aInt32, fieldValue.getDataOutput());
        recordBuilder.addField(datasetEntity.pendingOpIndex(), fieldValue);

        // write open fields
        writeOpenFields(dataset);

        // write record
        recordBuilder.write(tupleBuilder.getDataOutput(), true);
        tupleBuilder.addFieldEndOffset();

        tuple.reset(tupleBuilder.getFieldEndOffsets(), tupleBuilder.getByteArray());
        return tuple;
    }

    protected void writeCompactionPolicy(DatasetType datasetType, String compactionPolicy,
            Map<String, String> compactionPolicyProperties, OrderedListBuilder listBuilder,
            ArrayBackedValueStorage itemValue) throws HyracksDataException {
        // write field 6
        fieldValue.reset();
        aString.setValue(compactionPolicy);
        stringSerde.serialize(aString, fieldValue.getDataOutput());
        recordBuilder.addField(datasetEntity.compactionPolicyIndex(), fieldValue);

        // write field 7
        listBuilder.reset((AOrderedListType) datasetEntity.getRecordType().getFieldTypes()[datasetEntity
                .compactionPolicyPropertiesIndex()]);
        if (compactionPolicyProperties != null && !compactionPolicyProperties.isEmpty()) {
            for (Map.Entry<String, String> property : compactionPolicyProperties.entrySet()) {
                String name = property.getKey();
                String value = property.getValue();
                itemValue.reset();
                DatasetUtil.writePropertyTypeRecord(name, value, itemValue.getDataOutput(),
                        MetadataRecordTypes.COMPACTION_POLICY_PROPERTIES_RECORDTYPE);
                listBuilder.addItem(itemValue);
            }
        }
        fieldValue.reset();
        listBuilder.write(fieldValue.getDataOutput(), true);
        recordBuilder.addField(datasetEntity.compactionPolicyPropertiesIndex(), fieldValue);
    }

    /**
     * Keep protected to allow other extensions to add additional fields
     */
    protected void writeOpenFields(Dataset dataset) throws HyracksDataException {
        writeItemTypeDatabase(dataset);
        writeMetaPart(dataset);
        writeRebalanceCount(dataset);
        writeBlockLevelStorageCompression(dataset);
        writeOpenDetails(dataset);
        writeDatasetFormatInfo(dataset);
    }

    private void writeOpenDetails(Dataset dataset) throws HyracksDataException {
        if (dataset.getDatasetType() == DatasetType.VIEW) {
            // write ViewDetails field
            fieldName.reset();
            aString.setValue(MetadataRecordTypes.FIELD_NAME_VIEW_DETAILS);
            stringSerde.serialize(aString, fieldName.getDataOutput());
            fieldValue.reset();
            dataset.getDatasetDetails().writeDatasetDetailsRecordType(fieldValue.getDataOutput(), datasetEntity);
            recordBuilder.addField(fieldName, fieldValue);
        }
    }

    private void writeItemTypeDatabase(Dataset dataset) throws HyracksDataException {
        //TODO(DB): should use something better
        if (datasetEntity.databaseNameIndex() >= 0) {
            fieldName.reset();
            aString.setValue(MetadataRecordTypes.FIELD_NAME_DATATYPE_DATABASE_NAME);
            stringSerde.serialize(aString, fieldName.getDataOutput());
            fieldValue.reset();
            aString.setValue(dataset.getItemTypeDatabaseName());
            stringSerde.serialize(aString, fieldValue.getDataOutput());
            recordBuilder.addField(fieldName, fieldValue);
        }
    }

    private void writeMetaPart(Dataset dataset) throws HyracksDataException {
        if (dataset.hasMetaPart()) {
            if (datasetEntity.databaseNameIndex() >= 0) {
                fieldName.reset();
                aString.setValue(MetadataRecordTypes.FIELD_NAME_METATYPE_DATABASE_NAME);
                stringSerde.serialize(aString, fieldName.getDataOutput());
                fieldValue.reset();
                //TODO(DB): can dataset.getMetaItemTypeDatabaseName() be null?
                aString.setValue(dataset.getMetaItemTypeDatabaseName());
                stringSerde.serialize(aString, fieldValue.getDataOutput());
                recordBuilder.addField(fieldName, fieldValue);
            }

            // write open field 1, the meta item type Dataverse name.
            fieldName.reset();
            aString.setValue(MetadataRecordTypes.FIELD_NAME_METATYPE_DATAVERSE_NAME);
            stringSerde.serialize(aString, fieldName.getDataOutput());
            fieldValue.reset();
            aString.setValue(dataset.getMetaItemTypeDataverseName().getCanonicalForm());
            stringSerde.serialize(aString, fieldValue.getDataOutput());
            recordBuilder.addField(fieldName, fieldValue);

            // write open field 2, the meta item type name.
            fieldName.reset();
            aString.setValue(MetadataRecordTypes.FIELD_NAME_METATYPE_NAME);
            stringSerde.serialize(aString, fieldName.getDataOutput());
            fieldValue.reset();
            aString.setValue(dataset.getMetaItemTypeName());
            stringSerde.serialize(aString, fieldValue.getDataOutput());
            recordBuilder.addField(fieldName, fieldValue);
        }
    }

    private void writeBlockLevelStorageCompression(Dataset dataset) throws HyracksDataException {
        if (CompressionManager.NONE.equals(dataset.getCompressionScheme())) {
            return;
        }
        RecordBuilder compressionObject = new RecordBuilder();
        compressionObject.reset(DefaultOpenFieldType.NESTED_OPEN_RECORD_TYPE);
        fieldName.reset();
        aString.setValue(MetadataRecordTypes.DATASET_ARECORD_DATASET_COMPRESSION_SCHEME_FIELD_NAME);
        stringSerde.serialize(aString, fieldName.getDataOutput());
        fieldValue.reset();
        aString.setValue(dataset.getCompressionScheme());
        stringSerde.serialize(aString, fieldValue.getDataOutput());
        compressionObject.addField(fieldName, fieldValue);

        fieldName.reset();
        aString.setValue(MetadataRecordTypes.DATASET_ARECORD_BLOCK_LEVEL_STORAGE_COMPRESSION_FIELD_NAME);
        stringSerde.serialize(aString, fieldName.getDataOutput());
        fieldValue.reset();
        compressionObject.write(fieldValue.getDataOutput(), true);
        recordBuilder.addField(fieldName, fieldValue);
    }

    private void writeDatasetFormatInfo(Dataset dataset) throws HyracksDataException {
        DatasetFormatInfo info = dataset.getDatasetFormatInfo();

        RecordBuilder datasetFormatObject = new RecordBuilder();
        datasetFormatObject.reset(DefaultOpenFieldType.NESTED_OPEN_RECORD_TYPE);

        fieldName.reset();
        aString.setValue(MetadataRecordTypes.DATASET_ARECORD_DATASET_FORMAT_FORMAT_FIELD_NAME);
        stringSerde.serialize(aString, fieldName.getDataOutput());
        fieldValue.reset();
        aString.setValue(info.getFormat().toString());
        stringSerde.serialize(aString, fieldValue.getDataOutput());
        datasetFormatObject.addField(fieldName, fieldValue);

        // Columnar settings
        if (info.getFormat() == DatasetConfig.DatasetFormat.COLUMN) {
            // Max tuple count
            fieldName.reset();
            aString.setValue(MetadataRecordTypes.DATASET_ARECORD_DATASET_MAX_TUPLE_COUNT_FIELD_NAME);
            stringSerde.serialize(aString, fieldName.getDataOutput());
            fieldValue.reset();
            aInt64.setValue(info.getMaxTupleCount());
            int64Serde.serialize(aInt64, fieldValue.getDataOutput());
            datasetFormatObject.addField(fieldName, fieldValue);

            // free space tolerance
            fieldName.reset();
            aString.setValue(MetadataRecordTypes.DATASET_ARECORD_DATASET_FREE_SPACE_TOLERANCE_FIELD_NAME);
            stringSerde.serialize(aString, fieldName.getDataOutput());
            fieldValue.reset();
            aDouble.setValue(info.getFreeSpaceTolerance());
            doubleSerde.serialize(aDouble, fieldValue.getDataOutput());
            datasetFormatObject.addField(fieldName, fieldValue);

            // max leaf node size
            fieldName.reset();
            aString.setValue(MetadataRecordTypes.DATASET_ARECORD_DATASET_MAX_LEAF_NODE_SIZE_FIELD_NAME);
            stringSerde.serialize(aString, fieldName.getDataOutput());
            fieldValue.reset();
            aInt64.setValue(info.getMaxLeafNodeSize());
            int64Serde.serialize(aInt64, fieldValue.getDataOutput());
            datasetFormatObject.addField(fieldName, fieldValue);

        }

        fieldName.reset();
        aString.setValue(MetadataRecordTypes.DATASET_ARECORD_DATASET_FORMAT_FIELD_NAME);
        stringSerde.serialize(aString, fieldName.getDataOutput());
        fieldValue.reset();
        datasetFormatObject.write(fieldValue.getDataOutput(), true);
        recordBuilder.addField(fieldName, fieldValue);
    }

    private void writeRebalanceCount(Dataset dataset) throws HyracksDataException {
        if (dataset.getRebalanceCount() > 0) {
            // Adds the field rebalanceCount.
            fieldName.reset();
            aString.setValue(MetadataRecordTypes.DATASET_ARECORD_REBALANCE_FIELD_NAME);
            stringSerde.serialize(aString, fieldName.getDataOutput());
            fieldValue.reset();
            aInt64.setValue(dataset.getRebalanceCount());
            int64Serde.serialize(aInt64, fieldValue.getDataOutput());
            recordBuilder.addField(fieldName, fieldValue);
        }
    }

    protected Map<String, String> getDatasetHints(ARecord datasetRecord) {
        Map<String, String> hints = new HashMap<>();
        String key;
        String value;
        AUnorderedList list = (AUnorderedList) datasetRecord.getValueByPos(datasetEntity.hintsIndex());
        IACursor cursor = list.getCursor();
        while (cursor.next()) {
            ARecord field = (ARecord) cursor.get();
            key = ((AString) field.getValueByPos(MetadataRecordTypes.PROPERTIES_NAME_FIELD_INDEX)).getStringValue();
            value = ((AString) field.getValueByPos(MetadataRecordTypes.PROPERTIES_VALUE_FIELD_INDEX)).getStringValue();
            hints.put(key, value);
        }
        return hints;
    }

    protected void writeDatasetHintRecord(String name, String value, DataOutput out) throws HyracksDataException {
        IARecordBuilder propertyRecordBuilder = new RecordBuilder();
        ArrayBackedValueStorage fieldValue = new ArrayBackedValueStorage();
        propertyRecordBuilder.reset(MetadataRecordTypes.DATASET_HINTS_RECORDTYPE);
        AMutableString aString = new AMutableString("");

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

        propertyRecordBuilder.write(out, true);
    }
}
