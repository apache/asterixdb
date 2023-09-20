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

import static org.apache.asterix.metadata.bootstrap.MetadataRecordTypes.FIELD_NAME_CAST;
import static org.apache.asterix.metadata.bootstrap.MetadataRecordTypes.FIELD_NAME_DEFAULT;

import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.asterix.builders.IARecordBuilder;
import org.apache.asterix.builders.OrderedListBuilder;
import org.apache.asterix.builders.RecordBuilder;
import org.apache.asterix.common.config.DatasetConfig.IndexType;
import org.apache.asterix.common.exceptions.AsterixException;
import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.common.metadata.DataverseName;
import org.apache.asterix.common.transactions.TxnId;
import org.apache.asterix.formats.nontagged.SerializerDeserializerProvider;
import org.apache.asterix.metadata.MetadataNode;
import org.apache.asterix.metadata.bootstrap.IndexEntity;
import org.apache.asterix.metadata.declared.MetadataManagerUtil;
import org.apache.asterix.metadata.entities.Dataset;
import org.apache.asterix.metadata.entities.Datatype;
import org.apache.asterix.metadata.entities.Index;
import org.apache.asterix.metadata.utils.KeyFieldTypeUtil;
import org.apache.asterix.metadata.utils.MetadataUtil;
import org.apache.asterix.om.base.ABoolean;
import org.apache.asterix.om.base.ACollectionCursor;
import org.apache.asterix.om.base.AInt32;
import org.apache.asterix.om.base.AInt64;
import org.apache.asterix.om.base.AInt8;
import org.apache.asterix.om.base.AMutableInt64;
import org.apache.asterix.om.base.AMutableInt8;
import org.apache.asterix.om.base.ANull;
import org.apache.asterix.om.base.AOrderedList;
import org.apache.asterix.om.base.ARecord;
import org.apache.asterix.om.base.AString;
import org.apache.asterix.om.base.IACursor;
import org.apache.asterix.om.base.IAObject;
import org.apache.asterix.om.types.AOrderedListType;
import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.asterix.om.types.IAType;
import org.apache.asterix.om.utils.RecordUtil;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.common.utils.Pair;
import org.apache.hyracks.algebricks.common.utils.Triple;
import org.apache.hyracks.api.dataflow.value.ISerializerDeserializer;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.job.profiling.IndexStats;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;
import org.apache.hyracks.util.OptionalBoolean;

import com.google.common.base.Strings;

/**
 * Translates an Index metadata entity to an ITupleReference and vice versa.
 */
public class IndexTupleTranslator extends AbstractTupleTranslator<Index> {

    private final IndexEntity indexEntity;
    // Field name of open field.
    public static final String GRAM_LENGTH_FIELD_NAME = "GramLength";
    public static final String FULL_TEXT_CONFIG_FIELD_NAME = "FullTextConfig";
    public static final String INDEX_SEARCHKEY_TYPE_FIELD_NAME = "SearchKeyType";
    public static final String INDEX_ISENFORCED_FIELD_NAME = "IsEnforced";
    public static final String INDEX_EXCLUDE_UNKNOWN_FIELD_NAME = "ExcludeUnknownKey";
    public static final String INDEX_SEARCHKEY_SOURCE_INDICATOR_FIELD_NAME = "SearchKeySourceIndicator";
    public static final String INDEX_SEARCHKEY_ELEMENTS_FIELD_NAME = "SearchKeyElements";
    public static final String COMPLEXSEARCHKEY_UNNEST_FIELD_NAME = "UnnestList";
    public static final String COMPLEXSEARCHKEY_PROJECT_FIELD_NAME = "ProjectList";
    public static final String SAMPLE_SEED = "SampleSeed";
    public static final String SAMPLE_CARDINALITY_TARGET = "SampleCardinalityTarget";
    public static final String SOURCE_CARDINALITY = "SourceCardinality";
    public static final String SOURCE_AVG_ITEM_SIZE = "SourceAvgItemSize";
    public static final String INDEXES_STATS = "IndexStats";
    public static final String STATS_NUM_PAGES = "NumPages";
    public static final String STATS_INDEX_NAME = "IndexName";

    protected final TxnId txnId;
    protected final MetadataNode metadataNode;

    protected OrderedListBuilder listBuilder;
    protected OrderedListBuilder innerListBuilder;
    protected OrderedListBuilder primaryKeyListBuilder;
    protected OrderedListBuilder complexSearchKeyNameListBuilder;
    protected IARecordBuilder complexSearchKeyNameRecordBuilder;
    protected IARecordBuilder castRecordBuilder;
    protected OrderedListBuilder indexesStatsListBuilder;
    protected IARecordBuilder indexStatsRecordBuilder;
    protected AOrderedListType stringList;
    protected AOrderedListType int8List;
    protected ArrayBackedValueStorage nameValue;
    protected ArrayBackedValueStorage itemValue;
    protected AMutableInt8 aInt8;
    protected AMutableInt64 aInt64;
    protected ISerializerDeserializer<AInt8> int8Serde;

    @SuppressWarnings("unchecked")
    protected IndexTupleTranslator(TxnId txnId, MetadataNode metadataNode, boolean getTuple, IndexEntity indexEntity) {
        super(getTuple, indexEntity.getIndex(), indexEntity.payloadPosition());
        this.txnId = txnId;
        this.metadataNode = metadataNode;
        this.indexEntity = indexEntity;
        if (getTuple) {
            listBuilder = new OrderedListBuilder();
            innerListBuilder = new OrderedListBuilder();
            primaryKeyListBuilder = new OrderedListBuilder();
            complexSearchKeyNameRecordBuilder = new RecordBuilder();
            castRecordBuilder = new RecordBuilder();
            indexesStatsListBuilder = new OrderedListBuilder();
            indexStatsRecordBuilder = new RecordBuilder();
            complexSearchKeyNameListBuilder = new OrderedListBuilder();
            stringList = new AOrderedListType(BuiltinType.ASTRING, null);
            int8List = new AOrderedListType(BuiltinType.AINT8, null);
            nameValue = new ArrayBackedValueStorage();
            itemValue = new ArrayBackedValueStorage();
            aInt8 = new AMutableInt8((byte) 0);
            aInt64 = new AMutableInt64(0);
            int8Serde = SerializerDeserializerProvider.INSTANCE.getSerializerDeserializer(BuiltinType.AINT8);
        }
    }

    @Override
    protected Index createMetadataEntityFromARecord(ARecord indexRecord) throws AlgebricksException {
        String dataverseCanonicalName =
                ((AString) indexRecord.getValueByPos(indexEntity.dataverseNameIndex())).getStringValue();
        DataverseName dataverseName = DataverseName.createFromCanonicalForm(dataverseCanonicalName);
        int databaseNameIndex = indexEntity.databaseNameIndex();
        String databaseName;
        if (databaseNameIndex >= 0) {
            databaseName = ((AString) indexRecord.getValueByPos(databaseNameIndex)).getStringValue();
        } else {
            databaseName = MetadataUtil.databaseFor(dataverseName);
        }
        String datasetName = ((AString) indexRecord.getValueByPos(indexEntity.datasetNameIndex())).getStringValue();
        String indexName = ((AString) indexRecord.getValueByPos(indexEntity.indexNameIndex())).getStringValue();
        IndexType indexType = IndexType
                .valueOf(((AString) indexRecord.getValueByPos(indexEntity.indexStructureIndex())).getStringValue());
        boolean isPrimaryIndex = ((ABoolean) indexRecord.getValueByPos(indexEntity.isPrimaryIndex())).getBoolean();

        // Read key names
        List<Pair<List<List<String>>, List<List<String>>>> searchElements = new ArrayList<>();
        switch (Index.IndexCategory.of(indexType)) {
            case VALUE:
            case TEXT:
                // Read the key names from the SearchKeyName field
                IACursor fieldNameCursor =
                        ((AOrderedList) indexRecord.getValueByPos(indexEntity.searchKeyIndex())).getCursor();
                AOrderedList fieldNameList;
                while (fieldNameCursor.next()) {
                    fieldNameList = (AOrderedList) fieldNameCursor.get();
                    IACursor nestedFieldNameCursor = (fieldNameList.getCursor());
                    List<String> nestedFieldName = new ArrayList<>();
                    while (nestedFieldNameCursor.next()) {
                        nestedFieldName.add(((AString) nestedFieldNameCursor.get()).getStringValue());
                    }
                    searchElements.add(new Pair<>(null, Collections.singletonList(nestedFieldName)));
                }
                break;
            case ARRAY:
                // Read the unnest/project from the ComplexSearchKeyName field
                int complexSearchKeyFieldPos = indexRecord.getType().getFieldIndex(INDEX_SEARCHKEY_ELEMENTS_FIELD_NAME);
                IACursor complexSearchKeyCursor = new ACollectionCursor();
                if (complexSearchKeyFieldPos > 0) {
                    complexSearchKeyCursor =
                            ((AOrderedList) indexRecord.getValueByPos(complexSearchKeyFieldPos)).getCursor();
                }
                while (complexSearchKeyCursor.next()) {
                    Pair<List<List<String>>, List<List<String>>> searchElement;
                    IAObject complexSearchKeyItem = complexSearchKeyCursor.get();
                    switch (complexSearchKeyItem.getType().getTypeTag()) {
                        case ARRAY:
                            AOrderedList complexSearchKeyArray = (AOrderedList) complexSearchKeyItem;
                            List<String> project = new ArrayList<>(complexSearchKeyArray.size());
                            // We only have one element.
                            AOrderedList innerListForArray = (AOrderedList) complexSearchKeyArray.getItem(0);
                            IACursor innerListCursorForArray = innerListForArray.getCursor();
                            while (innerListCursorForArray.next()) {
                                project.add(((AString) innerListCursorForArray.get()).getStringValue());
                            }
                            searchElement = new Pair<>(null, Collections.singletonList(project));
                            break;
                        case OBJECT:
                            ARecord complexSearchKeyRecord = (ARecord) complexSearchKeyItem;
                            ARecordType complexSearchKeyRecordType = complexSearchKeyRecord.getType();
                            int unnestFieldPos =
                                    complexSearchKeyRecordType.getFieldIndex(COMPLEXSEARCHKEY_UNNEST_FIELD_NAME);
                            if (unnestFieldPos < 0) {
                                throw new AsterixException(ErrorCode.METADATA_ERROR, complexSearchKeyRecord.toJSON());
                            }
                            AOrderedList unnestFieldList =
                                    (AOrderedList) complexSearchKeyRecord.getValueByPos(unnestFieldPos);
                            List<List<String>> unnestList = new ArrayList<>(unnestFieldList.size());
                            IACursor unnestFieldListCursor = unnestFieldList.getCursor();
                            while (unnestFieldListCursor.next()) {
                                AOrderedList innerList = (AOrderedList) unnestFieldListCursor.get();
                                List<String> unnestPath = new ArrayList<>(innerList.size());
                                IACursor innerListCursor = innerList.getCursor();
                                while (innerListCursor.next()) {
                                    unnestPath.add(((AString) innerListCursor.get()).getStringValue());
                                }
                                unnestList.add(unnestPath);
                            }
                            int projectFieldPos =
                                    complexSearchKeyRecordType.getFieldIndex(COMPLEXSEARCHKEY_PROJECT_FIELD_NAME);
                            List<List<String>> projectList = new ArrayList<>();
                            if (projectFieldPos >= 0) {
                                AOrderedList projectFieldList =
                                        (AOrderedList) complexSearchKeyRecord.getValueByPos(projectFieldPos);
                                projectList = new ArrayList<>(projectFieldList.size());
                                IACursor projectFieldListCursor = projectFieldList.getCursor();
                                while (projectFieldListCursor.next()) {
                                    AOrderedList innerList = (AOrderedList) projectFieldListCursor.get();
                                    List<String> projectPath = new ArrayList<>(innerList.size());
                                    IACursor innerListCursor = innerList.getCursor();
                                    while (innerListCursor.next()) {
                                        projectPath.add(((AString) innerListCursor.get()).getStringValue());
                                    }
                                    projectList.add(projectPath);
                                }
                            } else {
                                projectList.add(null);
                            }
                            searchElement = new Pair<>(unnestList, projectList);
                            break;
                        default:
                            throw new AsterixException(ErrorCode.METADATA_ERROR, complexSearchKeyItem.toJSON());
                    }
                    searchElements.add(searchElement);
                }
                break;
            case SAMPLE:
                searchElements = Collections.emptyList();
                break;
            default:
                throw new AsterixException(ErrorCode.METADATA_ERROR, indexType.toString());
        }
        int searchElementCount = searchElements.size();

        String fullTextConfig = null;
        int fullTextConfigPos = indexRecord.getType().getFieldIndex(FULL_TEXT_CONFIG_FIELD_NAME);
        if (fullTextConfigPos >= 0) {
            fullTextConfig = ((AString) indexRecord.getValueByPos(fullTextConfigPos)).getStringValue();
        }

        // Read a field-source-indicator field.
        List<Integer> keyFieldSourceIndicator = new ArrayList<>(searchElementCount);
        int keyFieldSourceIndicatorIndex =
                indexRecord.getType().getFieldIndex(INDEX_SEARCHKEY_SOURCE_INDICATOR_FIELD_NAME);
        if (keyFieldSourceIndicatorIndex >= 0) {
            IACursor cursor = ((AOrderedList) indexRecord.getValueByPos(keyFieldSourceIndicatorIndex)).getCursor();
            while (cursor.next()) {
                keyFieldSourceIndicator.add((int) ((AInt8) cursor.get()).getByteValue());
            }
        } else {
            for (int index = 0; index < searchElementCount; ++index) {
                keyFieldSourceIndicator.add(Index.RECORD_INDICATOR);
            }
        }

        // Read key types
        int indexKeyTypeFieldPos = indexRecord.getType().getFieldIndex(INDEX_SEARCHKEY_TYPE_FIELD_NAME);
        IACursor fieldTypeCursor = new ACollectionCursor();
        if (indexKeyTypeFieldPos > 0) {
            fieldTypeCursor = ((AOrderedList) indexRecord.getValueByPos(indexKeyTypeFieldPos)).getCursor();
        }
        List<List<IAType>> searchKeyType = new ArrayList<>(searchElementCount);
        while (fieldTypeCursor.next()) {
            IAObject fieldTypeItem = fieldTypeCursor.get();
            switch (fieldTypeItem.getType().getTypeTag()) {
                case STRING:
                    // This is a simple element, place in a single-element list.
                    String typeName = ((AString) fieldTypeItem).getStringValue();
                    IAType fieldType =
                            Datatype.getTypeFromTypeName(metadataNode, txnId, databaseName, dataverseName, typeName);
                    searchKeyType.add(Collections.singletonList(fieldType));
                    break;
                case ARRAY:
                    // This is a complex element, read all types.
                    List<IAType> fieldTypes = new ArrayList<>();
                    AOrderedList fieldTypeList = (AOrderedList) fieldTypeItem;
                    IACursor fieldTypeListCursor = fieldTypeList.getCursor();
                    while (fieldTypeListCursor.next()) {
                        typeName = ((AString) fieldTypeListCursor.get()).getStringValue();
                        fieldTypes.add(Datatype.getTypeFromTypeName(metadataNode, txnId, databaseName, dataverseName,
                                typeName));
                    }
                    searchKeyType.add(fieldTypes);
                    break;
                default:
                    throw new AsterixException(ErrorCode.METADATA_ERROR, fieldTypeItem.toJSON());
            }
        }
        boolean isOverridingKeyTypes;
        if (searchKeyType.isEmpty()) {
            // if index key type information is not persisted, then we extract type information
            // from the record metadata
            Dataset dataset = metadataNode.getDataset(txnId, databaseName, dataverseName, datasetName);
            String datatypeName = dataset.getItemTypeName();
            //TODO(DB): get 'database' of item type and meta type
            DataverseName datatypeDataverseName = dataset.getItemTypeDataverseName();
            String datatypeDatabase = MetadataUtil.databaseFor(datatypeDataverseName);
            ARecordType recordDt = (ARecordType) metadataNode
                    .getDatatype(txnId, datatypeDatabase, datatypeDataverseName, datatypeName).getDatatype();
            String metatypeName = dataset.getMetaItemTypeName();
            DataverseName metatypeDataverseName = dataset.getMetaItemTypeDataverseName();
            String metaTypeDatabase = MetadataUtil.databaseFor(metatypeDataverseName);
            ARecordType metaDt = null;
            if (metatypeName != null && metatypeDataverseName != null) {
                metaDt = (ARecordType) metadataNode
                        .getDatatype(txnId, metaTypeDatabase, metatypeDataverseName, metatypeName).getDatatype();
            }
            recordDt = (ARecordType) MetadataManagerUtil.findTypeForDatasetWithoutType(recordDt, metaDt, dataset);
            searchKeyType = new ArrayList<>(searchElementCount);
            for (int i = 0; i < searchElementCount; i++) {
                Pair<List<List<String>>, List<List<String>>> searchElement = searchElements.get(i);
                List<List<String>> unnestPathList = searchElement.first;
                List<List<String>> projectPathList = searchElement.second;

                ARecordType sourceRecordType = keyFieldSourceIndicator.get(i) == 1 ? metaDt : recordDt;
                IAType inputTypePrime;
                boolean inputTypeNullable, inputTypeMissable;
                if (unnestPathList == null) {
                    inputTypePrime = sourceRecordType;
                    inputTypeNullable = inputTypeMissable = false;
                } else {
                    Triple<IAType, Boolean, Boolean> unnestTypeResult =
                            KeyFieldTypeUtil.getKeyUnnestType(sourceRecordType, unnestPathList, null);
                    if (unnestTypeResult == null) {
                        inputTypePrime = null; // = ANY
                        inputTypeNullable = inputTypeMissable = true;
                    } else {
                        inputTypePrime = unnestTypeResult.first;
                        inputTypeNullable = unnestTypeResult.second;
                        inputTypeMissable = unnestTypeResult.third;
                    }
                }

                List<IAType> projectTypeList = new ArrayList<>(projectPathList.size());
                for (List<String> projectPath : projectPathList) {
                    IAType projectTypePrime;
                    boolean projectTypeNullable, projectTypeMissable;
                    if (projectPath == null) {
                        projectTypePrime = inputTypePrime;
                        projectTypeNullable = inputTypeNullable;
                        projectTypeMissable = inputTypeMissable;
                    } else if (inputTypePrime == null ||
                    // handle special case of the empty field name in
                    // ExternalIndexingOperations.FILE_INDEX_FIELD_NAMES
                            (projectPath.size() == 1 && projectPath.get(0).isEmpty())) {
                        projectTypePrime = null; // ANY
                        projectTypeNullable = projectTypeMissable = true;
                    } else {
                        if (inputTypePrime.getTypeTag() != ATypeTag.OBJECT) {
                            throw new AsterixException(ErrorCode.METADATA_ERROR, projectPath.toString());
                        }
                        Triple<IAType, Boolean, Boolean> projectTypeResult =
                                KeyFieldTypeUtil.getKeyProjectType((ARecordType) inputTypePrime, projectPath, null);
                        if (projectTypeResult == null) {
                            throw new AsterixException(ErrorCode.METADATA_ERROR, projectPath.toString());
                        }
                        projectTypePrime = projectTypeResult.first;
                        projectTypeNullable = inputTypeNullable || projectTypeResult.second;
                        projectTypeMissable = inputTypeMissable || projectTypeResult.third;
                    }
                    IAType projectType = projectTypePrime == null ? null
                            : KeyFieldTypeUtil.makeUnknownableType(projectTypePrime, projectTypeNullable,
                                    projectTypeMissable);

                    projectTypeList.add(projectType);
                }

                searchKeyType.add(projectTypeList);
            }
            isOverridingKeyTypes = false;
        } else {
            isOverridingKeyTypes = true;
        }

        // create index details structure
        Index.IIndexDetails indexDetails;
        switch (Index.IndexCategory.of(indexType)) {
            case VALUE:
                List<List<String>> keyFieldNames =
                        searchElements.stream().map(Pair::getSecond).map(l -> l.get(0)).collect(Collectors.toList());
                List<IAType> keyFieldTypes = searchKeyType.stream().map(l -> l.get(0)).collect(Collectors.toList());

                OptionalBoolean excludeUnknownKey = OptionalBoolean.empty();
                OptionalBoolean castDefaultNull = OptionalBoolean.empty();
                String datetimeFormat = null, dateFormat = null, timeFormat = null;
                boolean isBtreeIdx = indexType == IndexType.BTREE && !isPrimaryIndex && !keyFieldNames.isEmpty();
                if (isBtreeIdx) {
                    // exclude unknown key value; default to always include unknowns for normal b-trees
                    excludeUnknownKey = OptionalBoolean.FALSE();
                    int excludeUnknownKeyPos = indexRecord.getType().getFieldIndex(INDEX_EXCLUDE_UNKNOWN_FIELD_NAME);
                    if (excludeUnknownKeyPos >= 0) {
                        excludeUnknownKey = OptionalBoolean
                                .of(((ABoolean) indexRecord.getValueByPos(excludeUnknownKeyPos)).getBoolean());
                    }
                    // cast record
                    int castPos = indexRecord.getType().getFieldIndex(FIELD_NAME_CAST);
                    if (castPos >= 0) {
                        IAObject recValue = indexRecord.getValueByPos(castPos);
                        if (recValue.getType().getTypeTag() == ATypeTag.OBJECT) {
                            ARecord castRec = (ARecord) recValue;
                            ARecordType castRecType = castRec.getType();
                            // cast default value
                            int defaultFieldPos = castRecType.getFieldIndex(FIELD_NAME_DEFAULT);
                            if (defaultFieldPos >= 0) {
                                IAObject defaultVal = castRec.getValueByPos(defaultFieldPos);
                                if (defaultVal.getType().getTypeTag() == ATypeTag.NULL) {
                                    castDefaultNull = OptionalBoolean.TRUE();

                                    // Format fields
                                    Triple<String, String, String> dateTimeFormats = getDateTimeFormats(castRec);
                                    datetimeFormat = dateTimeFormats.first;
                                    dateFormat = dateTimeFormats.second;
                                    timeFormat = dateTimeFormats.third;
                                }
                            }
                        }
                    }
                }
                indexDetails = new Index.ValueIndexDetails(keyFieldNames, keyFieldSourceIndicator, keyFieldTypes,
                        isOverridingKeyTypes, excludeUnknownKey, castDefaultNull, datetimeFormat, dateFormat,
                        timeFormat);
                break;
            case TEXT:
                keyFieldNames =
                        searchElements.stream().map(Pair::getSecond).map(l -> l.get(0)).collect(Collectors.toList());
                keyFieldTypes = searchKeyType.stream().map(l -> l.get(0)).collect(Collectors.toList());
                // Check if there is a gram length as well.
                int gramLength = -1;
                int gramLenPos = indexRecord.getType().getFieldIndex(GRAM_LENGTH_FIELD_NAME);
                if (gramLenPos >= 0) {
                    gramLength = ((AInt32) indexRecord.getValueByPos(gramLenPos)).getIntegerValue();
                }
                indexDetails = new Index.TextIndexDetails(keyFieldNames, keyFieldSourceIndicator, keyFieldTypes,
                        isOverridingKeyTypes, gramLength, fullTextConfig);
                break;
            case ARRAY:
                List<Index.ArrayIndexElement> elementList = new ArrayList<>(searchElementCount);
                for (int i = 0; i < searchElementCount; i++) {
                    Pair<List<List<String>>, List<List<String>>> searchElement = searchElements.get(i);
                    List<IAType> typeList = searchKeyType.get(i);
                    int sourceIndicator = keyFieldSourceIndicator.get(i);
                    elementList.add(new Index.ArrayIndexElement(searchElement.first, searchElement.second, typeList,
                            sourceIndicator));
                }
                indexDetails = new Index.ArrayIndexDetails(elementList, isOverridingKeyTypes);
                break;
            case SAMPLE:
                keyFieldNames =
                        searchElements.stream().map(Pair::getSecond).map(l -> l.get(0)).collect(Collectors.toList());
                keyFieldTypes = searchKeyType.stream().map(l -> l.get(0)).collect(Collectors.toList());

                int sampleSeedPos = indexRecord.getType().getFieldIndex(SAMPLE_SEED);
                if (sampleSeedPos < 0) {
                    throw new AsterixException(ErrorCode.METADATA_ERROR, SAMPLE_SEED);
                }
                long sampleSeed = ((AInt64) indexRecord.getValueByPos(sampleSeedPos)).getLongValue();

                int sampleCardinalityTargetPos = indexRecord.getType().getFieldIndex(SAMPLE_CARDINALITY_TARGET);
                if (sampleCardinalityTargetPos < 0) {
                    throw new AsterixException(ErrorCode.METADATA_ERROR, SAMPLE_CARDINALITY_TARGET);
                }
                int sampleCardinalityTarget =
                        ((AInt32) indexRecord.getValueByPos(sampleCardinalityTargetPos)).getIntegerValue();

                int sourceCardinalityPos = indexRecord.getType().getFieldIndex(SOURCE_CARDINALITY);
                if (sourceCardinalityPos < 0) {
                    throw new AsterixException(ErrorCode.METADATA_ERROR, SOURCE_CARDINALITY);
                }
                long sourceCardinality = ((AInt64) indexRecord.getValueByPos(sourceCardinalityPos)).getLongValue();

                int sourceAvgItemSizePos = indexRecord.getType().getFieldIndex(SOURCE_AVG_ITEM_SIZE);
                if (sourceAvgItemSizePos < 0) {
                    throw new AsterixException(ErrorCode.METADATA_ERROR, SOURCE_AVG_ITEM_SIZE);
                }
                int sourceAvgItemSize = ((AInt32) indexRecord.getValueByPos(sourceAvgItemSizePos)).getIntegerValue();

                int indexesStatsPos = indexRecord.getType().getFieldIndex(INDEXES_STATS);
                Map<String, IndexStats> indexesStats;
                if (indexesStatsPos >= 0) {
                    AOrderedList indexesStatsList = (AOrderedList) indexRecord.getValueByPos(indexesStatsPos);
                    int numIndexes = indexesStatsList.size();
                    indexesStats = numIndexes > 0 ? new HashMap<>() : Collections.emptyMap();
                    for (int i = 0; i < numIndexes; i++) {
                        ARecord stats = (ARecord) indexesStatsList.getItem(i);
                        IAObject numPages = stats.getValueByPos(stats.getType().getFieldIndex(STATS_NUM_PAGES));
                        IAObject idxNameObj = stats.getValueByPos(stats.getType().getFieldIndex(STATS_INDEX_NAME));
                        String idxName = ((AString) idxNameObj).getStringValue();
                        IndexStats idxStats = new IndexStats(idxName, ((AInt64) numPages).getLongValue());
                        indexesStats.put(idxName, idxStats);
                    }
                } else {
                    indexesStats = Collections.emptyMap();
                }

                indexDetails = new Index.SampleIndexDetails(keyFieldNames, keyFieldSourceIndicator, keyFieldTypes,
                        sampleCardinalityTarget, sourceCardinality, sourceAvgItemSize, sampleSeed, indexesStats);
                break;
            default:
                throw new AsterixException(ErrorCode.METADATA_ERROR, indexType.toString());
        }

        int isEnforcedFieldPos = indexRecord.getType().getFieldIndex(INDEX_ISENFORCED_FIELD_NAME);
        Boolean isEnforcingKeys = false;
        if (isEnforcedFieldPos > 0) {
            isEnforcingKeys = ((ABoolean) indexRecord.getValueByPos(isEnforcedFieldPos)).getBoolean();
        }
        int pendingOp = ((AInt32) indexRecord.getValueByPos(indexEntity.pendingOpIndex())).getIntegerValue();

        return new Index(databaseName, dataverseName, datasetName, indexName, indexType, indexDetails, isEnforcingKeys,
                isPrimaryIndex, pendingOp);
    }

    @Override
    public ITupleReference getTupleFromMetadataEntity(Index index) throws HyracksDataException, AlgebricksException {
        String dataverseCanonicalName = index.getDataverseName().getCanonicalForm();

        // write the key in the first 3 fields of the tuple
        tupleBuilder.reset();
        if (indexEntity.databaseNameIndex() >= 0) {
            aString.setValue(index.getDatabaseName());
            stringSerde.serialize(aString, tupleBuilder.getDataOutput());
            tupleBuilder.addFieldEndOffset();
        }
        aString.setValue(dataverseCanonicalName);
        stringSerde.serialize(aString, tupleBuilder.getDataOutput());
        tupleBuilder.addFieldEndOffset();
        aString.setValue(index.getDatasetName());
        stringSerde.serialize(aString, tupleBuilder.getDataOutput());
        tupleBuilder.addFieldEndOffset();
        aString.setValue(index.getIndexName());
        stringSerde.serialize(aString, tupleBuilder.getDataOutput());
        tupleBuilder.addFieldEndOffset();

        // write the payload in the fourth field of the tuple
        recordBuilder.reset(indexEntity.getRecordType());

        if (indexEntity.databaseNameIndex() >= 0) {
            fieldValue.reset();
            aString.setValue(index.getDatabaseName());
            stringSerde.serialize(aString, fieldValue.getDataOutput());
            recordBuilder.addField(indexEntity.databaseNameIndex(), fieldValue);
        }
        // write field 0
        fieldValue.reset();
        aString.setValue(dataverseCanonicalName);
        stringSerde.serialize(aString, fieldValue.getDataOutput());
        recordBuilder.addField(indexEntity.dataverseNameIndex(), fieldValue);

        // write field 1
        fieldValue.reset();
        aString.setValue(index.getDatasetName());
        stringSerde.serialize(aString, fieldValue.getDataOutput());
        recordBuilder.addField(indexEntity.datasetNameIndex(), fieldValue);

        // write field 2
        fieldValue.reset();
        aString.setValue(index.getIndexName());
        stringSerde.serialize(aString, fieldValue.getDataOutput());
        recordBuilder.addField(indexEntity.indexNameIndex(), fieldValue);

        // write field 3
        IndexType indexType = index.getIndexType();
        fieldValue.reset();
        aString.setValue(indexType.toString());
        stringSerde.serialize(aString, fieldValue.getDataOutput());
        recordBuilder.addField(indexEntity.indexStructureIndex(), fieldValue);

        // write field 4
        primaryKeyListBuilder
                .reset((AOrderedListType) indexEntity.getRecordType().getFieldTypes()[indexEntity.searchKeyIndex()]);
        List<List<String>> searchKey;
        switch (Index.IndexCategory.of(indexType)) {
            case VALUE:
                searchKey = ((Index.ValueIndexDetails) index.getIndexDetails()).getKeyFieldNames();
                break;
            case TEXT:
                searchKey = ((Index.TextIndexDetails) index.getIndexDetails()).getKeyFieldNames();
                break;
            case ARRAY:
                // If we have a complex index, we persist all of the names in the complex SK name array instead.
                searchKey = Collections.emptyList();
                break;
            case SAMPLE:
                searchKey = ((Index.SampleIndexDetails) index.getIndexDetails()).getKeyFieldNames();
                break;
            default:
                throw new AsterixException(ErrorCode.METADATA_ERROR, indexType.toString());
        }
        for (List<String> field : searchKey) {
            listBuilder.reset(stringList);
            for (String subField : field) {
                itemValue.reset();
                aString.setValue(subField);
                stringSerde.serialize(aString, itemValue.getDataOutput());
                listBuilder.addItem(itemValue);
            }
            itemValue.reset();
            listBuilder.write(itemValue.getDataOutput(), true);
            primaryKeyListBuilder.addItem(itemValue);
        }
        fieldValue.reset();
        primaryKeyListBuilder.write(fieldValue.getDataOutput(), true);
        recordBuilder.addField(indexEntity.searchKeyIndex(), fieldValue);

        // write field 5
        fieldValue.reset();
        if (index.isPrimaryIndex()) {
            booleanSerde.serialize(ABoolean.TRUE, fieldValue.getDataOutput());
        } else {
            booleanSerde.serialize(ABoolean.FALSE, fieldValue.getDataOutput());
        }
        recordBuilder.addField(indexEntity.isPrimaryIndex(), fieldValue);

        // write field 6
        fieldValue.reset();
        aString.setValue(Calendar.getInstance().getTime().toString());
        stringSerde.serialize(aString, fieldValue.getDataOutput());
        recordBuilder.addField(indexEntity.timestampIndex(), fieldValue);

        // write field 7
        fieldValue.reset();
        int32Serde.serialize(new AInt32(index.getPendingOp()), fieldValue.getDataOutput());
        recordBuilder.addField(indexEntity.pendingOpIndex(), fieldValue);

        // write open fields
        writeOpenFields(index);

        // write record
        recordBuilder.write(tupleBuilder.getDataOutput(), true);
        tupleBuilder.addFieldEndOffset();

        tuple.reset(tupleBuilder.getFieldEndOffsets(), tupleBuilder.getByteArray());
        return tuple;
    }

    /**
     * Keep protected to allow other extensions to add additional fields
     */
    protected void writeOpenFields(Index index) throws HyracksDataException, AlgebricksException {
        switch (Index.IndexCategory.of(index.getIndexType())) {
            case TEXT:
                Index.TextIndexDetails textIndexDetails = (Index.TextIndexDetails) index.getIndexDetails();
                writeGramLength(textIndexDetails);
                writeFullTextConfig(textIndexDetails);
                break;
            case ARRAY:
                writeComplexSearchKeys((Index.ArrayIndexDetails) index.getIndexDetails());
                break;
        }
        writeSearchKeyType(index);
        writeEnforced(index);
        writeSearchKeySourceIndicator(index);
        writeExcludeUnknownKey(index);
        writeCast(index);
        writeSampleDetails(index);
    }

    private void writeComplexSearchKeys(Index.ArrayIndexDetails indexDetails) throws HyracksDataException {
        complexSearchKeyNameListBuilder.reset(AOrderedListType.FULL_OPEN_ORDEREDLIST_TYPE);
        for (Index.ArrayIndexElement element : indexDetails.getElementList()) {
            if (element.getUnnestList().isEmpty()) {
                // If this is not a complex search key, write the field names as before.
                buildSearchKeyNameList(element.getProjectList());
                itemValue.reset();
                listBuilder.write(itemValue.getDataOutput(), true);
            } else {
                // Otherwise, we create a complex searchkey name record.
                complexSearchKeyNameRecordBuilder.reset(RecordUtil.FULLY_OPEN_RECORD_TYPE);

                nameValue.reset();
                aString.setValue(COMPLEXSEARCHKEY_UNNEST_FIELD_NAME);
                stringSerde.serialize(aString, nameValue.getDataOutput());
                buildSearchKeyNameList(element.getUnnestList());
                itemValue.reset();
                listBuilder.write(itemValue.getDataOutput(), true);
                complexSearchKeyNameRecordBuilder.addField(nameValue, itemValue);

                if (element.getProjectList().get(0) != null) {
                    nameValue.reset();
                    aString.setValue(COMPLEXSEARCHKEY_PROJECT_FIELD_NAME);
                    stringSerde.serialize(aString, nameValue.getDataOutput());
                    buildSearchKeyNameList(element.getProjectList());
                    itemValue.reset();
                    listBuilder.write(itemValue.getDataOutput(), true);
                    complexSearchKeyNameRecordBuilder.addField(nameValue, itemValue);
                }

                itemValue.reset();
                complexSearchKeyNameRecordBuilder.write(itemValue.getDataOutput(), true);
            }
            complexSearchKeyNameListBuilder.addItem(itemValue);
        }

        nameValue.reset();
        fieldValue.reset();
        aString.setValue(INDEX_SEARCHKEY_ELEMENTS_FIELD_NAME);
        stringSerde.serialize(aString, nameValue.getDataOutput());
        complexSearchKeyNameListBuilder.write(fieldValue.getDataOutput(), true);
        recordBuilder.addField(nameValue, fieldValue);
    }

    private void buildSearchKeyNameList(List<List<String>> fieldList) throws HyracksDataException {
        listBuilder.reset(AOrderedListType.FULL_OPEN_ORDEREDLIST_TYPE);
        for (List<String> nestedField : fieldList) {
            if (nestedField == null) {
                itemValue.reset();
                nullSerde.serialize(ANull.NULL, itemValue.getDataOutput());
            } else {
                innerListBuilder.reset(AOrderedListType.FULL_OPEN_ORDEREDLIST_TYPE);
                for (String subField : nestedField) {
                    itemValue.reset();
                    aString.setValue(subField);
                    stringSerde.serialize(aString, itemValue.getDataOutput());
                    innerListBuilder.addItem(itemValue);
                }
                itemValue.reset();
                innerListBuilder.write(itemValue.getDataOutput(), true);
            }
            listBuilder.addItem(itemValue);
        }
    }

    private void writeGramLength(Index.TextIndexDetails index) throws HyracksDataException {
        if (index.getGramLength() > 0) {
            fieldValue.reset();
            nameValue.reset();
            aString.setValue(GRAM_LENGTH_FIELD_NAME);
            stringSerde.serialize(aString, nameValue.getDataOutput());
            int32Serde.serialize(new AInt32(index.getGramLength()), fieldValue.getDataOutput());
            recordBuilder.addField(nameValue, fieldValue);
        }
    }

    private void writeFullTextConfig(Index.TextIndexDetails index) throws HyracksDataException {
        if (!Strings.isNullOrEmpty(index.getFullTextConfigName())) {
            nameValue.reset();
            aString.setValue(FULL_TEXT_CONFIG_FIELD_NAME);
            stringSerde.serialize(aString, nameValue.getDataOutput());

            fieldValue.reset();
            aString.setValue(index.getFullTextConfigName());
            stringSerde.serialize(aString, fieldValue.getDataOutput());

            recordBuilder.addField(nameValue, fieldValue);
        }
    }

    private void writeSearchKeyType(Index index) throws HyracksDataException, AlgebricksException {
        if (!index.getIndexDetails().isOverridingKeyFieldTypes()) {
            return;
        }

        OrderedListBuilder typeListBuilder = new OrderedListBuilder();
        typeListBuilder.reset(AOrderedListType.FULL_OPEN_ORDEREDLIST_TYPE);

        nameValue.reset();
        aString.setValue(INDEX_SEARCHKEY_TYPE_FIELD_NAME);

        stringSerde.serialize(aString, nameValue.getDataOutput());

        switch (Index.IndexCategory.of(index.getIndexType())) {
            // For value and text indexes, we persist the type as a single string (backwards compatibility).
            case VALUE:
                for (IAType type : ((Index.ValueIndexDetails) index.getIndexDetails()).getKeyFieldTypes()) {
                    itemValue.reset();
                    aString.setValue(type.getTypeName());
                    stringSerde.serialize(aString, itemValue.getDataOutput());
                    typeListBuilder.addItem(itemValue);
                }
                break;
            case TEXT:
                for (IAType type : ((Index.TextIndexDetails) index.getIndexDetails()).getKeyFieldTypes()) {
                    itemValue.reset();
                    aString.setValue(type.getTypeName());
                    stringSerde.serialize(aString, itemValue.getDataOutput());
                    typeListBuilder.addItem(itemValue);
                }
                break;
            case ARRAY:
                // For array indexes we persist the type as a list of strings.
                for (Index.ArrayIndexElement element : ((Index.ArrayIndexDetails) index.getIndexDetails())
                        .getElementList()) {
                    listBuilder.reset(AOrderedListType.FULL_OPEN_ORDEREDLIST_TYPE);
                    for (IAType type : element.getTypeList()) {
                        itemValue.reset();
                        aString.setValue(type.getTypeName());
                        stringSerde.serialize(aString, itemValue.getDataOutput());
                        listBuilder.addItem(itemValue);
                    }
                    itemValue.reset();
                    listBuilder.write(itemValue.getDataOutput(), true);
                    typeListBuilder.addItem(itemValue);
                }
                break;
            default:
                throw new AsterixException(ErrorCode.METADATA_ERROR, index.getIndexType().toString());
        }
        fieldValue.reset();
        typeListBuilder.write(fieldValue.getDataOutput(), true);
        recordBuilder.addField(nameValue, fieldValue);
    }

    private void writeEnforced(Index index) throws HyracksDataException {
        if (index.isEnforced()) {
            fieldValue.reset();
            nameValue.reset();
            aString.setValue(INDEX_ISENFORCED_FIELD_NAME);
            stringSerde.serialize(aString, nameValue.getDataOutput());
            booleanSerde.serialize(ABoolean.TRUE, fieldValue.getDataOutput());
            recordBuilder.addField(nameValue, fieldValue);
        }
    }

    private void writeSearchKeySourceIndicator(Index index) throws HyracksDataException, AlgebricksException {
        List<Integer> keySourceIndicator;
        switch (Index.IndexCategory.of(index.getIndexType())) {
            case VALUE:
                keySourceIndicator = ((Index.ValueIndexDetails) index.getIndexDetails()).getKeyFieldSourceIndicators();
                break;
            case TEXT:
                keySourceIndicator = ((Index.TextIndexDetails) index.getIndexDetails()).getKeyFieldSourceIndicators();
                break;
            case ARRAY:
                keySourceIndicator = ((Index.ArrayIndexDetails) index.getIndexDetails()).getElementList().stream()
                        .map(Index.ArrayIndexElement::getSourceIndicator).collect(Collectors.toList());
                break;
            case SAMPLE:
                keySourceIndicator = ((Index.SampleIndexDetails) index.getIndexDetails()).getKeyFieldSourceIndicators();
                break;
            default:
                throw new AsterixException(ErrorCode.METADATA_ERROR, index.getIndexType().toString());
        }
        boolean needSerialization = false;
        if (keySourceIndicator != null) {
            for (int source : keySourceIndicator) {
                if (source != 0) {
                    needSerialization = true;
                    break;
                }
            }
        }
        if (needSerialization) {
            listBuilder.reset(int8List);
            nameValue.reset();
            aString.setValue(INDEX_SEARCHKEY_SOURCE_INDICATOR_FIELD_NAME);
            stringSerde.serialize(aString, nameValue.getDataOutput());
            for (int source : keySourceIndicator) {
                itemValue.reset();
                aInt8.setValue((byte) source);
                int8Serde.serialize(aInt8, itemValue.getDataOutput());
                listBuilder.addItem(itemValue);
            }
            fieldValue.reset();
            listBuilder.write(fieldValue.getDataOutput(), true);
            recordBuilder.addField(nameValue, fieldValue);
        }
    }

    private void writeExcludeUnknownKey(Index index) throws HyracksDataException {
        switch (index.getIndexType()) {
            case BTREE:
                if (!index.isPrimaryIndex() && !index.isPrimaryKeyIndex()) {
                    OptionalBoolean excludeUnknown =
                            ((Index.ValueIndexDetails) index.getIndexDetails()).getExcludeUnknownKey();
                    ABoolean bVal = excludeUnknown.isEmpty() ? ABoolean.FALSE : ABoolean.valueOf(excludeUnknown.get());
                    fieldValue.reset();
                    nameValue.reset();
                    aString.setValue(INDEX_EXCLUDE_UNKNOWN_FIELD_NAME);
                    stringSerde.serialize(aString, nameValue.getDataOutput());
                    booleanSerde.serialize(bVal, fieldValue.getDataOutput());
                    recordBuilder.addField(nameValue, fieldValue);
                }
                break;

            case ARRAY:
                // TODO: This value is written for back-compatibility, and is currently always assumed to be true.
                fieldValue.reset();
                nameValue.reset();
                aString.setValue(INDEX_EXCLUDE_UNKNOWN_FIELD_NAME);
                stringSerde.serialize(aString, nameValue.getDataOutput());
                booleanSerde.serialize(ABoolean.TRUE, fieldValue.getDataOutput());
                recordBuilder.addField(nameValue, fieldValue);
                break;
        }
    }

    private void writeCast(Index index) throws HyracksDataException {
        if (index.getIndexType() == IndexType.BTREE && !index.isPrimaryIndex() && !index.isPrimaryKeyIndex()) {
            Index.ValueIndexDetails indexDetails = (Index.ValueIndexDetails) index.getIndexDetails();
            boolean defaultNull = indexDetails.getCastDefaultNull().getOrElse(false);
            // write record field 'Cast'
            if (defaultNull) {
                // write field 'Default'
                castRecordBuilder.reset(RecordUtil.FULLY_OPEN_RECORD_TYPE);
                fieldValue.reset();
                nameValue.reset();
                aString.setValue(FIELD_NAME_DEFAULT);
                stringSerde.serialize(aString, nameValue.getDataOutput());
                nullSerde.serialize(ANull.NULL, fieldValue.getDataOutput());
                castRecordBuilder.addField(nameValue, fieldValue);

                // write field 'Format'
                String datetimeFormat = indexDetails.getCastDatetimeFormat();
                String dateFormat = indexDetails.getCastDateFormat();
                String timeFormat = indexDetails.getCastTimeFormat();
                writeDateTimeFormats(datetimeFormat, dateFormat, timeFormat, castRecordBuilder, aString, nullSerde,
                        stringSerde, nameValue, fieldValue, itemValue);

                nameValue.reset();
                fieldValue.reset();
                aString.setValue(FIELD_NAME_CAST);
                stringSerde.serialize(aString, nameValue.getDataOutput());
                castRecordBuilder.write(fieldValue.getDataOutput(), true);
                recordBuilder.addField(nameValue, fieldValue);
            }
        }
    }

    private void writeSampleDetails(Index index) throws HyracksDataException {
        if (index.getIndexType() == IndexType.SAMPLE) {
            Index.SampleIndexDetails indexDetails = (Index.SampleIndexDetails) index.getIndexDetails();

            nameValue.reset();
            fieldValue.reset();
            aString.setValue(SAMPLE_SEED);
            stringSerde.serialize(aString, nameValue.getDataOutput());
            int64Serde.serialize(new AInt64(indexDetails.getSampleSeed()), fieldValue.getDataOutput());
            recordBuilder.addField(nameValue, fieldValue);

            nameValue.reset();
            fieldValue.reset();
            aString.setValue(SAMPLE_CARDINALITY_TARGET);
            stringSerde.serialize(aString, nameValue.getDataOutput());
            int32Serde.serialize(new AInt32(indexDetails.getSampleCardinalityTarget()), fieldValue.getDataOutput());
            recordBuilder.addField(nameValue, fieldValue);

            nameValue.reset();
            fieldValue.reset();
            aString.setValue(SOURCE_CARDINALITY);
            stringSerde.serialize(aString, nameValue.getDataOutput());
            int64Serde.serialize(new AInt64(indexDetails.getSourceCardinality()), fieldValue.getDataOutput());
            recordBuilder.addField(nameValue, fieldValue);

            nameValue.reset();
            fieldValue.reset();
            aString.setValue(SOURCE_AVG_ITEM_SIZE);
            stringSerde.serialize(aString, nameValue.getDataOutput());
            int32Serde.serialize(new AInt32(indexDetails.getSourceAvgItemSize()), fieldValue.getDataOutput());
            recordBuilder.addField(nameValue, fieldValue);

            Map<String, IndexStats> indexesStats = indexDetails.getIndexesStats();
            if (!indexesStats.isEmpty()) {
                indexesStatsListBuilder.reset(AOrderedListType.FULL_OPEN_ORDEREDLIST_TYPE);
                for (Map.Entry<String, IndexStats> stats : indexesStats.entrySet()) {
                    indexStatsRecordBuilder.reset(RecordUtil.FULLY_OPEN_RECORD_TYPE);
                    // index name
                    nameValue.reset();
                    itemValue.reset();
                    aString.setValue(STATS_INDEX_NAME);
                    stringSerde.serialize(aString, nameValue.getDataOutput());
                    aString.setValue(stats.getKey());
                    stringSerde.serialize(aString, itemValue.getDataOutput());
                    indexStatsRecordBuilder.addField(nameValue, itemValue);

                    // index number of pages
                    nameValue.reset();
                    itemValue.reset();
                    aString.setValue(STATS_NUM_PAGES);
                    stringSerde.serialize(aString, nameValue.getDataOutput());
                    aInt64.setValue(stats.getValue().getNumPages());
                    int64Serde.serialize(aInt64, itemValue.getDataOutput());
                    indexStatsRecordBuilder.addField(nameValue, itemValue);

                    itemValue.reset();
                    indexStatsRecordBuilder.write(itemValue.getDataOutput(), true);
                    indexesStatsListBuilder.addItem(itemValue);
                }
                nameValue.reset();
                fieldValue.reset();
                aString.setValue(INDEXES_STATS);
                stringSerde.serialize(aString, nameValue.getDataOutput());
                indexesStatsListBuilder.write(fieldValue.getDataOutput(), true);
                recordBuilder.addField(nameValue, fieldValue);
            }
        }
    }
}
