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
import java.util.Calendar;
import java.util.List;

import org.apache.asterix.builders.OrderedListBuilder;
import org.apache.asterix.common.config.DatasetConfig.IndexType;
import org.apache.asterix.common.metadata.DataverseName;
import org.apache.asterix.common.transactions.TxnId;
import org.apache.asterix.formats.nontagged.SerializerDeserializerProvider;
import org.apache.asterix.metadata.MetadataNode;
import org.apache.asterix.metadata.bootstrap.MetadataPrimaryIndexes;
import org.apache.asterix.metadata.bootstrap.MetadataRecordTypes;
import org.apache.asterix.metadata.entities.BuiltinTypeMap;
import org.apache.asterix.metadata.entities.Dataset;
import org.apache.asterix.metadata.entities.Index;
import org.apache.asterix.metadata.utils.KeyFieldTypeUtil;
import org.apache.asterix.om.base.ABoolean;
import org.apache.asterix.om.base.ACollectionCursor;
import org.apache.asterix.om.base.AInt32;
import org.apache.asterix.om.base.AInt8;
import org.apache.asterix.om.base.AMutableInt8;
import org.apache.asterix.om.base.AOrderedList;
import org.apache.asterix.om.base.ARecord;
import org.apache.asterix.om.base.AString;
import org.apache.asterix.om.base.IACursor;
import org.apache.asterix.om.types.AOrderedListType;
import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.asterix.om.types.IAType;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.api.dataflow.value.ISerializerDeserializer;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;

/**
 * Translates an Index metadata entity to an ITupleReference and vice versa.
 */
public class IndexTupleTranslator extends AbstractTupleTranslator<Index> {

    // Payload field containing serialized Index.
    private static final int INDEX_PAYLOAD_TUPLE_FIELD_INDEX = 3;

    // Field name of open field.
    public static final String GRAM_LENGTH_FIELD_NAME = "GramLength";
    public static final String INDEX_SEARCHKEY_TYPE_FIELD_NAME = "SearchKeyType";
    public static final String INDEX_ISENFORCED_FIELD_NAME = "IsEnforced";
    public static final String INDEX_SEARCHKEY_SOURCE_INDICATOR_FIELD_NAME = "SearchKeySourceIndicator";

    protected final TxnId txnId;
    protected final MetadataNode metadataNode;

    protected OrderedListBuilder listBuilder;
    protected OrderedListBuilder primaryKeyListBuilder;
    protected AOrderedListType stringList;
    protected AOrderedListType int8List;
    protected ArrayBackedValueStorage nameValue;
    protected ArrayBackedValueStorage itemValue;
    protected AMutableInt8 aInt8;
    protected ISerializerDeserializer<AInt8> int8Serde;

    @SuppressWarnings("unchecked")
    protected IndexTupleTranslator(TxnId txnId, MetadataNode metadataNode, boolean getTuple) {
        super(getTuple, MetadataPrimaryIndexes.INDEX_DATASET, INDEX_PAYLOAD_TUPLE_FIELD_INDEX);
        this.txnId = txnId;
        this.metadataNode = metadataNode;
        if (getTuple) {
            listBuilder = new OrderedListBuilder();
            primaryKeyListBuilder = new OrderedListBuilder();
            stringList = new AOrderedListType(BuiltinType.ASTRING, null);
            int8List = new AOrderedListType(BuiltinType.AINT8, null);
            nameValue = new ArrayBackedValueStorage();
            itemValue = new ArrayBackedValueStorage();
            aInt8 = new AMutableInt8((byte) 0);
            int8Serde = SerializerDeserializerProvider.INSTANCE.getSerializerDeserializer(BuiltinType.AINT8);
        }
    }

    @Override
    protected Index createMetadataEntityFromARecord(ARecord indexRecord) throws AlgebricksException {
        String dataverseCanonicalName =
                ((AString) indexRecord.getValueByPos(MetadataRecordTypes.INDEX_ARECORD_DATAVERSENAME_FIELD_INDEX))
                        .getStringValue();
        DataverseName dataverseName = DataverseName.createFromCanonicalForm(dataverseCanonicalName);
        String datasetName =
                ((AString) indexRecord.getValueByPos(MetadataRecordTypes.INDEX_ARECORD_DATASETNAME_FIELD_INDEX))
                        .getStringValue();
        String indexName =
                ((AString) indexRecord.getValueByPos(MetadataRecordTypes.INDEX_ARECORD_INDEXNAME_FIELD_INDEX))
                        .getStringValue();
        IndexType indexStructure = IndexType.valueOf(
                ((AString) indexRecord.getValueByPos(MetadataRecordTypes.INDEX_ARECORD_INDEXSTRUCTURE_FIELD_INDEX))
                        .getStringValue());
        IACursor fieldNameCursor =
                ((AOrderedList) indexRecord.getValueByPos(MetadataRecordTypes.INDEX_ARECORD_SEARCHKEY_FIELD_INDEX))
                        .getCursor();
        List<List<String>> searchKey = new ArrayList<>();
        AOrderedList fieldNameList;
        while (fieldNameCursor.next()) {
            fieldNameList = (AOrderedList) fieldNameCursor.get();
            IACursor nestedFieldNameCursor = (fieldNameList.getCursor());
            List<String> nestedFieldName = new ArrayList<>();
            while (nestedFieldNameCursor.next()) {
                nestedFieldName.add(((AString) nestedFieldNameCursor.get()).getStringValue());
            }
            searchKey.add(nestedFieldName);
        }
        int indexKeyTypeFieldPos = indexRecord.getType().getFieldIndex(INDEX_SEARCHKEY_TYPE_FIELD_NAME);
        IACursor fieldTypeCursor = new ACollectionCursor();
        if (indexKeyTypeFieldPos > 0) {
            fieldTypeCursor = ((AOrderedList) indexRecord.getValueByPos(indexKeyTypeFieldPos)).getCursor();
        }
        List<IAType> searchKeyType = new ArrayList<>(searchKey.size());
        while (fieldTypeCursor.next()) {
            String typeName = ((AString) fieldTypeCursor.get()).getStringValue();
            IAType fieldType = BuiltinTypeMap.getTypeFromTypeName(metadataNode, txnId, dataverseName, typeName);
            searchKeyType.add(fieldType);
        }
        boolean isOverridingKeyTypes = !searchKeyType.isEmpty();

        int isEnforcedFieldPos = indexRecord.getType().getFieldIndex(INDEX_ISENFORCED_FIELD_NAME);
        Boolean isEnforcingKeys = false;
        if (isEnforcedFieldPos > 0) {
            isEnforcingKeys = ((ABoolean) indexRecord.getValueByPos(isEnforcedFieldPos)).getBoolean();
        }
        Boolean isPrimaryIndex =
                ((ABoolean) indexRecord.getValueByPos(MetadataRecordTypes.INDEX_ARECORD_ISPRIMARY_FIELD_INDEX))
                        .getBoolean();
        int pendingOp = ((AInt32) indexRecord.getValueByPos(MetadataRecordTypes.INDEX_ARECORD_PENDINGOP_FIELD_INDEX))
                .getIntegerValue();
        // Check if there is a gram length as well.
        int gramLength = -1;
        int gramLenPos = indexRecord.getType().getFieldIndex(GRAM_LENGTH_FIELD_NAME);
        if (gramLenPos >= 0) {
            gramLength = ((AInt32) indexRecord.getValueByPos(gramLenPos)).getIntegerValue();
        }

        // Read a field-source-indicator field.
        List<Integer> keyFieldSourceIndicator = new ArrayList<>();
        int keyFieldSourceIndicatorIndex =
                indexRecord.getType().getFieldIndex(INDEX_SEARCHKEY_SOURCE_INDICATOR_FIELD_NAME);
        if (keyFieldSourceIndicatorIndex >= 0) {
            IACursor cursor = ((AOrderedList) indexRecord.getValueByPos(keyFieldSourceIndicatorIndex)).getCursor();
            while (cursor.next()) {
                keyFieldSourceIndicator.add((int) ((AInt8) cursor.get()).getByteValue());
            }
        } else {
            for (int index = 0; index < searchKey.size(); ++index) {
                keyFieldSourceIndicator.add(0);
            }
        }

        // index key type information is not persisted, thus we extract type information
        // from the record metadata
        if (searchKeyType.isEmpty()) {
            Dataset dataset = metadataNode.getDataset(txnId, dataverseName, datasetName);
            String datatypeName = dataset.getItemTypeName();
            DataverseName datatypeDataverseName = dataset.getItemTypeDataverseName();
            ARecordType recordDt =
                    (ARecordType) metadataNode.getDatatype(txnId, datatypeDataverseName, datatypeName).getDatatype();
            String metatypeName = dataset.getMetaItemTypeName();
            DataverseName metatypeDataverseName = dataset.getMetaItemTypeDataverseName();
            ARecordType metaDt = null;
            if (metatypeName != null && metatypeDataverseName != null) {
                metaDt = (ARecordType) metadataNode.getDatatype(txnId, metatypeDataverseName, metatypeName)
                        .getDatatype();
            }
            searchKeyType = KeyFieldTypeUtil.getKeyTypes(recordDt, metaDt, searchKey, keyFieldSourceIndicator);
        }

        return new Index(dataverseName, datasetName, indexName, indexStructure, searchKey, keyFieldSourceIndicator,
                searchKeyType, gramLength, isOverridingKeyTypes, isEnforcingKeys, isPrimaryIndex, pendingOp);
    }

    @Override
    public ITupleReference getTupleFromMetadataEntity(Index index) throws HyracksDataException {
        String dataverseCanonicalName = index.getDataverseName().getCanonicalForm();

        // write the key in the first 3 fields of the tuple
        tupleBuilder.reset();
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
        recordBuilder.reset(MetadataRecordTypes.INDEX_RECORDTYPE);

        // write field 0
        fieldValue.reset();
        aString.setValue(dataverseCanonicalName);
        stringSerde.serialize(aString, fieldValue.getDataOutput());
        recordBuilder.addField(MetadataRecordTypes.INDEX_ARECORD_DATAVERSENAME_FIELD_INDEX, fieldValue);

        // write field 1
        fieldValue.reset();
        aString.setValue(index.getDatasetName());
        stringSerde.serialize(aString, fieldValue.getDataOutput());
        recordBuilder.addField(MetadataRecordTypes.INDEX_ARECORD_DATASETNAME_FIELD_INDEX, fieldValue);

        // write field 2
        fieldValue.reset();
        aString.setValue(index.getIndexName());
        stringSerde.serialize(aString, fieldValue.getDataOutput());
        recordBuilder.addField(MetadataRecordTypes.INDEX_ARECORD_INDEXNAME_FIELD_INDEX, fieldValue);

        // write field 3
        fieldValue.reset();
        aString.setValue(index.getIndexType().toString());
        stringSerde.serialize(aString, fieldValue.getDataOutput());
        recordBuilder.addField(MetadataRecordTypes.INDEX_ARECORD_INDEXSTRUCTURE_FIELD_INDEX, fieldValue);

        // write field 4
        primaryKeyListBuilder.reset((AOrderedListType) MetadataRecordTypes.INDEX_RECORDTYPE
                .getFieldTypes()[MetadataRecordTypes.INDEX_ARECORD_SEARCHKEY_FIELD_INDEX]);
        List<List<String>> searchKey = index.getKeyFieldNames();
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
        recordBuilder.addField(MetadataRecordTypes.INDEX_ARECORD_SEARCHKEY_FIELD_INDEX, fieldValue);

        // write field 5
        fieldValue.reset();
        if (index.isPrimaryIndex()) {
            booleanSerde.serialize(ABoolean.TRUE, fieldValue.getDataOutput());
        } else {
            booleanSerde.serialize(ABoolean.FALSE, fieldValue.getDataOutput());
        }
        recordBuilder.addField(MetadataRecordTypes.INDEX_ARECORD_ISPRIMARY_FIELD_INDEX, fieldValue);

        // write field 6
        fieldValue.reset();
        aString.setValue(Calendar.getInstance().getTime().toString());
        stringSerde.serialize(aString, fieldValue.getDataOutput());
        recordBuilder.addField(MetadataRecordTypes.INDEX_ARECORD_TIMESTAMP_FIELD_INDEX, fieldValue);

        // write field 7
        fieldValue.reset();
        int32Serde.serialize(new AInt32(index.getPendingOp()), fieldValue.getDataOutput());
        recordBuilder.addField(MetadataRecordTypes.INDEX_ARECORD_PENDINGOP_FIELD_INDEX, fieldValue);

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
    protected void writeOpenFields(Index index) throws HyracksDataException {
        writeGramLength(index);
        writeSearchKeyType(index);
        writeEnforced(index);
        writeSearchKeySourceIndicator(index);
    }

    private void writeGramLength(Index index) throws HyracksDataException {
        if (index.getGramLength() > 0) {
            fieldValue.reset();
            nameValue.reset();
            aString.setValue(GRAM_LENGTH_FIELD_NAME);
            stringSerde.serialize(aString, nameValue.getDataOutput());
            int32Serde.serialize(new AInt32(index.getGramLength()), fieldValue.getDataOutput());
            recordBuilder.addField(nameValue, fieldValue);
        }
    }

    private void writeSearchKeyType(Index index) throws HyracksDataException {
        if (index.isOverridingKeyFieldTypes()) {
            OrderedListBuilder typeListBuilder = new OrderedListBuilder();
            typeListBuilder.reset(new AOrderedListType(BuiltinType.ANY, null));
            nameValue.reset();
            aString.setValue(INDEX_SEARCHKEY_TYPE_FIELD_NAME);

            stringSerde.serialize(aString, nameValue.getDataOutput());

            List<IAType> searchKeyType = index.getKeyFieldTypes();
            for (IAType type : searchKeyType) {
                itemValue.reset();
                aString.setValue(type.getTypeName());
                stringSerde.serialize(aString, itemValue.getDataOutput());
                typeListBuilder.addItem(itemValue);
            }
            fieldValue.reset();
            typeListBuilder.write(fieldValue.getDataOutput(), true);
            recordBuilder.addField(nameValue, fieldValue);
        }
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

    private void writeSearchKeySourceIndicator(Index index) throws HyracksDataException {
        List<Integer> keySourceIndicator = index.getKeyFieldSourceIndicators();
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
}
