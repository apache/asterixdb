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
import java.io.IOException;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;

import org.apache.asterix.builders.OrderedListBuilder;
import org.apache.asterix.common.config.DatasetConfig.IndexType;
import org.apache.asterix.common.exceptions.AsterixException;
import org.apache.asterix.common.transactions.JobId;
import org.apache.asterix.formats.nontagged.AqlSerializerDeserializerProvider;
import org.apache.asterix.metadata.MetadataException;
import org.apache.asterix.metadata.MetadataNode;
import org.apache.asterix.metadata.bootstrap.MetadataPrimaryIndexes;
import org.apache.asterix.metadata.bootstrap.MetadataRecordTypes;
import org.apache.asterix.metadata.entities.AsterixBuiltinTypeMap;
import org.apache.asterix.metadata.entities.Index;
import org.apache.asterix.om.base.ABoolean;
import org.apache.asterix.om.base.ACollectionCursor;
import org.apache.asterix.om.base.AInt32;
import org.apache.asterix.om.base.AOrderedList;
import org.apache.asterix.om.base.ARecord;
import org.apache.asterix.om.base.AString;
import org.apache.asterix.om.base.IACursor;
import org.apache.asterix.om.types.AOrderedListType;
import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.asterix.om.types.IAType;
import org.apache.hyracks.api.dataflow.value.ISerializerDeserializer;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;

/**
 * Translates an Index metadata entity to an ITupleReference and vice versa.
 */
public class IndexTupleTranslator extends AbstractTupleTranslator<Index> {
    // Field indexes of serialized Index in a tuple.
    // First key field.
    public static final int INDEX_DATAVERSENAME_TUPLE_FIELD_INDEX = 0;
    // Second key field.
    public static final int INDEX_DATASETNAME_TUPLE_FIELD_INDEX = 1;
    // Third key field.
    public static final int INDEX_INDEXNAME_TUPLE_FIELD_INDEX = 2;
    // Payload field containing serialized Index.
    public static final int INDEX_PAYLOAD_TUPLE_FIELD_INDEX = 3;
    // Field name of open field.
    public static final String GRAM_LENGTH_FIELD_NAME = "GramLength";
    public static final String INDEX_SEARCHKEY_TYPE_FIELD_NAME = "SearchKeyType";
    public static final String INDEX_ISENFORCED_FIELD_NAME = "IsEnforced";

    private OrderedListBuilder listBuilder = new OrderedListBuilder();
    private OrderedListBuilder primaryKeyListBuilder = new OrderedListBuilder();
    private AOrderedListType stringList = new AOrderedListType(BuiltinType.ASTRING, null);
    private ArrayBackedValueStorage nameValue = new ArrayBackedValueStorage();
    private ArrayBackedValueStorage itemValue = new ArrayBackedValueStorage();
    private List<List<String>> searchKey;
    private List<IAType> searchKeyType;
    @SuppressWarnings("unchecked")
    protected ISerializerDeserializer<AInt32> intSerde = AqlSerializerDeserializerProvider.INSTANCE
            .getSerializerDeserializer(BuiltinType.AINT32);
    @SuppressWarnings("unchecked")
    private ISerializerDeserializer<ARecord> recordSerde = AqlSerializerDeserializerProvider.INSTANCE
            .getSerializerDeserializer(MetadataRecordTypes.INDEX_RECORDTYPE);
    private final MetadataNode metadataNode;
    private final JobId jobId;

    public IndexTupleTranslator(JobId jobId, MetadataNode metadataNode, boolean getTuple) {
        super(getTuple, MetadataPrimaryIndexes.INDEX_DATASET.getFieldCount());
        this.jobId = jobId;
        this.metadataNode = metadataNode;
    }

    @Override
    public Index getMetadataEntityFromTuple(ITupleReference frameTuple) throws IOException, MetadataException {
        byte[] serRecord = frameTuple.getFieldData(INDEX_PAYLOAD_TUPLE_FIELD_INDEX);
        int recordStartOffset = frameTuple.getFieldStart(INDEX_PAYLOAD_TUPLE_FIELD_INDEX);
        int recordLength = frameTuple.getFieldLength(INDEX_PAYLOAD_TUPLE_FIELD_INDEX);
        ByteArrayInputStream stream = new ByteArrayInputStream(serRecord, recordStartOffset, recordLength);
        DataInput in = new DataInputStream(stream);
        ARecord rec = (ARecord) recordSerde.deserialize(in);
        String dvName = ((AString) rec.getValueByPos(MetadataRecordTypes.INDEX_ARECORD_DATAVERSENAME_FIELD_INDEX))
                .getStringValue();
        String dsName = ((AString) rec.getValueByPos(MetadataRecordTypes.INDEX_ARECORD_DATASETNAME_FIELD_INDEX))
                .getStringValue();
        String indexName = ((AString) rec.getValueByPos(MetadataRecordTypes.INDEX_ARECORD_INDEXNAME_FIELD_INDEX))
                .getStringValue();
        IndexType indexStructure = IndexType.valueOf(((AString) rec
                .getValueByPos(MetadataRecordTypes.INDEX_ARECORD_INDEXSTRUCTURE_FIELD_INDEX)).getStringValue());
        IACursor fieldNameCursor = ((AOrderedList) rec
                .getValueByPos(MetadataRecordTypes.INDEX_ARECORD_SEARCHKEY_FIELD_INDEX)).getCursor();
        List<List<String>> searchKey = new ArrayList<List<String>>();
        AOrderedList fieldNameList;
        while (fieldNameCursor.next()) {
            fieldNameList = (AOrderedList) fieldNameCursor.get();
            IACursor nestedFieldNameCursor = (fieldNameList.getCursor());
            List<String> nestedFieldName = new ArrayList<String>();
            while (nestedFieldNameCursor.next()) {
                nestedFieldName.add(((AString) nestedFieldNameCursor.get()).getStringValue());
            }
            searchKey.add(nestedFieldName);
        }
        int indexKeyTypeFieldPos = rec.getType().getFieldIndex(INDEX_SEARCHKEY_TYPE_FIELD_NAME);
        IACursor fieldTypeCursor = new ACollectionCursor();
        if (indexKeyTypeFieldPos > 0)
            fieldTypeCursor = ((AOrderedList) rec.getValueByPos(indexKeyTypeFieldPos)).getCursor();
        List<IAType> searchKeyType = new ArrayList<IAType>(searchKey.size());
        while (fieldTypeCursor.next()) {
            String typeName = ((AString) fieldTypeCursor.get()).getStringValue();
            IAType fieldType = AsterixBuiltinTypeMap.getTypeFromTypeName(metadataNode, jobId, dvName, typeName, false);
            searchKeyType.add(fieldType);
        }
        // index key type information is not persisted, thus we extract type information from the record metadata
        if (searchKeyType.isEmpty()) {
            String datatypeName = metadataNode.getDataset(jobId, dvName, dsName).getItemTypeName();
            ARecordType recordDt = (ARecordType) metadataNode.getDatatype(jobId, dvName, datatypeName).getDatatype();
            for (int i = 0; i < searchKey.size(); i++) {
                IAType fieldType = recordDt.getSubFieldType(searchKey.get(i));
                searchKeyType.add(fieldType);
            }
        }
        int isEnforcedFieldPos = rec.getType().getFieldIndex(INDEX_ISENFORCED_FIELD_NAME);
        Boolean isEnforcingKeys = false;
        if (isEnforcedFieldPos > 0)
            isEnforcingKeys = ((ABoolean) rec.getValueByPos(isEnforcedFieldPos)).getBoolean();
        Boolean isPrimaryIndex = ((ABoolean) rec.getValueByPos(MetadataRecordTypes.INDEX_ARECORD_ISPRIMARY_FIELD_INDEX))
                .getBoolean();
        int pendingOp = ((AInt32) rec.getValueByPos(MetadataRecordTypes.INDEX_ARECORD_PENDINGOP_FIELD_INDEX))
                .getIntegerValue();
        // Check if there is a gram length as well.
        int gramLength = -1;
        int gramLenPos = rec.getType().getFieldIndex(GRAM_LENGTH_FIELD_NAME);
        if (gramLenPos >= 0) {
            gramLength = ((AInt32) rec.getValueByPos(gramLenPos)).getIntegerValue();
        }
        return new Index(dvName, dsName, indexName, indexStructure, searchKey, searchKeyType, gramLength,
                isEnforcingKeys, isPrimaryIndex, pendingOp);
    }

    @Override
    public ITupleReference getTupleFromMetadataEntity(Index instance) throws IOException, MetadataException {
        // write the key in the first 3 fields of the tuple
        tupleBuilder.reset();
        aString.setValue(instance.getDataverseName());
        stringSerde.serialize(aString, tupleBuilder.getDataOutput());
        tupleBuilder.addFieldEndOffset();
        aString.setValue(instance.getDatasetName());
        stringSerde.serialize(aString, tupleBuilder.getDataOutput());
        tupleBuilder.addFieldEndOffset();
        aString.setValue(instance.getIndexName());
        stringSerde.serialize(aString, tupleBuilder.getDataOutput());
        tupleBuilder.addFieldEndOffset();

        // write the payload in the fourth field of the tuple
        recordBuilder.reset(MetadataRecordTypes.INDEX_RECORDTYPE);
        // write field 0
        fieldValue.reset();
        aString.setValue(instance.getDataverseName());
        stringSerde.serialize(aString, fieldValue.getDataOutput());
        recordBuilder.addField(MetadataRecordTypes.INDEX_ARECORD_DATAVERSENAME_FIELD_INDEX, fieldValue);

        // write field 1
        fieldValue.reset();
        aString.setValue(instance.getDatasetName());
        stringSerde.serialize(aString, fieldValue.getDataOutput());
        recordBuilder.addField(MetadataRecordTypes.INDEX_ARECORD_DATASETNAME_FIELD_INDEX, fieldValue);

        // write field 2
        fieldValue.reset();
        aString.setValue(instance.getIndexName());
        stringSerde.serialize(aString, fieldValue.getDataOutput());
        recordBuilder.addField(MetadataRecordTypes.INDEX_ARECORD_INDEXNAME_FIELD_INDEX, fieldValue);

        // write field 3
        fieldValue.reset();
        aString.setValue(instance.getIndexType().toString());
        stringSerde.serialize(aString, fieldValue.getDataOutput());
        recordBuilder.addField(MetadataRecordTypes.INDEX_ARECORD_INDEXSTRUCTURE_FIELD_INDEX, fieldValue);

        // write field 4
        primaryKeyListBuilder
                .reset((AOrderedListType) MetadataRecordTypes.INDEX_RECORDTYPE.getFieldTypes()[MetadataRecordTypes.INDEX_ARECORD_SEARCHKEY_FIELD_INDEX]);
        this.searchKey = instance.getKeyFieldNames();
        for (List<String> field : this.searchKey) {
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
        if (instance.isPrimaryIndex()) {
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
        intSerde.serialize(new AInt32(instance.getPendingOp()), fieldValue.getDataOutput());
        recordBuilder.addField(MetadataRecordTypes.INDEX_ARECORD_PENDINGOP_FIELD_INDEX, fieldValue);

        // write optional field 8        
        if (instance.getGramLength() > 0) {
            fieldValue.reset();
            nameValue.reset();
            aString.setValue(GRAM_LENGTH_FIELD_NAME);
            stringSerde.serialize(aString, nameValue.getDataOutput());
            intSerde.serialize(new AInt32(instance.getGramLength()), fieldValue.getDataOutput());
            try {
                recordBuilder.addField(nameValue, fieldValue);
            } catch (AsterixException e) {
                throw new MetadataException(e);
            }
        }

        if (instance.isEnforcingKeyFileds()) {
            // write optional field 9
            OrderedListBuilder typeListBuilder = new OrderedListBuilder();
            typeListBuilder.reset(new AOrderedListType(BuiltinType.ASTRING, null));
            ArrayBackedValueStorage nameValue = new ArrayBackedValueStorage();
            nameValue.reset();
            aString.setValue(INDEX_SEARCHKEY_TYPE_FIELD_NAME);

            stringSerde.serialize(aString, nameValue.getDataOutput());

            this.searchKeyType = instance.getKeyFieldTypes();
            for (IAType type : this.searchKeyType) {
                itemValue.reset();
                aString.setValue(type.getTypeName());
                stringSerde.serialize(aString, itemValue.getDataOutput());
                typeListBuilder.addItem(itemValue);
            }
            fieldValue.reset();
            typeListBuilder.write(fieldValue.getDataOutput(), true);
            try {
                recordBuilder.addField(nameValue, fieldValue);
            } catch (AsterixException e) {
                throw new MetadataException(e);
            }

            // write optional field 10
            fieldValue.reset();
            nameValue.reset();
            aString.setValue(INDEX_ISENFORCED_FIELD_NAME);

            stringSerde.serialize(aString, nameValue.getDataOutput());

            booleanSerde.serialize(ABoolean.TRUE, fieldValue.getDataOutput());

            try {
                recordBuilder.addField(nameValue, fieldValue);
            } catch (AsterixException e) {
                throw new MetadataException(e);
            }
        }

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
