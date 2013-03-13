/*
 * Copyright 2009-2013 by The Regents of the University of California
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
import java.io.IOException;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;

import edu.uci.ics.asterix.builders.OrderedListBuilder;
import edu.uci.ics.asterix.common.config.DatasetConfig.IndexType;
import edu.uci.ics.asterix.common.exceptions.AsterixException;
import edu.uci.ics.asterix.formats.nontagged.AqlSerializerDeserializerProvider;
import edu.uci.ics.asterix.metadata.MetadataException;
import edu.uci.ics.asterix.metadata.bootstrap.MetadataPrimaryIndexes;
import edu.uci.ics.asterix.metadata.bootstrap.MetadataRecordTypes;
import edu.uci.ics.asterix.metadata.entities.Index;
import edu.uci.ics.asterix.om.base.ABoolean;
import edu.uci.ics.asterix.om.base.AInt32;
import edu.uci.ics.asterix.om.base.AOrderedList;
import edu.uci.ics.asterix.om.base.ARecord;
import edu.uci.ics.asterix.om.base.AString;
import edu.uci.ics.asterix.om.base.IACursor;
import edu.uci.ics.asterix.om.types.AOrderedListType;
import edu.uci.ics.asterix.om.types.BuiltinType;
import edu.uci.ics.hyracks.api.dataflow.value.ISerializerDeserializer;
import edu.uci.ics.hyracks.data.std.util.ArrayBackedValueStorage;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.ITupleReference;

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

    private OrderedListBuilder listBuilder = new OrderedListBuilder();
    private ArrayBackedValueStorage nameValue = new ArrayBackedValueStorage();
    private ArrayBackedValueStorage itemValue = new ArrayBackedValueStorage();
    private List<String> searchKey;
    @SuppressWarnings("unchecked")
    protected ISerializerDeserializer<AInt32> intSerde = AqlSerializerDeserializerProvider.INSTANCE
            .getSerializerDeserializer(BuiltinType.AINT32);
    @SuppressWarnings("unchecked")
    private ISerializerDeserializer<ARecord> recordSerde = AqlSerializerDeserializerProvider.INSTANCE
            .getSerializerDeserializer(MetadataRecordTypes.INDEX_RECORDTYPE);

    public IndexTupleTranslator(boolean getTuple) {
        super(getTuple, MetadataPrimaryIndexes.INDEX_DATASET.getFieldCount());
    }

    @Override
    public Index getMetadataEntytiFromTuple(ITupleReference frameTuple) throws IOException {
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
        IACursor cursor = ((AOrderedList) rec.getValueByPos(MetadataRecordTypes.INDEX_ARECORD_SEARCHKEY_FIELD_INDEX))
                .getCursor();
        List<String> searchKey = new ArrayList<String>();
        while (cursor.next()) {
            searchKey.add(((AString) cursor.get()).getStringValue());
        }
        Boolean isPrimaryIndex = ((ABoolean) rec.getValueByPos(MetadataRecordTypes.INDEX_ARECORD_ISPRIMARY_FIELD_INDEX))
                .getBoolean();
        int pendingOp = ((AInt32) rec.getValueByPos(MetadataRecordTypes.INDEX_ARECORD_PENDINGOP_FIELD_INDEX))
                .getIntegerValue();
        // Check if there is a gram length as well.
        int gramLength = -1;
        int gramLenPos = rec.getType().findFieldPosition(GRAM_LENGTH_FIELD_NAME);
        if (gramLenPos >= 0) {
            gramLength = ((AInt32) rec.getValueByPos(gramLenPos)).getIntegerValue();
        }
        return new Index(dvName, dsName, indexName, indexStructure, searchKey, gramLength, isPrimaryIndex, pendingOp);
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
        listBuilder
                .reset((AOrderedListType) MetadataRecordTypes.INDEX_RECORDTYPE.getFieldTypes()[MetadataRecordTypes.INDEX_ARECORD_SEARCHKEY_FIELD_INDEX]);
        this.searchKey = instance.getKeyFieldNames();
        for (String field : this.searchKey) {
            itemValue.reset();
            aString.setValue(field);
            stringSerde.serialize(aString, itemValue.getDataOutput());
            listBuilder.addItem(itemValue);
        }
        fieldValue.reset();
        listBuilder.write(fieldValue.getDataOutput(), true);
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
