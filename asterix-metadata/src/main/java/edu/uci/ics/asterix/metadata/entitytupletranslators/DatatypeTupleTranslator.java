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
import java.io.DataOutput;
import java.io.IOException;
import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;

import edu.uci.ics.asterix.builders.IARecordBuilder;
import edu.uci.ics.asterix.builders.OrderedListBuilder;
import edu.uci.ics.asterix.builders.RecordBuilder;
import edu.uci.ics.asterix.common.exceptions.AsterixException;
import edu.uci.ics.asterix.common.transactions.JobId;
import edu.uci.ics.asterix.formats.nontagged.AqlSerializerDeserializerProvider;
import edu.uci.ics.asterix.metadata.MetadataException;
import edu.uci.ics.asterix.metadata.MetadataNode;
import edu.uci.ics.asterix.metadata.bootstrap.MetadataPrimaryIndexes;
import edu.uci.ics.asterix.metadata.bootstrap.MetadataRecordTypes;
import edu.uci.ics.asterix.metadata.entities.AsterixBuiltinTypeMap;
import edu.uci.ics.asterix.metadata.entities.Datatype;
import edu.uci.ics.asterix.om.base.ABoolean;
import edu.uci.ics.asterix.om.base.AOrderedList;
import edu.uci.ics.asterix.om.base.ARecord;
import edu.uci.ics.asterix.om.base.AString;
import edu.uci.ics.asterix.om.base.IACursor;
import edu.uci.ics.asterix.om.types.AOrderedListType;
import edu.uci.ics.asterix.om.types.ARecordType;
import edu.uci.ics.asterix.om.types.ATypeTag;
import edu.uci.ics.asterix.om.types.AUnionType;
import edu.uci.ics.asterix.om.types.AUnorderedListType;
import edu.uci.ics.asterix.om.types.AbstractCollectionType;
import edu.uci.ics.asterix.om.types.BuiltinType;
import edu.uci.ics.asterix.om.types.IAType;
import edu.uci.ics.hyracks.algebricks.common.exceptions.NotImplementedException;
import edu.uci.ics.hyracks.api.dataflow.value.ISerializerDeserializer;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.data.std.util.ArrayBackedValueStorage;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.ITupleReference;
import edu.uci.ics.hyracks.storage.am.common.exceptions.TreeIndexDuplicateKeyException;

/**
 * Translates a Datatype metadata entity to an ITupleReference and vice versa.
 */
public class DatatypeTupleTranslator extends AbstractTupleTranslator<Datatype> {
    // Field indexes of serialized Dataset in a tuple.
    // First key field.
    public static final int DATATYPE_DATAVERSENAME_TUPLE_FIELD_INDEX = 0;
    // Second key field.
    public static final int DATATYPE_DATATYPE_TUPLE_FIELD_INDEX = 1;
    // Payload field containing serialized Datatype.
    public static final int DATATYPE_PAYLOAD_TUPLE_FIELD_INDEX = 2;

    public enum DerivedTypeTag {
        ENUM,
        RECORD,
        UNION,
        UNORDEREDLIST,
        ORDEREDLIST
    };

    @SuppressWarnings("unchecked")
    private ISerializerDeserializer<ARecord> recordSerDes = AqlSerializerDeserializerProvider.INSTANCE
            .getSerializerDeserializer(MetadataRecordTypes.DATATYPE_RECORDTYPE);
    private final MetadataNode metadataNode;
    private final JobId jobId;

    public DatatypeTupleTranslator(JobId jobId, MetadataNode metadataNode, boolean getTuple) {
        super(getTuple, MetadataPrimaryIndexes.DATATYPE_DATASET.getFieldCount());
        this.jobId = jobId;
        this.metadataNode = metadataNode;
    }

    @Override
    public Datatype getMetadataEntytiFromTuple(ITupleReference frameTuple) throws MetadataException, IOException {
        byte[] serRecord = frameTuple.getFieldData(DATATYPE_PAYLOAD_TUPLE_FIELD_INDEX);
        int recordStartOffset = frameTuple.getFieldStart(DATATYPE_PAYLOAD_TUPLE_FIELD_INDEX);
        int recordLength = frameTuple.getFieldLength(DATATYPE_PAYLOAD_TUPLE_FIELD_INDEX);
        ByteArrayInputStream stream = new ByteArrayInputStream(serRecord, recordStartOffset, recordLength);
        DataInput in = new DataInputStream(stream);
        ARecord datatypeRecord = (ARecord) recordSerDes.deserialize(in);
        return createDataTypeFromARecord(datatypeRecord);
    }

    private Datatype createDataTypeFromARecord(ARecord datatypeRecord) throws MetadataException {
        String dataverseName = ((AString) datatypeRecord
                .getValueByPos(MetadataRecordTypes.DATATYPE_ARECORD_DATAVERSENAME_FIELD_INDEX)).getStringValue();
        String datatypeName = ((AString) datatypeRecord
                .getValueByPos(MetadataRecordTypes.DATATYPE_ARECORD_DATATYPENAME_FIELD_INDEX)).getStringValue();
        IAType type = AsterixBuiltinTypeMap.getBuiltinTypes().get(datatypeName);
        if (type == null) {
            // Derived Type
            ARecord derivedTypeRecord = (ARecord) datatypeRecord
                    .getValueByPos(MetadataRecordTypes.DATATYPE_ARECORD_DERIVED_FIELD_INDEX);
            DerivedTypeTag tag = DerivedTypeTag.valueOf(((AString) derivedTypeRecord
                    .getValueByPos(MetadataRecordTypes.DERIVEDTYPE_ARECORD_TAG_FIELD_INDEX)).getStringValue());
            boolean isAnonymous = ((ABoolean) derivedTypeRecord
                    .getValueByPos(MetadataRecordTypes.DERIVEDTYPE_ARECORD_ISANONYMOUS_FIELD_INDEX)).getBoolean();
            switch (tag) {
                case ENUM:
                    throw new NotImplementedException("Enum type");
                case RECORD: {
                    ARecord recordType = (ARecord) derivedTypeRecord
                            .getValueByPos(MetadataRecordTypes.DERIVEDTYPE_ARECORD_RECORD_FIELD_INDEX);
                    boolean isOpen = ((ABoolean) recordType
                            .getValueByPos(MetadataRecordTypes.RECORDTYPE_ARECORD_ISOPEN_FIELD_INDEX)).getBoolean()
                            .booleanValue();
                    int numberOfFields = ((AOrderedList) recordType
                            .getValueByPos(MetadataRecordTypes.RECORDTYPE_ARECORD_FIELDS_FIELD_INDEX)).size();
                    IACursor cursor = ((AOrderedList) recordType
                            .getValueByPos(MetadataRecordTypes.RECORDTYPE_ARECORD_FIELDS_FIELD_INDEX)).getCursor();
                    String[] fieldNames = new String[numberOfFields];
                    IAType[] fieldTypes = new IAType[numberOfFields];
                    int fieldId = 0;
                    String fieldTypeName;
                    while (cursor.next()) {
                        ARecord field = (ARecord) cursor.get();
                        fieldNames[fieldId] = ((AString) field
                                .getValueByPos(MetadataRecordTypes.FIELD_ARECORD_FIELDNAME_FIELD_INDEX))
                                .getStringValue();
                        fieldTypeName = ((AString) field
                                .getValueByPos(MetadataRecordTypes.FIELD_ARECORD_FIELDTYPE_FIELD_INDEX))
                                .getStringValue();
                        fieldTypes[fieldId] = getTypeFromTypeName(dataverseName, fieldTypeName);
                        fieldId++;
                    }
                    try {
                        return new Datatype(dataverseName, datatypeName, new ARecordType(datatypeName, fieldNames,
                                fieldTypes, isOpen), isAnonymous);
                    } catch (AsterixException e) {
                        throw new MetadataException(e);
                    }
                }
                case UNION: {
                    IACursor cursor = ((AOrderedList) derivedTypeRecord
                            .getValueByPos(MetadataRecordTypes.DERIVEDTYPE_ARECORD_UNION_FIELD_INDEX)).getCursor();
                    List<IAType> unionList = new ArrayList<IAType>();
                    String itemTypeName;
                    while (cursor.next()) {
                        itemTypeName = ((AString) cursor.get()).getStringValue();
                        unionList.add(getTypeFromTypeName(dataverseName, itemTypeName));
                    }
                    return new Datatype(dataverseName, datatypeName, new AUnionType(unionList, datatypeName),
                            isAnonymous);
                }
                case UNORDEREDLIST: {
                    String unorderedlistTypeName = ((AString) derivedTypeRecord
                            .getValueByPos(MetadataRecordTypes.DERIVEDTYPE_ARECORD_UNORDEREDLIST_FIELD_INDEX))
                            .getStringValue();
                    return new Datatype(dataverseName, datatypeName, new AUnorderedListType(getTypeFromTypeName(
                            dataverseName, unorderedlistTypeName), datatypeName), isAnonymous);
                }
                case ORDEREDLIST: {
                    String orderedlistTypeName = ((AString) derivedTypeRecord
                            .getValueByPos(MetadataRecordTypes.DERIVEDTYPE_ARECORD_ORDEREDLIST_FIELD_INDEX))
                            .getStringValue();
                    return new Datatype(dataverseName, datatypeName, new AOrderedListType(getTypeFromTypeName(
                            dataverseName, orderedlistTypeName), datatypeName), isAnonymous);
                }
                default:
                    throw new UnsupportedOperationException("Unsupported derived type: " + tag);
            }
        }
        return new Datatype(dataverseName, datatypeName, type, false);
    }

    private IAType getTypeFromTypeName(String dataverseName, String typeName) throws MetadataException {
        IAType type = AsterixBuiltinTypeMap.getBuiltinTypes().get(typeName);
        if (type == null) {
            try {
                return metadataNode.getDatatype(jobId, dataverseName, typeName).getDatatype();
            } catch (RemoteException e) {
                throw new MetadataException(e);
            }
        }
        return type;
    }

    @Override
    public ITupleReference getTupleFromMetadataEntity(Datatype dataType) throws IOException, MetadataException {
        // write the key in the first two fields of the tuple
        tupleBuilder.reset();
        aString.setValue(dataType.getDataverseName());
        stringSerde.serialize(aString, tupleBuilder.getDataOutput());
        tupleBuilder.addFieldEndOffset();
        aString.setValue(dataType.getDatatypeName());
        stringSerde.serialize(aString, tupleBuilder.getDataOutput());
        tupleBuilder.addFieldEndOffset();

        // write the payload in the third field of the tuple
        recordBuilder.reset(MetadataRecordTypes.DATATYPE_RECORDTYPE);

        // write field 0
        fieldValue.reset();
        aString.setValue(dataType.getDataverseName());
        stringSerde.serialize(aString, fieldValue.getDataOutput());
        recordBuilder.addField(MetadataRecordTypes.DATATYPE_ARECORD_DATAVERSENAME_FIELD_INDEX, fieldValue);

        // write field 1
        fieldValue.reset();
        aString.setValue(dataType.getDatatypeName());
        stringSerde.serialize(aString, fieldValue.getDataOutput());
        recordBuilder.addField(MetadataRecordTypes.DATATYPE_ARECORD_DATATYPENAME_FIELD_INDEX, fieldValue);

        // write field 2
        ATypeTag tag = dataType.getDatatype().getTypeTag();
        if (isDerivedType(tag)) {
            fieldValue.reset();
            try {
                writeDerivedTypeRecord(dataType, fieldValue.getDataOutput());
            } catch (AsterixException e) {
                throw new MetadataException(e);
            }
            recordBuilder.addField(MetadataRecordTypes.DATATYPE_ARECORD_DERIVED_FIELD_INDEX, fieldValue);
        }

        // write field 3
        fieldValue.reset();
        aString.setValue(Calendar.getInstance().getTime().toString());
        stringSerde.serialize(aString, fieldValue.getDataOutput());
        recordBuilder.addField(MetadataRecordTypes.DATATYPE_ARECORD_TIMESTAMP_FIELD_INDEX, fieldValue);

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

    private void writeDerivedTypeRecord(Datatype type, DataOutput out) throws IOException, AsterixException {
        DerivedTypeTag tag;
        IARecordBuilder derivedRecordBuilder = new RecordBuilder();
        ArrayBackedValueStorage fieldValue = new ArrayBackedValueStorage();
        switch (type.getDatatype().getTypeTag()) {
            case UNION:
                tag = DerivedTypeTag.UNION;
                break;
            case ORDEREDLIST:
                tag = DerivedTypeTag.ORDEREDLIST;
                break;
            case UNORDEREDLIST:
                tag = DerivedTypeTag.UNORDEREDLIST;
                break;
            case RECORD:
                tag = DerivedTypeTag.RECORD;
                break;
            default:
                throw new UnsupportedOperationException("No metadata record Type for"
                        + type.getDatatype().getDisplayName());
        }

        derivedRecordBuilder.reset(MetadataRecordTypes.DERIVEDTYPE_RECORDTYPE);

        // write field 0
        fieldValue.reset();
        aString.setValue(tag.toString());
        stringSerde.serialize(aString, fieldValue.getDataOutput());
        derivedRecordBuilder.addField(MetadataRecordTypes.DERIVEDTYPE_ARECORD_TAG_FIELD_INDEX, fieldValue);

        // write field 1
        fieldValue.reset();
        booleanSerde.serialize(type.getIsAnonymous() ? ABoolean.TRUE : ABoolean.FALSE, fieldValue.getDataOutput());
        derivedRecordBuilder.addField(MetadataRecordTypes.DERIVEDTYPE_ARECORD_ISANONYMOUS_FIELD_INDEX, fieldValue);

        switch (tag) {
            case ENUM:
                break;
            case RECORD:
                fieldValue.reset();
                writeRecordType(type, fieldValue.getDataOutput());
                derivedRecordBuilder.addField(MetadataRecordTypes.DERIVEDTYPE_ARECORD_RECORD_FIELD_INDEX, fieldValue);
                break;
            case UNION:
                fieldValue.reset();
                writeUnionType(type, fieldValue.getDataOutput());
                derivedRecordBuilder.addField(MetadataRecordTypes.DERIVEDTYPE_ARECORD_UNION_FIELD_INDEX, fieldValue);
                break;
            case UNORDEREDLIST:
                fieldValue.reset();
                writeCollectionType(type, fieldValue.getDataOutput());
                derivedRecordBuilder.addField(MetadataRecordTypes.DERIVEDTYPE_ARECORD_UNORDEREDLIST_FIELD_INDEX,
                        fieldValue);
                break;
            case ORDEREDLIST:
                fieldValue.reset();
                writeCollectionType(type, fieldValue.getDataOutput());
                derivedRecordBuilder.addField(MetadataRecordTypes.DERIVEDTYPE_ARECORD_ORDEREDLIST_FIELD_INDEX,
                        fieldValue);
                break;
        }
        derivedRecordBuilder.write(out, true);
    }

    private void writeCollectionType(Datatype instance, DataOutput out) throws HyracksDataException {
        AbstractCollectionType listType = (AbstractCollectionType) instance.getDatatype();
        String itemTypeName = listType.getItemType().getTypeName();
        if (isDerivedType(listType.getItemType().getTypeTag())) {
            try {
                itemTypeName = handleNestedDerivedType(itemTypeName, instance.getDatatypeName() + "_ItemType",
                        listType.getItemType(), instance);
            } catch (Exception e) {
                // TODO: This should not be a HyracksDataException. Can't
                // fix this currently because of BTree exception model whose
                // fixes must get in.
                throw new HyracksDataException(e);
            }
        }
        aString.setValue(itemTypeName);
        stringSerde.serialize(aString, out);
    }

    private void writeUnionType(Datatype instance, DataOutput dataOutput) throws HyracksDataException {
        List<IAType> unionList = ((AUnionType) instance.getDatatype()).getUnionList();
        OrderedListBuilder listBuilder = new OrderedListBuilder();
        listBuilder.reset(new AOrderedListType(BuiltinType.ASTRING, null));
        ArrayBackedValueStorage itemValue = new ArrayBackedValueStorage();
        String typeName = null;

        int i = 0;
        for (IAType t : unionList) {
            typeName = t.getTypeName();
            if (isDerivedType(t.getTypeTag())) {
                try {
                    typeName = handleNestedDerivedType(typeName,
                            "Type_#" + i + "_UnionType_" + instance.getDatatypeName(), t, instance);
                } catch (Exception e) {
                    // TODO: This should not be a HyracksDataException. Can't
                    // fix this currently because of BTree exception model whose
                    // fixes must get in.
                    throw new HyracksDataException(e);
                }
            }
            itemValue.reset();
            aString.setValue(typeName);
            stringSerde.serialize(aString, itemValue.getDataOutput());
            listBuilder.addItem(itemValue);
            i++;
        }
        listBuilder.write(dataOutput, true);
    }

    private void writeRecordType(Datatype instance, DataOutput out) throws IOException, AsterixException {

        ArrayBackedValueStorage fieldValue = new ArrayBackedValueStorage();
        ArrayBackedValueStorage itemValue = new ArrayBackedValueStorage();
        IARecordBuilder recordRecordBuilder = new RecordBuilder();
        IARecordBuilder fieldRecordBuilder = new RecordBuilder();

        ARecordType recType = (ARecordType) instance.getDatatype();
        OrderedListBuilder listBuilder = new OrderedListBuilder();
        listBuilder.reset(new AOrderedListType(MetadataRecordTypes.FIELD_RECORDTYPE, null));
        String fieldTypeName = null;
        for (int i = 0; i < recType.getFieldNames().length; i++) {
            fieldTypeName = recType.getFieldTypes()[i].getTypeName();
            if (isDerivedType(recType.getFieldTypes()[i].getTypeTag())) {
                try {
                    fieldTypeName = handleNestedDerivedType(fieldTypeName, "Field_" + recType.getFieldNames()[i]
                            + "_in_" + instance.getDatatypeName(), recType.getFieldTypes()[i], instance);
                } catch (Exception e) {
                    // TODO: This should not be a HyracksDataException. Can't
                    // fix this currently because of BTree exception model whose
                    // fixes must get in.
                    throw new HyracksDataException(e);
                }
            }

            itemValue.reset();
            fieldRecordBuilder.reset(MetadataRecordTypes.FIELD_RECORDTYPE);

            // write field 0
            fieldValue.reset();
            aString.setValue(recType.getFieldNames()[i]);
            stringSerde.serialize(aString, fieldValue.getDataOutput());
            fieldRecordBuilder.addField(MetadataRecordTypes.FIELD_ARECORD_FIELDNAME_FIELD_INDEX, fieldValue);

            // write field 1
            fieldValue.reset();
            aString.setValue(fieldTypeName);
            stringSerde.serialize(aString, fieldValue.getDataOutput());
            fieldRecordBuilder.addField(MetadataRecordTypes.FIELD_ARECORD_FIELDTYPE_FIELD_INDEX, fieldValue);

            // write record
            fieldRecordBuilder.write(itemValue.getDataOutput(), true);

            // add item to the list of fields
            listBuilder.addItem(itemValue);
        }

        recordRecordBuilder.reset(MetadataRecordTypes.RECORD_RECORDTYPE);
        // write field 0
        fieldValue.reset();
        booleanSerde.serialize(recType.isOpen() ? ABoolean.TRUE : ABoolean.FALSE, fieldValue.getDataOutput());
        recordRecordBuilder.addField(MetadataRecordTypes.RECORDTYPE_ARECORD_ISOPEN_FIELD_INDEX, fieldValue);

        // write field 1
        fieldValue.reset();
        listBuilder.write(fieldValue.getDataOutput(), true);
        recordRecordBuilder.addField(MetadataRecordTypes.RECORDTYPE_ARECORD_FIELDS_FIELD_INDEX, fieldValue);

        // write record
        recordRecordBuilder.write(out, true);
    }

    private String handleNestedDerivedType(String typeName, String suggestedTypeName, IAType nestedType,
            Datatype topLevelType) throws Exception {
        MetadataNode mn = MetadataNode.INSTANCE;
        try {
            if (typeName == null) {
                typeName = suggestedTypeName;
                metadataNode.addDatatype(jobId, new Datatype(topLevelType.getDataverseName(), typeName, nestedType,
                        true));

            }
            mn.insertIntoDatatypeSecondaryIndex(jobId, topLevelType.getDataverseName(), typeName,
                    topLevelType.getDatatypeName());

        } catch (TreeIndexDuplicateKeyException e) {
            // The key may have been inserted by a previous DDL statement or by
            // a previous nested type.
        }
        return typeName;
    }

    private boolean isDerivedType(ATypeTag tag) {
        if (tag == ATypeTag.RECORD || tag == ATypeTag.ORDEREDLIST || tag == ATypeTag.UNORDEREDLIST
                || tag == ATypeTag.UNION)
            return true;
        return false;
    }
}
