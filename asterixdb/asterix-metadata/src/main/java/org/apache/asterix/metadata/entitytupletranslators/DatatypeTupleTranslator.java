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
import java.rmi.RemoteException;
import java.util.Calendar;

import org.apache.asterix.builders.IARecordBuilder;
import org.apache.asterix.builders.OrderedListBuilder;
import org.apache.asterix.builders.RecordBuilder;
import org.apache.asterix.common.transactions.TxnId;
import org.apache.asterix.formats.nontagged.SerializerDeserializerProvider;
import org.apache.asterix.metadata.MetadataNode;
import org.apache.asterix.metadata.bootstrap.MetadataPrimaryIndexes;
import org.apache.asterix.metadata.bootstrap.MetadataRecordTypes;
import org.apache.asterix.metadata.entities.BuiltinTypeMap;
import org.apache.asterix.metadata.entities.Datatype;
import org.apache.asterix.om.base.ABoolean;
import org.apache.asterix.om.base.AOrderedList;
import org.apache.asterix.om.base.ARecord;
import org.apache.asterix.om.base.AString;
import org.apache.asterix.om.base.IACursor;
import org.apache.asterix.om.types.AOrderedListType;
import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.AUnionType;
import org.apache.asterix.om.types.AUnorderedListType;
import org.apache.asterix.om.types.AbstractCollectionType;
import org.apache.asterix.om.types.AbstractComplexType;
import org.apache.asterix.om.types.IAType;
import org.apache.asterix.om.utils.NonTaggedFormatUtil;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.api.dataflow.value.ISerializerDeserializer;
import org.apache.hyracks.api.exceptions.ErrorCode;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;

/**
 * Translates a Datatype metadata entity to an ITupleReference and vice versa.
 */
public class DatatypeTupleTranslator extends AbstractTupleTranslator<Datatype> {
    private static final long serialVersionUID = -2324433490801381399L;

    // Field indexes of serialized Dataset in a tuple.
    // First key field.
    public static final int DATATYPE_DATAVERSENAME_TUPLE_FIELD_INDEX = 0;
    // Second key field.
    public static final int DATATYPE_DATATYPE_TUPLE_FIELD_INDEX = 1;
    // Payload field containing serialized Datatype.
    public static final int DATATYPE_PAYLOAD_TUPLE_FIELD_INDEX = 2;

    public enum DerivedTypeTag {
        RECORD,
        UNORDEREDLIST,
        ORDEREDLIST
    }

    @SuppressWarnings("unchecked")
    private ISerializerDeserializer<ARecord> recordSerDes =
            SerializerDeserializerProvider.INSTANCE.getSerializerDeserializer(MetadataRecordTypes.DATATYPE_RECORDTYPE);
    private final MetadataNode metadataNode;
    private final TxnId txnId;

    protected DatatypeTupleTranslator(TxnId txnId, MetadataNode metadataNode, boolean getTuple) {
        super(getTuple, MetadataPrimaryIndexes.DATATYPE_DATASET.getFieldCount());
        this.txnId = txnId;
        this.metadataNode = metadataNode;
    }

    @Override
    public Datatype getMetadataEntityFromTuple(ITupleReference frameTuple)
            throws AlgebricksException, HyracksDataException {
        byte[] serRecord = frameTuple.getFieldData(DATATYPE_PAYLOAD_TUPLE_FIELD_INDEX);
        int recordStartOffset = frameTuple.getFieldStart(DATATYPE_PAYLOAD_TUPLE_FIELD_INDEX);
        int recordLength = frameTuple.getFieldLength(DATATYPE_PAYLOAD_TUPLE_FIELD_INDEX);
        ByteArrayInputStream stream = new ByteArrayInputStream(serRecord, recordStartOffset, recordLength);
        DataInput in = new DataInputStream(stream);
        ARecord datatypeRecord = recordSerDes.deserialize(in);
        return createDataTypeFromARecord(datatypeRecord);
    }

    private Datatype createDataTypeFromARecord(ARecord datatypeRecord) throws AlgebricksException {
        String dataverseName =
                ((AString) datatypeRecord.getValueByPos(MetadataRecordTypes.DATATYPE_ARECORD_DATAVERSENAME_FIELD_INDEX))
                        .getStringValue();
        String datatypeName =
                ((AString) datatypeRecord.getValueByPos(MetadataRecordTypes.DATATYPE_ARECORD_DATATYPENAME_FIELD_INDEX))
                        .getStringValue();
        IAType type = BuiltinTypeMap.getBuiltinType(datatypeName);
        if (type == null) {
            // Derived Type
            ARecord derivedTypeRecord =
                    (ARecord) datatypeRecord.getValueByPos(MetadataRecordTypes.DATATYPE_ARECORD_DERIVED_FIELD_INDEX);
            DerivedTypeTag tag = DerivedTypeTag.valueOf(
                    ((AString) derivedTypeRecord.getValueByPos(MetadataRecordTypes.DERIVEDTYPE_ARECORD_TAG_FIELD_INDEX))
                            .getStringValue());
            boolean isAnonymous = ((ABoolean) derivedTypeRecord
                    .getValueByPos(MetadataRecordTypes.DERIVEDTYPE_ARECORD_ISANONYMOUS_FIELD_INDEX)).getBoolean();
            switch (tag) {
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
                        fieldNames[fieldId] =
                                ((AString) field.getValueByPos(MetadataRecordTypes.FIELD_ARECORD_FIELDNAME_FIELD_INDEX))
                                        .getStringValue();
                        fieldTypeName =
                                ((AString) field.getValueByPos(MetadataRecordTypes.FIELD_ARECORD_FIELDTYPE_FIELD_INDEX))
                                        .getStringValue();
                        boolean isNullable = ((ABoolean) field
                                .getValueByPos(MetadataRecordTypes.FIELD_ARECORD_ISNULLABLE_FIELD_INDEX)).getBoolean()
                                        .booleanValue();
                        fieldTypes[fieldId] = BuiltinTypeMap.getTypeFromTypeName(metadataNode, txnId, dataverseName,
                                fieldTypeName, isNullable);
                        fieldId++;
                    }
                    return new Datatype(dataverseName, datatypeName,
                            new ARecordType(datatypeName, fieldNames, fieldTypes, isOpen), isAnonymous);
                }
                case UNORDEREDLIST: {
                    String unorderedlistTypeName = ((AString) derivedTypeRecord
                            .getValueByPos(MetadataRecordTypes.DERIVEDTYPE_ARECORD_UNORDEREDLIST_FIELD_INDEX))
                                    .getStringValue();
                    return new Datatype(dataverseName, datatypeName,
                            new AUnorderedListType(BuiltinTypeMap.getTypeFromTypeName(metadataNode, txnId,
                                    dataverseName, unorderedlistTypeName, false), datatypeName),
                            isAnonymous);
                }
                case ORDEREDLIST: {
                    String orderedlistTypeName = ((AString) derivedTypeRecord
                            .getValueByPos(MetadataRecordTypes.DERIVEDTYPE_ARECORD_ORDEREDLIST_FIELD_INDEX))
                                    .getStringValue();
                    return new Datatype(dataverseName, datatypeName,
                            new AOrderedListType(BuiltinTypeMap.getTypeFromTypeName(metadataNode, txnId, dataverseName,
                                    orderedlistTypeName, false), datatypeName),
                            isAnonymous);
                }
                default:
                    throw new UnsupportedOperationException("Unsupported derived type: " + tag);
            }
        }
        return new Datatype(dataverseName, datatypeName, type, false);
    }

    @Override
    public ITupleReference getTupleFromMetadataEntity(Datatype dataType)
            throws HyracksDataException, AlgebricksException {
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

        IAType fieldType = dataType.getDatatype();
        // unwrap nullable type out of the union
        if (fieldType.getTypeTag() == ATypeTag.UNION) {
            fieldType = ((AUnionType) dataType.getDatatype()).getActualType();
        }

        // write field 3
        if (fieldType.getTypeTag().isDerivedType()) {
            fieldValue.reset();
            writeDerivedTypeRecord(dataType, (AbstractComplexType) fieldType, fieldValue.getDataOutput());
            recordBuilder.addField(MetadataRecordTypes.DATATYPE_ARECORD_DERIVED_FIELD_INDEX, fieldValue);
        }

        // write field 4
        fieldValue.reset();
        aString.setValue(Calendar.getInstance().getTime().toString());
        stringSerde.serialize(aString, fieldValue.getDataOutput());
        recordBuilder.addField(MetadataRecordTypes.DATATYPE_ARECORD_TIMESTAMP_FIELD_INDEX, fieldValue);

        // write record
        recordBuilder.write(tupleBuilder.getDataOutput(), true);
        tupleBuilder.addFieldEndOffset();

        tuple.reset(tupleBuilder.getFieldEndOffsets(), tupleBuilder.getByteArray());
        return tuple;
    }

    private void writeDerivedTypeRecord(Datatype type, AbstractComplexType derivedDatatype, DataOutput out)
            throws HyracksDataException {
        DerivedTypeTag tag = null;
        IARecordBuilder derivedRecordBuilder = new RecordBuilder();
        ArrayBackedValueStorage fieldValue = new ArrayBackedValueStorage();
        switch (derivedDatatype.getTypeTag()) {
            case ARRAY:
                tag = DerivedTypeTag.ORDEREDLIST;
                break;
            case MULTISET:
                tag = DerivedTypeTag.UNORDEREDLIST;
                break;
            case OBJECT:
                tag = DerivedTypeTag.RECORD;
                break;
            default:
                throw new UnsupportedOperationException(
                        "No metadata record Type for " + derivedDatatype.getDisplayName());
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
            case RECORD:
                fieldValue.reset();
                writeRecordType(type, derivedDatatype, fieldValue.getDataOutput());
                derivedRecordBuilder.addField(MetadataRecordTypes.DERIVEDTYPE_ARECORD_RECORD_FIELD_INDEX, fieldValue);
                break;
            case UNORDEREDLIST:
                fieldValue.reset();
                writeCollectionType(type, derivedDatatype, fieldValue.getDataOutput());
                derivedRecordBuilder.addField(MetadataRecordTypes.DERIVEDTYPE_ARECORD_UNORDEREDLIST_FIELD_INDEX,
                        fieldValue);
                break;
            case ORDEREDLIST:
                fieldValue.reset();
                writeCollectionType(type, derivedDatatype, fieldValue.getDataOutput());
                derivedRecordBuilder.addField(MetadataRecordTypes.DERIVEDTYPE_ARECORD_ORDEREDLIST_FIELD_INDEX,
                        fieldValue);
                break;
        }
        derivedRecordBuilder.write(out, true);
    }

    private void writeCollectionType(Datatype instance, AbstractComplexType type, DataOutput out)
            throws HyracksDataException {
        AbstractCollectionType listType = (AbstractCollectionType) type;
        IAType itemType = listType.getItemType();
        if (itemType.getTypeTag().isDerivedType()) {
            handleNestedDerivedType(itemType.getTypeName(), (AbstractComplexType) itemType, instance,
                    instance.getDataverseName(), instance.getDatatypeName());
        }
        aString.setValue(listType.getItemType().getTypeName());
        stringSerde.serialize(aString, out);
    }

    private void writeRecordType(Datatype instance, AbstractComplexType type, DataOutput out)
            throws HyracksDataException {

        ArrayBackedValueStorage fieldValue = new ArrayBackedValueStorage();
        ArrayBackedValueStorage itemValue = new ArrayBackedValueStorage();
        IARecordBuilder recordRecordBuilder = new RecordBuilder();
        IARecordBuilder fieldRecordBuilder = new RecordBuilder();

        ARecordType recType = (ARecordType) type;
        OrderedListBuilder listBuilder = new OrderedListBuilder();
        listBuilder.reset(new AOrderedListType(MetadataRecordTypes.FIELD_RECORDTYPE, null));
        IAType fieldType = null;

        for (int i = 0; i < recType.getFieldNames().length; i++) {
            fieldType = recType.getFieldTypes()[i];
            boolean fieldIsNullable = false;
            if (NonTaggedFormatUtil.isOptional(fieldType)) {
                fieldIsNullable = true;
                fieldType = ((AUnionType) fieldType).getActualType();
            }
            if (fieldType.getTypeTag().isDerivedType()) {
                handleNestedDerivedType(fieldType.getTypeName(), (AbstractComplexType) fieldType, instance,
                        instance.getDataverseName(), instance.getDatatypeName());
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
            aString.setValue(fieldType.getTypeName());
            stringSerde.serialize(aString, fieldValue.getDataOutput());
            fieldRecordBuilder.addField(MetadataRecordTypes.FIELD_ARECORD_FIELDTYPE_FIELD_INDEX, fieldValue);

            // write field 2
            fieldValue.reset();
            booleanSerde.serialize(fieldIsNullable ? ABoolean.TRUE : ABoolean.FALSE, fieldValue.getDataOutput());
            fieldRecordBuilder.addField(MetadataRecordTypes.FIELD_ARECORD_ISNULLABLE_FIELD_INDEX, fieldValue);

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

    private String handleNestedDerivedType(String typeName, AbstractComplexType nestedType, Datatype topLevelType,
            String dataverseName, String datatypeName) throws HyracksDataException {
        try {
            metadataNode.addDatatype(txnId, new Datatype(dataverseName, typeName, nestedType, true));
        } catch (AlgebricksException e) {
            // The nested record type may have been inserted by a previous DDL statement or
            // by
            // a previous nested type.
            if (!(e.getCause() instanceof HyracksDataException)) {
                throw HyracksDataException.create(e);
            } else {
                HyracksDataException hde = (HyracksDataException) e.getCause();
                if (!hde.getComponent().equals(ErrorCode.HYRACKS) || hde.getErrorCode() != ErrorCode.DUPLICATE_KEY) {
                    throw hde;
                }
            }
        } catch (RemoteException e) {
            // TODO: This should not be a HyracksDataException. Can't
            // fix this currently because of BTree exception model whose
            // fixes must get in.
            throw HyracksDataException.create(e);
        }
        return typeName;
    }
}
