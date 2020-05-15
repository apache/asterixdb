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

import java.util.Calendar;

import org.apache.asterix.common.metadata.DataverseName;
import org.apache.asterix.common.transactions.TxnId;
import org.apache.asterix.metadata.MetadataNode;
import org.apache.asterix.metadata.bootstrap.MetadataPrimaryIndexes;
import org.apache.asterix.metadata.bootstrap.MetadataRecordTypes;
import org.apache.asterix.metadata.entities.BuiltinTypeMap;
import org.apache.asterix.metadata.entities.Datatype;
import org.apache.asterix.metadata.utils.TypeUtil;
import org.apache.asterix.om.base.ABoolean;
import org.apache.asterix.om.base.AOrderedList;
import org.apache.asterix.om.base.ARecord;
import org.apache.asterix.om.base.AString;
import org.apache.asterix.om.base.IACursor;
import org.apache.asterix.om.types.AOrderedListType;
import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.om.types.AUnorderedListType;
import org.apache.asterix.om.types.AbstractComplexType;
import org.apache.asterix.om.types.IAType;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;

/**
 * Translates a Datatype metadata entity to an ITupleReference and vice versa.
 */
public class DatatypeTupleTranslator extends AbstractDatatypeTupleTranslator<Datatype> {

    // Payload field containing serialized Datatype.
    private static final int DATATYPE_PAYLOAD_TUPLE_FIELD_INDEX = 2;

    protected DatatypeTupleTranslator(TxnId txnId, MetadataNode metadataNode, boolean getTuple) {
        super(txnId, metadataNode, getTuple, MetadataPrimaryIndexes.DATATYPE_DATASET,
                DATATYPE_PAYLOAD_TUPLE_FIELD_INDEX);
    }

    @Override
    protected Datatype createMetadataEntityFromARecord(ARecord datatypeRecord) throws AlgebricksException {
        String dataverseCanonicalName =
                ((AString) datatypeRecord.getValueByPos(MetadataRecordTypes.DATATYPE_ARECORD_DATAVERSENAME_FIELD_INDEX))
                        .getStringValue();
        DataverseName dataverseName = DataverseName.createFromCanonicalForm(dataverseCanonicalName);
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
                            .getValueByPos(MetadataRecordTypes.RECORDTYPE_ARECORD_ISOPEN_FIELD_INDEX)).getBoolean();
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
                                .getValueByPos(MetadataRecordTypes.FIELD_ARECORD_ISNULLABLE_FIELD_INDEX)).getBoolean();
                        int isMissableIdx = field.getType().getFieldIndex(MetadataRecordTypes.FIELD_NAME_IS_MISSABLE);
                        boolean isMissable;
                        if (isMissableIdx >= 0) {
                            isMissable = ((ABoolean) field.getValueByPos(isMissableIdx)).getBoolean();
                        } else {
                            // back-compat
                            // we previously stored 'isNullable' = true if type was 'unknowable',
                            // or 'isNullable' = 'false' if the type was 'not unknowable'.
                            isMissable = isNullable;
                        }

                        IAType fieldType =
                                BuiltinTypeMap.getTypeFromTypeName(metadataNode, txnId, dataverseName, fieldTypeName);
                        fieldTypes[fieldId] = TypeUtil.createQuantifiedType(fieldType, isNullable, isMissable);
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
                                    dataverseName, unorderedlistTypeName), datatypeName),
                            isAnonymous);
                }
                case ORDEREDLIST: {
                    String orderedlistTypeName = ((AString) derivedTypeRecord
                            .getValueByPos(MetadataRecordTypes.DERIVEDTYPE_ARECORD_ORDEREDLIST_FIELD_INDEX))
                                    .getStringValue();
                    return new Datatype(dataverseName, datatypeName, new AOrderedListType(
                            BuiltinTypeMap.getTypeFromTypeName(metadataNode, txnId, dataverseName, orderedlistTypeName),
                            datatypeName), isAnonymous);
                }
                default:
                    throw new UnsupportedOperationException("Unsupported derived type: " + tag);
            }
        }
        return new Datatype(dataverseName, datatypeName, type, false);
    }

    @Override
    public ITupleReference getTupleFromMetadataEntity(Datatype dataType) throws HyracksDataException {
        String dataverseCanonicalName = dataType.getDataverseName().getCanonicalForm();

        // write the key in the first two fields of the tuple
        tupleBuilder.reset();
        aString.setValue(dataverseCanonicalName);
        stringSerde.serialize(aString, tupleBuilder.getDataOutput());
        tupleBuilder.addFieldEndOffset();
        aString.setValue(dataType.getDatatypeName());
        stringSerde.serialize(aString, tupleBuilder.getDataOutput());
        tupleBuilder.addFieldEndOffset();

        // write the payload in the third field of the tuple
        recordBuilder.reset(MetadataRecordTypes.DATATYPE_RECORDTYPE);

        // write field 0
        fieldValue.reset();
        aString.setValue(dataverseCanonicalName);
        stringSerde.serialize(aString, fieldValue.getDataOutput());
        recordBuilder.addField(MetadataRecordTypes.DATATYPE_ARECORD_DATAVERSENAME_FIELD_INDEX, fieldValue);

        // write field 1
        fieldValue.reset();
        aString.setValue(dataType.getDatatypeName());
        stringSerde.serialize(aString, fieldValue.getDataOutput());
        recordBuilder.addField(MetadataRecordTypes.DATATYPE_ARECORD_DATATYPENAME_FIELD_INDEX, fieldValue);

        // write field 2
        IAType fieldType = dataType.getDatatype();
        if (fieldType.getTypeTag().isDerivedType()) {
            fieldValue.reset();
            writeDerivedTypeRecord(dataType.getDataverseName(), (AbstractComplexType) fieldType,
                    fieldValue.getDataOutput(), dataType.getIsAnonymous());
            recordBuilder.addField(MetadataRecordTypes.DATATYPE_ARECORD_DERIVED_FIELD_INDEX, fieldValue);
        }

        // write field 3
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
}
