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

import org.apache.asterix.builders.IARecordBuilder;
import org.apache.asterix.builders.OrderedListBuilder;
import org.apache.asterix.builders.RecordBuilder;
import org.apache.asterix.common.metadata.DataverseName;
import org.apache.asterix.common.transactions.TxnId;
import org.apache.asterix.metadata.MetadataNode;
import org.apache.asterix.metadata.api.IMetadataIndex;
import org.apache.asterix.metadata.bootstrap.MetadataRecordTypes;
import org.apache.asterix.metadata.entities.Datatype;
import org.apache.asterix.om.base.ABoolean;
import org.apache.asterix.om.types.AOrderedListType;
import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.AUnionType;
import org.apache.asterix.om.types.AbstractCollectionType;
import org.apache.asterix.om.types.AbstractComplexType;
import org.apache.asterix.om.types.IAType;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.api.exceptions.ErrorCode;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;

public abstract class AbstractDatatypeTupleTranslator<T> extends AbstractTupleTranslator<T> {

    public enum DerivedTypeTag {
        RECORD,
        UNORDEREDLIST,
        ORDEREDLIST
    }

    protected final MetadataNode metadataNode;

    protected final TxnId txnId;

    public AbstractDatatypeTupleTranslator(TxnId txnId, MetadataNode metadataNode, boolean getTuple,
            IMetadataIndex metadataIndex, int payloadTupleFieldIndex) {
        super(getTuple, metadataIndex, payloadTupleFieldIndex);
        this.txnId = txnId;
        this.metadataNode = metadataNode;
    }

    protected void writeDerivedTypeRecord(DataverseName dataverseName, AbstractComplexType derivedDatatype,
            DataOutput out, boolean isAnonymous) throws HyracksDataException {
        DerivedTypeTag tag;
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
        booleanSerde.serialize(ABoolean.valueOf(isAnonymous), fieldValue.getDataOutput());
        derivedRecordBuilder.addField(MetadataRecordTypes.DERIVEDTYPE_ARECORD_ISANONYMOUS_FIELD_INDEX, fieldValue);

        switch (tag) {
            case RECORD:
                fieldValue.reset();
                writeRecordType(dataverseName, derivedDatatype, fieldValue.getDataOutput());
                derivedRecordBuilder.addField(MetadataRecordTypes.DERIVEDTYPE_ARECORD_RECORD_FIELD_INDEX, fieldValue);
                break;
            case UNORDEREDLIST:
                fieldValue.reset();
                writeCollectionType(dataverseName, derivedDatatype, fieldValue.getDataOutput());
                derivedRecordBuilder.addField(MetadataRecordTypes.DERIVEDTYPE_ARECORD_UNORDEREDLIST_FIELD_INDEX,
                        fieldValue);
                break;
            case ORDEREDLIST:
                fieldValue.reset();
                writeCollectionType(dataverseName, derivedDatatype, fieldValue.getDataOutput());
                derivedRecordBuilder.addField(MetadataRecordTypes.DERIVEDTYPE_ARECORD_ORDEREDLIST_FIELD_INDEX,
                        fieldValue);
                break;
        }
        derivedRecordBuilder.write(out, true);
    }

    private void writeCollectionType(DataverseName dataverseName, AbstractComplexType type, DataOutput out)
            throws HyracksDataException {
        AbstractCollectionType listType = (AbstractCollectionType) type;
        IAType itemType = listType.getItemType();
        if (itemType.getTypeTag().isDerivedType()) {
            handleNestedDerivedType(dataverseName, itemType.getTypeName(), (AbstractComplexType) itemType);
        }
        aString.setValue(listType.getItemType().getTypeName());
        stringSerde.serialize(aString, out);
    }

    private void writeRecordType(DataverseName dataverseName, AbstractComplexType type, DataOutput out)
            throws HyracksDataException {

        ArrayBackedValueStorage fieldValue = new ArrayBackedValueStorage();
        ArrayBackedValueStorage itemValue = new ArrayBackedValueStorage();
        IARecordBuilder recordRecordBuilder = new RecordBuilder();
        IARecordBuilder fieldRecordBuilder = new RecordBuilder();

        ARecordType recType = (ARecordType) type;
        OrderedListBuilder listBuilder = new OrderedListBuilder();
        listBuilder.reset(new AOrderedListType(MetadataRecordTypes.FIELD_RECORDTYPE, null));

        for (int i = 0, n = recType.getFieldNames().length; i < n; i++) {
            IAType fieldType = recType.getFieldTypes()[i];
            boolean fieldIsNullable = false;
            boolean fieldIsMissable = false;

            if (fieldType.getTypeTag() == ATypeTag.UNION) {
                AUnionType fieldUnionType = (AUnionType) fieldType;
                fieldIsNullable = fieldUnionType.isNullableType();
                fieldIsMissable = fieldUnionType.isMissableType();
                fieldType = fieldUnionType.getActualType();
            }
            if (fieldType.getTypeTag().isDerivedType()) {
                handleNestedDerivedType(dataverseName, fieldType.getTypeName(), (AbstractComplexType) fieldType);
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
            booleanSerde.serialize(ABoolean.valueOf(fieldIsNullable), fieldValue.getDataOutput());
            fieldRecordBuilder.addField(MetadataRecordTypes.FIELD_ARECORD_ISNULLABLE_FIELD_INDEX, fieldValue);

            // write open fields
            fieldName.reset();
            aString.setValue(MetadataRecordTypes.FIELD_NAME_IS_MISSABLE);
            stringSerde.serialize(aString, fieldName.getDataOutput());
            fieldValue.reset();
            booleanSerde.serialize(ABoolean.valueOf(fieldIsMissable), fieldValue.getDataOutput());
            fieldRecordBuilder.addField(fieldName, fieldValue);

            // write record
            fieldRecordBuilder.write(itemValue.getDataOutput(), true);

            // add item to the list of fields
            listBuilder.addItem(itemValue);
        }

        recordRecordBuilder.reset(MetadataRecordTypes.RECORD_RECORDTYPE);
        // write field 0
        fieldValue.reset();
        booleanSerde.serialize(ABoolean.valueOf(recType.isOpen()), fieldValue.getDataOutput());
        recordRecordBuilder.addField(MetadataRecordTypes.RECORDTYPE_ARECORD_ISOPEN_FIELD_INDEX, fieldValue);

        // write field 1
        fieldValue.reset();
        listBuilder.write(fieldValue.getDataOutput(), true);
        recordRecordBuilder.addField(MetadataRecordTypes.RECORDTYPE_ARECORD_FIELDS_FIELD_INDEX, fieldValue);

        // write record
        recordRecordBuilder.write(out, true);
    }

    protected void handleNestedDerivedType(DataverseName dataverseName, String typeName, AbstractComplexType nestedType)
            throws HyracksDataException {
        try {
            metadataNode.addDatatype(txnId, new Datatype(dataverseName, typeName, nestedType, true));
        } catch (AlgebricksException e) {
            // The nested record type may have been inserted by a previous DDL statement or
            // by a previous nested type.
            if (!(e.getCause() instanceof HyracksDataException)) {
                throw HyracksDataException.create(e);
            } else {
                HyracksDataException hde = (HyracksDataException) e.getCause();
                if (!hde.getComponent().equals(ErrorCode.HYRACKS) || hde.getErrorCode() != ErrorCode.DUPLICATE_KEY) {
                    throw hde;
                }
            }
        }
    }
}
