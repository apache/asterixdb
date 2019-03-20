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

package org.apache.asterix.om.pointables;

import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.asterix.dataflow.data.nontagged.serde.AInt32SerializerDeserializer;
import org.apache.asterix.om.pointables.base.IVisitablePointable;
import org.apache.asterix.om.pointables.visitor.IVisitablePointableVisitor;
import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.AUnionType;
import org.apache.asterix.om.types.EnumDeserializer;
import org.apache.asterix.om.types.IAType;
import org.apache.asterix.om.util.container.IObjectFactory;
import org.apache.asterix.om.utils.NonTaggedFormatUtil;
import org.apache.asterix.om.utils.RecordUtil;
import org.apache.asterix.om.utils.ResettableByteArrayOutputStream;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.util.string.UTF8StringWriter;

/**
 * This class interprets the binary data representation of a record. One can
 * call getFieldNames, getFieldTypeTags and getFieldValues to get pointable
 * objects for field names, field type tags, and field values.
 */
public class ARecordVisitablePointable extends AbstractVisitablePointable {

    /**
     * DO NOT allow to create ARecordPointable object arbitrarily, force to use
     * object pool based allocator, in order to have object reuse
     */
    static IObjectFactory<ARecordVisitablePointable, IAType> FACTORY =
            type -> new ARecordVisitablePointable((ARecordType) type);

    // access results: field names, field types, and field values
    private final List<IVisitablePointable> fieldNames = new ArrayList<>();
    private final List<IVisitablePointable> fieldTypeTags = new ArrayList<>();
    private final List<IVisitablePointable> fieldValues = new ArrayList<>();

    // pointable allocator
    private final PointableAllocator allocator = new PointableAllocator();

    private final ResettableByteArrayOutputStream typeBos = new ResettableByteArrayOutputStream();

    private final ResettableByteArrayOutputStream dataBos = new ResettableByteArrayOutputStream();
    private final DataOutputStream dataDos = new DataOutputStream(dataBos);

    private final ARecordType inputRecType;

    private final int numberOfSchemaFields;
    private final int[] fieldOffsets;
    private final IVisitablePointable nullReference = PointableAllocator.allocateUnrestableEmpty();
    private final IVisitablePointable missingReference = PointableAllocator.allocateUnrestableEmpty();

    private int closedPartTypeInfoSize = 0;
    private ATypeTag typeTag;

    /**
     * private constructor, to prevent constructing it arbitrarily
     *
     * @param inputType
     *            inputType should not be null. Use FULLY_OPEN_RECORD_TYPE instead.
     */
    public ARecordVisitablePointable(ARecordType inputType) {
        this.inputRecType = inputType;
        IAType[] fieldTypes = inputType.getFieldTypes();
        String[] fieldNameStrs = inputType.getFieldNames();
        numberOfSchemaFields = fieldTypes.length;

        // initialize the buffer for closed parts(fieldName bytes+ type bytes) +
        // constant(null bytes)
        try {
            final DataOutputStream typeDos = new DataOutputStream(typeBos);
            final UTF8StringWriter utf8Writer = new UTF8StringWriter();
            for (int i = 0; i < numberOfSchemaFields; i++) {
                ATypeTag ftypeTag = fieldTypes[i].getTypeTag();

                if (NonTaggedFormatUtil.isOptional(fieldTypes[i])) {
                    // optional field: add the embedded non-null type tag
                    ftypeTag = ((AUnionType) fieldTypes[i]).getActualType().getTypeTag();
                }

                // add type tag Reference
                int tagStart = typeBos.size();
                typeDos.writeByte(ftypeTag.serialize());
                int tagEnd = typeBos.size();
                IVisitablePointable typeTagReference = AFlatValuePointable.FACTORY.create(null);
                typeTagReference.set(typeBos.getByteArray(), tagStart, tagEnd - tagStart);
                fieldTypeTags.add(typeTagReference);

                // add type name Reference (including a astring type tag)
                int nameStart = typeBos.size();
                typeDos.writeByte(ATypeTag.SERIALIZED_STRING_TYPE_TAG);
                utf8Writer.writeUTF8(fieldNameStrs[i], typeDos);
                int nameEnd = typeBos.size();
                IVisitablePointable typeNameReference = AFlatValuePointable.FACTORY.create(null);
                typeNameReference.set(typeBos.getByteArray(), nameStart, nameEnd - nameStart);
                fieldNames.add(typeNameReference);
            }

            // initialize a constant: null value bytes reference
            int nullFieldStart = typeBos.size();
            typeDos.writeByte(ATypeTag.SERIALIZED_NULL_TYPE_TAG);
            int nullFieldEnd = typeBos.size();
            nullReference.set(typeBos.getByteArray(), nullFieldStart, nullFieldEnd - nullFieldStart);

            // initialize a constant: missing value bytes reference
            int missingFieldStart = typeBos.size();
            typeDos.writeByte(ATypeTag.SERIALIZED_MISSING_TYPE_TAG);
            int missingFieldEnd = typeBos.size();
            missingReference.set(typeBos.getByteArray(), missingFieldStart, missingFieldEnd - missingFieldStart);
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
        closedPartTypeInfoSize = typeBos.size();
        fieldOffsets = new int[numberOfSchemaFields];
    }

    private void reset() {
        typeBos.reset(closedPartTypeInfoSize);
        dataBos.reset(0);
        // reset the allocator
        allocator.reset();

        // clean up the returned containers
        for (int i = fieldNames.size() - 1; i >= numberOfSchemaFields; i--) {
            fieldNames.remove(i);
        }
        for (int i = fieldTypeTags.size() - 1; i >= numberOfSchemaFields; i--) {
            fieldTypeTags.remove(i);
        }
        fieldValues.clear();
    }

    @Override
    public void set(byte[] b, int start, int len) {
        // clear the previous states
        reset();
        super.set(b, start, len);

        boolean isExpanded = false;
        int openPartOffset = 0;
        int recordOffset = start;
        int offsetArrayOffset;

        //advance to either isExpanded or numberOfSchemaFields
        int s = start + 5;
        //inputRecType will never be null.
        if (inputRecType.isOpen()) {
            isExpanded = b[s] == 1;
            //advance either to openPartOffset or numberOfSchemaFields
            s += 1;
            if (isExpanded) {
                openPartOffset = start + AInt32SerializerDeserializer.getInt(b, s);
                //advance to numberOfSchemaFields
                s += 4;
            }
        }

        try {
            if (numberOfSchemaFields > 0) {
                //advance to nullBitMap if hasOptionalFields, or fieldOffsets
                s += 4;
                int nullBitMapOffset = 0;
                boolean hasOptionalFields = NonTaggedFormatUtil.hasOptionalField(inputRecType);
                if (hasOptionalFields) {
                    nullBitMapOffset = s;
                    offsetArrayOffset = s + (this.numberOfSchemaFields % 4 == 0 ? numberOfSchemaFields / 4
                            : numberOfSchemaFields / 4 + 1);
                } else {
                    offsetArrayOffset = s;
                }
                for (int i = 0; i < numberOfSchemaFields; i++) {
                    fieldOffsets[i] = AInt32SerializerDeserializer.getInt(b, offsetArrayOffset) + recordOffset;
                    offsetArrayOffset += 4;
                }
                for (int fieldNumber = 0; fieldNumber < numberOfSchemaFields; fieldNumber++) {
                    if (hasOptionalFields) {
                        byte b1 = b[nullBitMapOffset + fieldNumber / 4];
                        if (RecordUtil.isNull(b1, fieldNumber)) {
                            // set null value (including type tag inside)
                            fieldValues.add(nullReference);
                            continue;
                        }
                        if (RecordUtil.isMissing(b1, fieldNumber)) {
                            // set missing value (including type tag inside)
                            fieldValues.add(missingReference);
                            continue;
                        }
                    }
                    IAType[] fieldTypes = inputRecType.getFieldTypes();
                    int fieldValueLength = 0;

                    IAType fieldType = fieldTypes[fieldNumber];
                    if (fieldTypes[fieldNumber].getTypeTag() == ATypeTag.UNION) {
                        if (((AUnionType) fieldTypes[fieldNumber]).isUnknownableType()) {
                            fieldType = ((AUnionType) fieldTypes[fieldNumber]).getActualType();
                            typeTag = fieldType.getTypeTag();
                            fieldValueLength = NonTaggedFormatUtil.getFieldValueLength(b, fieldOffsets[fieldNumber],
                                    typeTag, false);
                        }
                    } else {
                        typeTag = fieldTypes[fieldNumber].getTypeTag();
                        fieldValueLength =
                                NonTaggedFormatUtil.getFieldValueLength(b, fieldOffsets[fieldNumber], typeTag, false);
                    }
                    // set field value (including the type tag)
                    int fstart = dataBos.size();
                    dataDos.writeByte(typeTag.serialize());
                    dataDos.write(b, fieldOffsets[fieldNumber], fieldValueLength);
                    int fend = dataBos.size();
                    IVisitablePointable fieldValue = allocator.allocateFieldValue(fieldType);
                    fieldValue.set(dataBos.getByteArray(), fstart, fend - fstart);
                    fieldValues.add(fieldValue);
                }
            }
            if (isExpanded) {
                int numberOfOpenFields = AInt32SerializerDeserializer.getInt(b, openPartOffset);
                int fieldOffset = openPartOffset + 4 + (8 * numberOfOpenFields);
                for (int i = 0; i < numberOfOpenFields; i++) {
                    // set the field name (including a type tag, which is
                    // astring)
                    int fieldValueLength =
                            NonTaggedFormatUtil.getFieldValueLength(b, fieldOffset, ATypeTag.STRING, false);
                    int fnstart = dataBos.size();
                    dataDos.writeByte(ATypeTag.SERIALIZED_STRING_TYPE_TAG);
                    dataDos.write(b, fieldOffset, fieldValueLength);
                    int fnend = dataBos.size();
                    IVisitablePointable fieldName = allocator.allocateEmpty();
                    fieldName.set(dataBos.getByteArray(), fnstart, fnend - fnstart);
                    fieldNames.add(fieldName);
                    fieldOffset += fieldValueLength;

                    // set the field type tag
                    IVisitablePointable fieldTypeTag = allocator.allocateEmpty();
                    fieldTypeTag.set(b, fieldOffset, 1);
                    fieldTypeTags.add(fieldTypeTag);
                    typeTag = EnumDeserializer.ATYPETAGDESERIALIZER.deserialize(b[fieldOffset]);

                    // set the field value (already including type tag)
                    fieldValueLength = NonTaggedFormatUtil.getFieldValueLength(b, fieldOffset, typeTag, true) + 1;

                    // allocate
                    IVisitablePointable fieldValueAccessor = allocator.allocateFieldValue(typeTag, b, fieldOffset + 1);
                    fieldValueAccessor.set(b, fieldOffset, fieldValueLength);
                    fieldValues.add(fieldValueAccessor);
                    fieldOffset += fieldValueLength;
                }
            }
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    public List<IVisitablePointable> getFieldNames() {
        return fieldNames;
    }

    public List<IVisitablePointable> getFieldTypeTags() {
        return fieldTypeTags;
    }

    public List<IVisitablePointable> getFieldValues() {
        return fieldValues;
    }

    public ARecordType getInputRecordType() {
        return inputRecType;
    }

    @Override
    public <R, T> R accept(IVisitablePointableVisitor<R, T> vistor, T tag) throws HyracksDataException {
        return vistor.visit(this, tag);
    }

}
