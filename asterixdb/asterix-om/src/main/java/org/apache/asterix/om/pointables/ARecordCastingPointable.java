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

import static org.apache.asterix.om.pointables.AFlatValueCastingPointable.missingPointable;

import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.asterix.builders.RecordBuilder;
import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.common.exceptions.RuntimeDataException;
import org.apache.asterix.dataflow.data.nontagged.serde.AInt32SerializerDeserializer;
import org.apache.asterix.om.pointables.base.DefaultOpenFieldType;
import org.apache.asterix.om.pointables.base.ICastingPointable;
import org.apache.asterix.om.pointables.cast.ACastingPointableVisitor;
import org.apache.asterix.om.pointables.cast.CastResult;
import org.apache.asterix.om.pointables.visitor.ICastingPointableVisitor;
import org.apache.asterix.om.typecomputer.impl.TypeComputeUtils;
import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.AUnionType;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.asterix.om.types.EnumDeserializer;
import org.apache.asterix.om.types.IAType;
import org.apache.asterix.om.types.TypeTagUtil;
import org.apache.asterix.om.types.hierachy.ATypeHierarchy;
import org.apache.asterix.om.util.container.IObjectFactory;
import org.apache.asterix.om.utils.NonTaggedFormatUtil;
import org.apache.asterix.om.utils.RecordUtil;
import org.apache.asterix.om.utils.ResettableByteArrayOutputStream;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.api.IPointable;
import org.apache.hyracks.data.std.primitive.VoidPointable;
import org.apache.hyracks.data.std.util.ByteArrayAccessibleOutputStream;
import org.apache.hyracks.util.string.UTF8StringUtil;
import org.apache.hyracks.util.string.UTF8StringWriter;

import it.unimi.dsi.fastutil.ints.IntArrayList;

public class ARecordCastingPointable extends AbstractCastingPointable {

    public static final IObjectFactory<ARecordCastingPointable, IAType> FACTORY =
            type -> new ARecordCastingPointable((ARecordType) type);

    private final CastResult fieldCastResult = new CastResult(new VoidPointable(), null);
    private final ResettableByteArrayOutputStream inClosedFieldsNames = new ResettableByteArrayOutputStream();
    private final ResettableByteArrayOutputStream reqFieldNames = new ResettableByteArrayOutputStream();
    private final DataOutputStream reqFieldNamesDos = new DataOutputStream(reqFieldNames);
    private final ByteArrayAccessibleOutputStream outBos = new ByteArrayAccessibleOutputStream();
    private final DataOutputStream outDos = new DataOutputStream(outBos);
    private final UTF8StringWriter utf8Writer = new UTF8StringWriter();
    private final RecordBuilder recBuilder = new RecordBuilder();
    private final IPointable namePointable = new VoidPointable();
    private final ARecordType inRecType;
    private ARecordType cachedReqType;

    private final IntArrayList reqFieldsNamesOffset = new IntArrayList();
    private final IntArrayList inFieldsNamesOffset;
    private final IntArrayList inFieldsNamesLength;
    private final IntArrayList inFieldsOffsets;
    private final IntArrayList inFieldsLength;
    private final List<ATypeTag> inFieldTags;
    private final int inSchemaFieldsCount;
    private int inFieldsCount;
    private boolean[] inFieldsToOpen;
    private int[] inFieldNamesSorted;
    private int[] reqFieldsPositions;
    private int[] reqFieldNamesSorted;
    private boolean[] reqOptionalFields;

    public ARecordCastingPointable(ARecordType inputType) {
        this.inRecType = inputType;
        IAType[] fieldTypes = inputType.getFieldTypes();
        String[] fieldNames = inputType.getFieldNames();
        inSchemaFieldsCount = fieldTypes.length;
        inFieldsNamesOffset = new IntArrayList(inSchemaFieldsCount);
        inFieldsNamesLength = new IntArrayList(inSchemaFieldsCount);
        inFieldsOffsets = new IntArrayList(inSchemaFieldsCount);
        inFieldsLength = new IntArrayList(inSchemaFieldsCount);
        inFieldTags = new ArrayList<>(inSchemaFieldsCount);
        try {
            DataOutputStream dos = new DataOutputStream(inClosedFieldsNames);
            for (int i = 0; i < inSchemaFieldsCount; i++) {
                int nameStart = inClosedFieldsNames.size();
                inFieldsNamesOffset.add(nameStart);
                utf8Writer.writeUTF8(fieldNames[i], dos);
                int nameEnd = inClosedFieldsNames.size();
                inFieldsNamesLength.add(nameEnd - nameStart);
            }
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }

    @Override
    public <R, T> R accept(ICastingPointableVisitor<R, T> visitor, T arg) throws HyracksDataException {
        return visitor.visit(this, arg);
    }

    @Override
    public Type getType() {
        return Type.RECORD;
    }

    @Override
    protected void reset() {
        super.reset();
        inFieldsNamesOffset.size(inSchemaFieldsCount);
        inFieldsNamesLength.size(inSchemaFieldsCount);
        inFieldsOffsets.clear();
        inFieldsLength.clear();
        inFieldTags.clear();
    }

    @Override
    public void prepare(byte[] b, int start) {
        reset();
        prepareRecordInfo(b, start);
    }

    private void prepareRecordInfo(byte[] b, int start) {
        boolean isExpanded = false;
        int openPartOffset = 0;
        int tagByte = isTagged() ? 1 : 0;
        int adjust = isTagged() ? 0 : 1;
        // advance to either isExpanded or numberOfSchemaFields
        int pointer = start + 4 + tagByte;
        if (inRecType.isOpen()) {
            isExpanded = b[pointer] == 1;
            // advance either to openPartOffset or numberOfSchemaFields
            pointer += 1;
            if (isExpanded) {
                openPartOffset = start + AInt32SerializerDeserializer.getInt(b, pointer) - adjust;
                // advance to numberOfSchemaFields
                pointer += 4;
            }
        }
        try {
            if (inSchemaFieldsCount > 0) {
                // advance to nullBitMap if hasOptionalFields, or fieldOffsets
                pointer += 4;
                prepareClosedFieldsInfo(b, start, adjust, pointer);
            }
            int openFieldsCount = 0;
            if (isExpanded) {
                openFieldsCount = AInt32SerializerDeserializer.getInt(b, openPartOffset);
                prepareOpenFieldsInfo(b, openPartOffset, openFieldsCount);
            }
            resetForNewInFields(openFieldsCount);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private void prepareClosedFieldsInfo(byte[] b, int start, int adjust, int currentPointer)
            throws HyracksDataException {
        int offsetArrayPosition;
        int nullBitMapPosition = 0;
        boolean hasOptionalFields = NonTaggedFormatUtil.hasOptionalField(inRecType);
        if (hasOptionalFields) {
            nullBitMapPosition = currentPointer;
            offsetArrayPosition = currentPointer
                    + (this.inSchemaFieldsCount % 4 == 0 ? inSchemaFieldsCount / 4 : inSchemaFieldsCount / 4 + 1);
        } else {
            offsetArrayPosition = currentPointer;
        }
        for (int field = 0; field < inSchemaFieldsCount; field++, offsetArrayPosition += 4) {
            int fieldOffset = AInt32SerializerDeserializer.getInt(b, offsetArrayPosition) + start - adjust;
            inFieldsOffsets.add(fieldOffset);
            if (hasOptionalFields) {
                byte byteTag = b[nullBitMapPosition + field / 4];
                if (RecordUtil.isNull(byteTag, field)) {
                    inFieldsLength.add(0);
                    inFieldTags.add(ATypeTag.NULL);
                    continue;
                }
                if (RecordUtil.isMissing(byteTag, field)) {
                    inFieldsLength.add(0);
                    inFieldTags.add(ATypeTag.MISSING);
                    continue;
                }
            }
            IAType[] fieldTypes = inRecType.getFieldTypes();
            int fieldValueLength;
            ATypeTag fieldTag;
            if (fieldTypes[field].getTypeTag() == ATypeTag.UNION) {
                if (((AUnionType) fieldTypes[field]).isUnknownableType()) {
                    IAType fieldType = ((AUnionType) fieldTypes[field]).getActualType();
                    fieldTag = fieldType.getTypeTag();
                    fieldValueLength = NonTaggedFormatUtil.getFieldValueLength(b, fieldOffset, fieldTag, false);
                } else {
                    throw new IllegalStateException("Got unexpected UNION type " + fieldTypes[field]);
                }
            } else {
                fieldTag = fieldTypes[field].getTypeTag();
                fieldValueLength = NonTaggedFormatUtil.getFieldValueLength(b, fieldOffset, fieldTag, false);
            }
            inFieldsLength.add(fieldValueLength);
            inFieldTags.add(fieldTag);
        }
    }

    private void prepareOpenFieldsInfo(byte[] b, int openPartOffset, int openFieldsCount) throws HyracksDataException {
        int fieldOffset = openPartOffset + 4 + (8 * openFieldsCount);
        for (int i = 0; i < openFieldsCount; i++) {
            inFieldsNamesOffset.add(fieldOffset);
            int fieldValueLen = NonTaggedFormatUtil.getFieldValueLength(b, fieldOffset, ATypeTag.STRING, false);
            inFieldsNamesLength.add(fieldValueLen);
            fieldOffset += fieldValueLen;
            ATypeTag fieldTag = EnumDeserializer.ATYPETAGDESERIALIZER.deserialize(b[fieldOffset]);
            inFieldTags.add(fieldTag);
            inFieldsOffsets.add(fieldOffset);
            // +1 to include the tag in the length since field offset starts at the tag
            fieldValueLen = NonTaggedFormatUtil.getFieldValueLength(b, fieldOffset, fieldTag, true) + 1;
            inFieldsLength.add(fieldValueLen);
            fieldOffset += fieldValueLen;
        }
    }

    private void resetForNewInFields(int openFieldsCount) {
        inFieldsCount = inSchemaFieldsCount + openFieldsCount;
        if (inFieldsToOpen == null || inFieldsCount > inFieldsToOpen.length) {
            inFieldsToOpen = new boolean[inFieldsCount];
            inFieldNamesSorted = new int[inFieldsCount];
        }
        for (int i = 0; i < inFieldsCount; i++) {
            inFieldsToOpen[i] = true;
            inFieldNamesSorted[i] = i;
        }
    }

    public void castRecord(IPointable castOutResult, ARecordType reqType, ACastingPointableVisitor visitor)
            throws HyracksDataException {
        ensureRequiredTypeSatisfied(reqType);
        outBos.reset();
        writeOutput(outDos, visitor);
        castOutResult.set(outBos.getByteArray(), 0, outBos.size());
    }

    private void ensureRequiredTypeSatisfied(ARecordType reqType) throws HyracksDataException {
        if (!reqType.equals(cachedReqType)) {
            loadRequiredType(reqType);
        }
        Arrays.fill(reqFieldsPositions, -1);
        ensureClosedPart();
    }

    private void loadRequiredType(ARecordType reqType) throws HyracksDataException {
        cachedReqType = reqType;
        IAType[] fieldTypes = reqType.getFieldTypes();
        String[] fieldNames = reqType.getFieldNames();
        int numSchemaFields = fieldTypes.length;
        reqFieldsNamesOffset.size(numSchemaFields);
        reqFieldsNamesOffset.clear();
        reqFieldsPositions = new int[numSchemaFields];
        reqOptionalFields = new boolean[numSchemaFields];
        reqFieldNamesSorted = new int[numSchemaFields];
        reqFieldNames.reset();
        for (int i = 0; i < numSchemaFields; i++) {
            int nameStart = reqFieldNames.size(); // TODO use this?
            String reqFieldName = fieldNames[i];
            try {
                utf8Writer.writeUTF8(reqFieldName, reqFieldNamesDos);
            } catch (IOException e) {
                throw HyracksDataException.create(e);
            }
            reqFieldsNamesOffset.add(nameStart);
            if (NonTaggedFormatUtil.isOptional(fieldTypes[i])) {
                reqOptionalFields[i] = true;
            }
        }
        for (int i = 0; i < reqFieldNamesSorted.length; i++) {
            reqFieldNamesSorted[i] = i;
        }
        // sort the field name index
        quickSort(reqFieldNamesSorted, this::compareRequiredFieldNames, 0, reqFieldNamesSorted.length - 1);
    }

    private void ensureClosedPart() throws HyracksDataException {
        int inFnStart = 0;
        int reqFnStart = 0;
        if (inFnStart < inFieldsCount && reqFnStart < reqFieldsNamesOffset.size()) {
            quickSort(inFieldNamesSorted, this::compareInputFieldNames, 0, inFieldsCount - 1);
        }
        while (inFnStart < inFieldsCount && reqFnStart < reqFieldsNamesOffset.size()) {
            int inFnPos = inFieldNamesSorted[inFnStart];
            int reqFnPos = reqFieldNamesSorted[reqFnStart];
            int c = compareInputToRequired(inFnPos, reqFnPos);
            if (c == 0) {
                ATypeTag inFieldTag = inFieldTags.get(inFnPos);
                ATypeTag reqFieldTag = getRequiredFieldTag(reqFnPos);
                if (inFieldTag == reqFieldTag || acceptsNullMissing(reqFnPos, inFieldTag)
                        || canPromoteDemote(inFieldTag, reqFieldTag)) {
                    reqFieldsPositions[reqFnPos] = inFnPos;
                    inFieldsToOpen[inFnPos] = false;
                } else {
                    throw new RuntimeDataException(ErrorCode.CASTING_FIELD, inFieldTag, reqFieldTag);
                }
                inFnStart++;
                reqFnStart++;
            }
            if (c > 0) {
                reqFnStart++;
            }
            if (c < 0) {
                inFnStart++;
            }
        }

        for (int i = 0; i < inFieldsToOpen.length; i++) {
            if (inFieldsToOpen[i] && !cachedReqType.isOpen()) {
                // required type is closed and cannot allow extra fields to be placed in the open section
                String fieldName = UTF8StringUtil.toString(getInFieldNames(i), inFieldsNamesOffset.getInt(i));
                ATypeTag fieldTag = inFieldTags.get(i);
                throw new RuntimeDataException(ErrorCode.TYPE_MISMATCH_EXTRA_FIELD, fieldName + ":" + fieldTag);
            }
        }

        for (int i = 0; i < reqFieldsPositions.length; i++) {
            if (reqFieldsPositions[i] < 0) {
                IAType t = cachedReqType.getFieldTypes()[i];
                // if the (required) field is optional, it's ok if it does not exist in the input record
                if (!NonTaggedFormatUtil.isOptional(t)) {
                    // the field is required and not optional, fail since the input record does not have it
                    throw new RuntimeDataException(ErrorCode.TYPE_MISMATCH_MISSING_FIELD,
                            cachedReqType.getFieldNames()[i], t.getTypeName());
                }
            }
        }
    }

    private byte[] getInFieldNames(int fieldId) {
        return fieldId < inSchemaFieldsCount ? inClosedFieldsNames.getByteArray() : data;
    }

    private ATypeTag getRequiredFieldTag(int fieldId) {
        return TypeComputeUtils.getActualType(cachedReqType.getFieldTypes()[fieldId]).getTypeTag();
    }

    private static boolean canPromoteDemote(ATypeTag inFieldTag, ATypeTag reqFieldTag) {
        return ATypeHierarchy.canPromote(inFieldTag, reqFieldTag) || ATypeHierarchy.canDemote(inFieldTag, reqFieldTag);
    }

    private boolean acceptsNullMissing(int reqFnPos, ATypeTag inFieldTag) {
        return reqOptionalFields[reqFnPos] && (inFieldTag == ATypeTag.NULL || inFieldTag == ATypeTag.MISSING);
    }

    private void writeOutput(DataOutput output, ACastingPointableVisitor visitor) throws HyracksDataException {
        recBuilder.reset(cachedReqType);
        recBuilder.init();
        writeClosedPart(visitor);
        writeOpenPart(visitor);
        recBuilder.write(output, true);
    }

    private void writeClosedPart(ACastingPointableVisitor visitor) throws HyracksDataException {
        for (int i = 0; i < reqFieldsPositions.length; i++) {
            int pos = reqFieldsPositions[i];
            ICastingPointable field = pos >= 0 ? allocateField(pos) : missingPointable;
            IAType fieldType = cachedReqType.getFieldTypes()[i];
            fieldCastResult.setOutType(fieldType);
            if (reqOptionalFields[i]) {
                ATypeTag fieldTag = pos >= 0 ? inFieldTags.get(pos) : null;
                if (fieldTag == null || fieldTag == ATypeTag.MISSING) {
                    fieldCastResult.setOutType(BuiltinType.AMISSING);
                } else if (fieldTag == ATypeTag.NULL) {
                    fieldCastResult.setOutType(BuiltinType.ANULL);
                } else {
                    fieldCastResult.setOutType(((AUnionType) fieldType).getActualType());
                }
            }
            field.accept(visitor, fieldCastResult);
            recBuilder.addField(i, fieldCastResult.getOutPointable());
            free(field);
        }
    }

    private void writeOpenPart(ACastingPointableVisitor visitor) throws HyracksDataException {
        for (int i = 0; i < inFieldsCount; i++) {
            if (inFieldsToOpen[i]) {
                ICastingPointable field = allocateField(i);
                ATypeTag fieldTag = inFieldTags.get(i);
                fieldCastResult.setOutType(DefaultOpenFieldType.getDefaultOpenFieldType(fieldTag));
                field.accept(visitor, fieldCastResult);
                IPointable name = getInFieldName(i);
                recBuilder.addNonTaggedFieldName(name, fieldCastResult.getOutPointable());
                free(field);
            }
        }
    }

    private ICastingPointable allocateField(int fieldId) throws HyracksDataException {
        IAType fieldType;
        boolean openField;
        ATypeTag fieldTag = inFieldTags.get(fieldId);
        if (fieldId < inSchemaFieldsCount) {
            openField = false;
            fieldType = TypeComputeUtils.getActualType(inRecType.getFieldTypes()[fieldId]);
        } else {
            openField = true;
            fieldType = TypeTagUtil.getBuiltinTypeByTag(fieldTag);
        }
        return allocate(data, inFieldsOffsets.getInt(fieldId), inFieldsLength.getInt(fieldId), fieldType, fieldTag,
                openField);
    }

    private IPointable getInFieldName(int fieldId) {
        if (fieldId < inSchemaFieldsCount) {
            namePointable.set(inClosedFieldsNames.getByteArray(), inFieldsNamesOffset.getInt(fieldId),
                    inFieldsNamesLength.getInt(fieldId));
        } else {
            namePointable.set(data, inFieldsNamesOffset.getInt(fieldId), inFieldsNamesLength.getInt(fieldId));
        }
        return namePointable;
    }

    private void quickSort(int[] index, Comparator comparator, int start, int end) {
        if (end <= start) {
            return;
        }
        int i = partition(index, comparator, start, end);
        quickSort(index, comparator, start, i - 1);
        quickSort(index, comparator, i + 1, end);
    }

    private int partition(int[] index, Comparator comparator, int left, int right) {
        int i = left - 1;
        int j = right;
        while (true) {
            while (comparator.compare(index[++i], index[right]) < 0) {
                ;
            }
            while (comparator.compare(index[right], index[--j]) < 0) {
                if (j == left) {
                    break;
                }
            }
            if (i >= j) {
                break;
            }
            swap(index, i, j);
        }
        swap(index, i, right);
        return i;
    }

    private void swap(int[] array, int i, int j) {
        int temp = array[i];
        array[i] = array[j];
        array[j] = temp;
    }

    private int compareInputToRequired(int a, int b) {
        return UTF8StringUtil.compareTo(getInFieldNames(a), inFieldsNamesOffset.getInt(a), reqFieldNames.getByteArray(),
                reqFieldsNamesOffset.getInt(b));
    }

    private int compareInputFieldNames(int i, int j) {
        return UTF8StringUtil.compareTo(getInFieldNames(i), inFieldsNamesOffset.getInt(i), getInFieldNames(j),
                inFieldsNamesOffset.getInt(j));
    }

    private int compareRequiredFieldNames(int i, int j) {
        return UTF8StringUtil.compareTo(reqFieldNames.getByteArray(), reqFieldsNamesOffset.getInt(i),
                reqFieldNames.getByteArray(), reqFieldsNamesOffset.getInt(j));
    }

    @FunctionalInterface
    private interface Comparator {
        int compare(int i, int j);
    }
}
