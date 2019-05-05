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

package org.apache.asterix.om.pointables.cast;

import java.io.ByteArrayOutputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.List;

import org.apache.asterix.builders.RecordBuilder;
import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.common.exceptions.RuntimeDataException;
import org.apache.asterix.om.pointables.ARecordVisitablePointable;
import org.apache.asterix.om.pointables.PointableAllocator;
import org.apache.asterix.om.pointables.base.DefaultOpenFieldType;
import org.apache.asterix.om.pointables.base.IVisitablePointable;
import org.apache.asterix.om.pointables.printer.adm.APrintVisitor;
import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.AUnionType;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.asterix.om.types.EnumDeserializer;
import org.apache.asterix.om.types.IAType;
import org.apache.asterix.om.types.hierachy.ATypeHierarchy;
import org.apache.asterix.om.utils.NonTaggedFormatUtil;
import org.apache.asterix.om.utils.ResettableByteArrayOutputStream;
import org.apache.hyracks.algebricks.common.utils.Pair;
import org.apache.hyracks.algebricks.common.utils.Triple;
import org.apache.hyracks.api.dataflow.value.IBinaryComparator;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.accessors.UTF8StringBinaryComparatorFactory;
import org.apache.hyracks.data.std.api.IValueReference;
import org.apache.hyracks.data.std.util.ByteArrayAccessibleOutputStream;
import org.apache.hyracks.util.string.UTF8StringWriter;

/**
 * This class is to do the runtime type cast for a record. It is ONLY visible to
 * ACastVisitor.
 */
class ARecordCaster {

    // pointable allocator
    private final PointableAllocator allocator = new PointableAllocator();
    private final List<IVisitablePointable> reqFieldNames = new ArrayList<>();
    private final List<IVisitablePointable> reqFieldTypeTags = new ArrayList<>();
    private ARecordType cachedReqType = null;
    private final ResettableByteArrayOutputStream bos = new ResettableByteArrayOutputStream();
    private final DataOutputStream dos = new DataOutputStream(bos);
    private final RecordBuilder recBuilder = new RecordBuilder();
    private final IVisitablePointable nullTypeTag = PointableAllocator.allocateUnrestableEmpty();
    private final IVisitablePointable missingTypeTag = PointableAllocator.allocateUnrestableEmpty();
    private final IBinaryComparator fieldNameComparator =
            UTF8StringBinaryComparatorFactory.INSTANCE.createBinaryComparator();
    private final ByteArrayAccessibleOutputStream outputBos = new ByteArrayAccessibleOutputStream();
    private final DataOutputStream outputDos = new DataOutputStream(outputBos);
    private final IVisitablePointable fieldTempReference = PointableAllocator.allocateUnrestableEmpty();
    private final Triple<IVisitablePointable, IAType, Boolean> nestedVisitorArg =
            new Triple<>(fieldTempReference, null, null);
    private int numInputFields = 0;
    // describe closed fields in the required type
    private int[] fieldPermutation;
    private boolean[] optionalFields;
    // describe fields (open or not) in the input records
    private boolean[] openFields;
    private int[] fieldNamesSortedIndex;
    private int[] reqFieldNamesSortedIndex;
    private final UTF8StringWriter utf8Writer = new UTF8StringWriter();

    public ARecordCaster() throws HyracksDataException {
        try {
            bos.reset();
            int start = bos.size();
            dos.writeByte(ATypeTag.SERIALIZED_MISSING_TYPE_TAG);
            int end = bos.size();
            missingTypeTag.set(bos.getByteArray(), start, end - start);
            start = bos.size();
            dos.writeByte(ATypeTag.SERIALIZED_NULL_TYPE_TAG);
            end = bos.size();
            nullTypeTag.set(bos.getByteArray(), start, end - start);
        } catch (IOException e) {
            throw HyracksDataException.create(e);
        }
    }

    public void castRecord(ARecordVisitablePointable recordAccessor, IVisitablePointable resultAccessor,
            ARecordType reqType, ACastVisitor visitor) throws HyracksDataException {
        List<IVisitablePointable> fieldNames = recordAccessor.getFieldNames();
        List<IVisitablePointable> fieldTypeTags = recordAccessor.getFieldTypeTags();
        List<IVisitablePointable> fieldValues = recordAccessor.getFieldValues();
        numInputFields = fieldNames.size();

        if (openFields == null || numInputFields > openFields.length) {
            openFields = new boolean[numInputFields];
            fieldNamesSortedIndex = new int[numInputFields];
        }
        if (!reqType.equals(cachedReqType)) {
            try {
                loadRequiredType(reqType);
            } catch (IOException e) {
                throw HyracksDataException.create(e);
            }
        }

        // clear the previous states
        reset();
        matchClosedPart(fieldNames, fieldTypeTags);
        writeOutput(fieldNames, fieldTypeTags, fieldValues, outputDos, visitor);
        resultAccessor.set(outputBos.getByteArray(), 0, outputBos.size());
    }

    private void reset() {
        for (int i = 0; i < numInputFields; i++) {
            openFields[i] = true;
        }
        for (int i = 0; i < fieldPermutation.length; i++) {
            fieldPermutation[i] = -1;
        }
        for (int i = 0; i < numInputFields; i++) {
            fieldNamesSortedIndex[i] = i;
        }
        outputBos.reset();
    }

    private void loadRequiredType(ARecordType reqType) throws IOException {
        reqFieldNames.clear();
        reqFieldTypeTags.clear();
        allocator.reset();

        cachedReqType = reqType;
        int numSchemaFields = reqType.getFieldTypes().length;
        IAType[] fieldTypes = reqType.getFieldTypes();
        String[] fieldNames = reqType.getFieldNames();
        fieldPermutation = new int[numSchemaFields];
        optionalFields = new boolean[numSchemaFields];
        for (int i = 0; i < optionalFields.length; i++) {
            optionalFields[i] = false;
        }

        bos.reset(nullTypeTag.getStartOffset() + nullTypeTag.getLength());
        for (int i = 0; i < numSchemaFields; i++) {
            ATypeTag ftypeTag = fieldTypes[i].getTypeTag();
            String fname = fieldNames[i];

            // add type tag pointable
            if (NonTaggedFormatUtil.isOptional(fieldTypes[i])) {
                // optional field: add the embedded non-null type tag
                ftypeTag = ((AUnionType) fieldTypes[i]).getActualType().getTypeTag();
                optionalFields[i] = true;
            }
            int tagStart = bos.size();
            dos.writeByte(ftypeTag.serialize());
            int tagEnd = bos.size();
            IVisitablePointable typeTagPointable = allocator.allocateEmpty();
            typeTagPointable.set(bos.getByteArray(), tagStart, tagEnd - tagStart);
            reqFieldTypeTags.add(typeTagPointable);

            // add type name pointable (including a string type tag)
            int nameStart = bos.size();
            dos.writeByte(ATypeTag.SERIALIZED_STRING_TYPE_TAG);
            utf8Writer.writeUTF8(fname, dos);
            int nameEnd = bos.size();
            IVisitablePointable typeNamePointable = allocator.allocateEmpty();
            typeNamePointable.set(bos.getByteArray(), nameStart, nameEnd - nameStart);
            reqFieldNames.add(typeNamePointable);
        }

        reqFieldNamesSortedIndex = new int[reqFieldNames.size()];
        for (int i = 0; i < reqFieldNamesSortedIndex.length; i++) {
            reqFieldNamesSortedIndex[i] = i;
        }
        // sort the field name index
        quickSort(reqFieldNamesSortedIndex, reqFieldNames, 0, reqFieldNamesSortedIndex.length - 1);
    }

    private void matchClosedPart(List<IVisitablePointable> fieldNames, List<IVisitablePointable> fieldTypeTags)
            throws HyracksDataException {
        // sort-merge based match
        quickSort(fieldNamesSortedIndex, fieldNames, 0, numInputFields - 1);
        int fnStart = 0;
        int reqFnStart = 0;
        while (fnStart < numInputFields && reqFnStart < reqFieldNames.size()) {
            int fnPos = fieldNamesSortedIndex[fnStart];
            int reqFnPos = reqFieldNamesSortedIndex[reqFnStart];
            int c = compare(fieldNames.get(fnPos), reqFieldNames.get(reqFnPos));
            if (c == 0) {
                IVisitablePointable fieldTypeTag = fieldTypeTags.get(fnPos);
                IVisitablePointable reqFieldTypeTag = reqFieldTypeTags.get(reqFnPos);
                if (fieldTypeTag.equals(reqFieldTypeTag) || (
                // match the null type of optional field
                optionalFields[reqFnPos] && (fieldTypeTag.equals(nullTypeTag))
                        || fieldTypeTag.equals(missingTypeTag))) {
                    fieldPermutation[reqFnPos] = fnPos;
                    openFields[fnPos] = false;
                } else {
                    // if mismatch, check whether input type can be promoted to the required type
                    ATypeTag inputTypeTag = EnumDeserializer.ATYPETAGDESERIALIZER
                            .deserialize(fieldTypeTag.getByteArray()[fieldTypeTag.getStartOffset()]);
                    ATypeTag requiredTypeTag = EnumDeserializer.ATYPETAGDESERIALIZER
                            .deserialize(reqFieldTypeTag.getByteArray()[reqFieldTypeTag.getStartOffset()]);

                    if (ATypeHierarchy.canPromote(inputTypeTag, requiredTypeTag)
                            || ATypeHierarchy.canDemote(inputTypeTag, requiredTypeTag)) {
                        fieldPermutation[reqFnPos] = fnPos;
                        openFields[fnPos] = false;
                    } else {
                        throw new RuntimeDataException(ErrorCode.CASTING_FIELD, inputTypeTag, requiredTypeTag);
                    }
                }
                fnStart++;
                reqFnStart++;
            }
            if (c > 0) {
                reqFnStart++;
            }
            if (c < 0) {
                fnStart++;
            }
        }

        // check unmatched fields in the input type
        for (int i = 0; i < openFields.length; i++) {
            if (openFields[i] && !cachedReqType.isOpen()) {
                //print the field name
                IVisitablePointable fieldName = fieldNames.get(i);
                ByteArrayOutputStream fieldBos = new ByteArrayOutputStream();
                PrintStream ps = new PrintStream(fieldBos);
                APrintVisitor printVisitor = new APrintVisitor();
                Pair<PrintStream, ATypeTag> visitorArg = new Pair<>(ps, ATypeTag.STRING);
                fieldName.accept(printVisitor, visitorArg);

                //print the colon
                ps.print(":");

                //print the field type
                IVisitablePointable fieldType = fieldTypeTags.get(i);
                ATypeTag typeTag = EnumDeserializer.ATYPETAGDESERIALIZER
                        .deserialize(fieldType.getByteArray()[fieldType.getStartOffset()]);
                ps.print(typeTag);

                //collect the output message and throw the exception
                throw new HyracksDataException("type mismatch: including an extra field " + fieldBos.toString());
            }
        }

        // check unmatched fields in the required type
        for (int i = 0; i < fieldPermutation.length; i++) {
            if (fieldPermutation[i] < 0) {
                IAType t = cachedReqType.getFieldTypes()[i];
                if (!NonTaggedFormatUtil.isOptional(t)) {
                    // no matched field in the input for a required closed field
                    throw new HyracksDataException("type mismatch: missing a required closed field "
                            + cachedReqType.getFieldNames()[i] + ": " + t.getTypeName());
                }
            }
        }
    }

    private void writeOutput(List<IVisitablePointable> fieldNames, List<IVisitablePointable> fieldTypeTags,
            List<IVisitablePointable> fieldValues, DataOutput output, ACastVisitor visitor)
            throws HyracksDataException {
        // reset the states of the record builder
        recBuilder.reset(cachedReqType);
        recBuilder.init();

        // write the closed part
        for (int i = 0; i < fieldPermutation.length; i++) {
            final int pos = fieldPermutation[i];
            final IVisitablePointable field = pos >= 0 ? fieldValues.get(pos) : missingTypeTag;
            final IAType fType = cachedReqType.getFieldTypes()[i];
            nestedVisitorArg.second = fType;

            // recursively casting, the result of casting can always be thought
            // as flat
            if (optionalFields[i]) {
                //the field is optional in the input record
                IVisitablePointable fieldTypeTag = pos >= 0 ? fieldTypeTags.get(pos) : null;
                if (fieldTypeTag == null || fieldTypeTag.equals(missingTypeTag)) {
                    nestedVisitorArg.second = BuiltinType.AMISSING;
                } else if (fieldTypeTag.equals(nullTypeTag)) {
                    nestedVisitorArg.second = BuiltinType.ANULL;
                } else {
                    nestedVisitorArg.second = ((AUnionType) fType).getActualType();
                }
            }
            field.accept(visitor, nestedVisitorArg);
            recBuilder.addField(i, nestedVisitorArg.first);
        }

        // write the open part
        for (int i = 0; i < numInputFields; i++) {
            if (openFields[i]) {
                IVisitablePointable name = fieldNames.get(i);
                IVisitablePointable field = fieldValues.get(i);
                IVisitablePointable fieldTypeTag = fieldTypeTags.get(i);

                ATypeTag typeTag = EnumDeserializer.ATYPETAGDESERIALIZER
                        .deserialize(fieldTypeTag.getByteArray()[fieldTypeTag.getStartOffset()]);
                nestedVisitorArg.second = DefaultOpenFieldType.getDefaultOpenFieldType(typeTag);
                field.accept(visitor, nestedVisitorArg);
                recBuilder.addField(name, nestedVisitorArg.first);
            }
        }
        recBuilder.write(output, true);
    }

    private void quickSort(int[] index, List<IVisitablePointable> names, int start, int end)
            throws HyracksDataException {
        if (end <= start) {
            return;
        }
        int i = partition(index, names, start, end);
        quickSort(index, names, start, i - 1);
        quickSort(index, names, i + 1, end);
    }

    private int partition(int[] index, List<IVisitablePointable> names, int left, int right)
            throws HyracksDataException {
        int i = left - 1;
        int j = right;
        while (true) {
            // grow from the left
            while (compare(names.get(index[++i]), names.get(index[right])) < 0) {
                ;
            }
            // lower from the right
            while (compare(names.get(index[right]), names.get(index[--j])) < 0) {
                if (j == left) {
                    break;
                }
            }
            if (i >= j) {
                break;
            }
            // swap i and j
            swap(index, i, j);
        }
        // swap i and right
        swap(index, i, right); // swap with partition element
        return i;
    }

    private void swap(int[] array, int i, int j) {
        int temp = array[i];
        array[i] = array[j];
        array[j] = temp;
    }

    private int compare(IValueReference a, IValueReference b) throws HyracksDataException {
        // start+1 and len-1 due to the type tag
        return fieldNameComparator.compare(a.getByteArray(), a.getStartOffset() + 1, a.getLength() - 1,
                b.getByteArray(), b.getStartOffset() + 1, b.getLength() - 1);
    }
}
