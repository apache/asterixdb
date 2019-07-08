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
package org.apache.asterix.runtime.evaluators.functions.records;

import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.asterix.builders.RecordBuilder;
import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.common.exceptions.RuntimeDataException;
import org.apache.asterix.om.pointables.ARecordVisitablePointable;
import org.apache.asterix.om.pointables.PointableAllocator;
import org.apache.asterix.om.pointables.base.DefaultOpenFieldType;
import org.apache.asterix.om.pointables.base.IVisitablePointable;
import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.IAType;
import org.apache.asterix.om.types.runtime.RuntimeRecordTypeInfo;
import org.apache.asterix.runtime.evaluators.comparisons.DeepEqualAssessor;
import org.apache.asterix.runtime.evaluators.functions.AbstractScalarEval;
import org.apache.asterix.runtime.evaluators.functions.PointableHelper;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import org.apache.hyracks.algebricks.runtime.base.IEvaluatorContext;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluator;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluatorFactory;
import org.apache.hyracks.api.dataflow.value.IBinaryComparator;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.exceptions.SourceLocation;
import org.apache.hyracks.data.std.accessors.UTF8StringBinaryComparatorFactory;
import org.apache.hyracks.data.std.api.IPointable;
import org.apache.hyracks.data.std.primitive.VoidPointable;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.apache.hyracks.dataflow.common.data.accessors.IFrameTupleReference;

/**
 * record merge evaluator is used to combine two records with no matching fieldnames
 * If both records have the same fieldname for a non-record field anywhere in the schema, the merge will fail
 * This function is performed on a recursive level, meaning that nested records can be combined
 * for instance if both records have a nested field called "metadata"
 * where metadata from A is {"comments":"this rocks"} and metadata from B is {"index":7, "priority":5}
 * Records A and B can be combined yielding a nested record called "metadata"
 * That will have all three fields
 */

public class RecordMergeEvaluator extends AbstractScalarEval {

    private final boolean isIgnoreDuplicates;

    private final ARecordType outRecType;

    private final IVisitablePointable vp0;
    private final IVisitablePointable vp1;

    private final IPointable argPtr0 = new VoidPointable();
    private final IPointable argPtr1 = new VoidPointable();

    private final IScalarEvaluator eval0;
    private final IScalarEvaluator eval1;

    private final List<RecordBuilder> rbStack = new ArrayList<>();

    private final ArrayBackedValueStorage tabvs = new ArrayBackedValueStorage();
    private final IBinaryComparator stringBinaryComparator =
            UTF8StringBinaryComparatorFactory.INSTANCE.createBinaryComparator();

    private final RuntimeRecordTypeInfo runtimeRecordTypeInfo = new RuntimeRecordTypeInfo();
    private final DeepEqualAssessor deepEqualAssessor = new DeepEqualAssessor();
    private ArrayBackedValueStorage resultStorage = new ArrayBackedValueStorage();
    private DataOutput out = resultStorage.getDataOutput();

    RecordMergeEvaluator(IEvaluatorContext ctx, IScalarEvaluatorFactory[] args, IAType[] argTypes,
            SourceLocation sourceLocation, FunctionIdentifier identifier, boolean isIgnoreDuplicates)
            throws HyracksDataException {
        super(sourceLocation, identifier);

        this.isIgnoreDuplicates = isIgnoreDuplicates;

        eval0 = args[0].createScalarEvaluator(ctx);
        eval1 = args[1].createScalarEvaluator(ctx);

        outRecType = (ARecordType) argTypes[0];
        ARecordType inRecType0 = (ARecordType) argTypes[1];
        ARecordType inRecType1 = (ARecordType) argTypes[2];

        PointableAllocator pa = new PointableAllocator();
        vp0 = pa.allocateRecordValue(inRecType0);
        vp1 = pa.allocateRecordValue(inRecType1);
    }

    @Override
    public void evaluate(IFrameTupleReference tuple, IPointable result) throws HyracksDataException {
        resultStorage.reset();
        eval0.evaluate(tuple, argPtr0);
        eval1.evaluate(tuple, argPtr1);

        if (PointableHelper.checkAndSetMissingOrNull(result, argPtr0, argPtr1)) {
            return;
        }

        vp0.set(argPtr0);
        vp1.set(argPtr1);

        ARecordVisitablePointable rp0 = (ARecordVisitablePointable) vp0;
        ARecordVisitablePointable rp1 = (ARecordVisitablePointable) vp1;

        try {
            mergeFields(outRecType, rp0, rp1, 0);
            rbStack.get(0).write(out, true);
        } catch (IOException e) {
            throw HyracksDataException.create(e);
        }
        result.set(resultStorage);
    }

    private void mergeFields(ARecordType combinedType, ARecordVisitablePointable leftRecord,
            ARecordVisitablePointable rightRecord, int nestedLevel) throws IOException {
        if (rbStack.size() < (nestedLevel + 1)) {
            rbStack.add(new RecordBuilder());
        }

        rbStack.get(nestedLevel).reset(combinedType);
        rbStack.get(nestedLevel).init();

        // Add all fields from left record
        for (int i = 0; i < leftRecord.getFieldNames().size(); i++) {
            IVisitablePointable leftName = leftRecord.getFieldNames().get(i);
            IVisitablePointable leftValue = leftRecord.getFieldValues().get(i);
            IVisitablePointable leftType = leftRecord.getFieldTypeTags().get(i);

            // Check if a match for the left record exists on the right record
            boolean foundMatch = false;
            for (int j = 0; j < rightRecord.getFieldNames().size(); j++) {
                IVisitablePointable rightName = rightRecord.getFieldNames().get(j);
                IVisitablePointable rightValue = rightRecord.getFieldValues().get(j);
                IVisitablePointable rightType = rightRecord.getFieldTypeTags().get(j);

                // Check if same field name and not same value exists (same name and value, just take the left one)
                if (PointableHelper.isEqual(leftName, rightName, stringBinaryComparator)
                        && !deepEqualAssessor.isEqual(leftValue, rightValue)) {

                    // Same name, different value, both of type Record, do nested join
                    if (PointableHelper.sameType(ATypeTag.OBJECT, rightType)
                            && PointableHelper.sameType(ATypeTag.OBJECT, leftType)) {
                        // We are merging two sub records
                        addFieldToSubRecord(combinedType, leftName, leftValue, rightValue, nestedLevel);
                        foundMatch = true;
                    }
                    // Same name, different value, not of type Record, handle duplicate field
                    else {
                        // Ignore and take left field if ignore duplicate flag is true, otherwise, throw an exception
                        if (!isIgnoreDuplicates) {
                            throw new RuntimeDataException(ErrorCode.DUPLICATE_FIELD_NAME, functionIdentifier);
                        }
                    }
                }
            }

            // If no match is found, we add the left field
            if (!foundMatch) {
                addFieldToSubRecord(combinedType, leftName, leftValue, null, nestedLevel);
            }

        }

        // Repeat for right side (ignoring duplicates this time, all duplicates were handled on the previous step)
        for (int j = 0; j < rightRecord.getFieldNames().size(); j++) {
            IVisitablePointable rightName = rightRecord.getFieldNames().get(j);
            IVisitablePointable rightValue = rightRecord.getFieldValues().get(j);
            boolean foundMatch = false;
            for (int i = 0; i < leftRecord.getFieldNames().size(); i++) {
                IVisitablePointable leftName = leftRecord.getFieldNames().get(i);
                if (rightName.equals(leftName)) {
                    foundMatch = true;
                }
            }

            // If no match is found, we add the right field
            if (!foundMatch) {
                addFieldToSubRecord(combinedType, rightName, rightValue, null, nestedLevel);
            }
        }
    }

    /*
     * Takes in a record type, field name, and the field values (which are record) from two records
     * Merges them into one record of combinedType
     * And adds that record as a field to the Record in subrb
     * the second value can be null, indicated that you just add the value of left as a field to subrb
     *
     */
    private void addFieldToSubRecord(ARecordType combinedType, IVisitablePointable fieldNamePointable,
            IVisitablePointable leftValue, IVisitablePointable rightValue, int nestedLevel) throws IOException {

        runtimeRecordTypeInfo.reset(combinedType);
        int pos = runtimeRecordTypeInfo.getFieldIndex(fieldNamePointable.getByteArray(),
                fieldNamePointable.getStartOffset() + 1, fieldNamePointable.getLength() - 1);

        //Add the merged field
        if (combinedType != null && pos >= 0) {
            if (rightValue == null) {
                rbStack.get(nestedLevel).addField(pos, leftValue);
            } else {
                mergeFields((ARecordType) combinedType.getFieldTypes()[pos], (ARecordVisitablePointable) leftValue,
                        (ARecordVisitablePointable) rightValue, nestedLevel + 1);

                tabvs.reset();
                rbStack.get(nestedLevel + 1).write(tabvs.getDataOutput(), true);
                rbStack.get(nestedLevel).addField(pos, tabvs);
            }
        } else {
            if (rightValue == null) {
                rbStack.get(nestedLevel).addField(fieldNamePointable, leftValue);
            } else {
                mergeFields(DefaultOpenFieldType.NESTED_OPEN_RECORD_TYPE, (ARecordVisitablePointable) leftValue,
                        (ARecordVisitablePointable) rightValue, nestedLevel + 1);
                tabvs.reset();
                rbStack.get(nestedLevel + 1).write(tabvs.getDataOutput(), true);
                rbStack.get(nestedLevel).addField(fieldNamePointable, tabvs);
            }
        }
    }
}
