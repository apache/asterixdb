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
import java.util.List;

import org.apache.asterix.builders.RecordBuilder;
import org.apache.asterix.om.functions.BuiltinFunctions;
import org.apache.asterix.om.pointables.ARecordVisitablePointable;
import org.apache.asterix.om.pointables.base.DefaultOpenFieldType;
import org.apache.asterix.om.pointables.base.IVisitablePointable;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.asterix.om.types.IAType;
import org.apache.asterix.om.types.hierachy.ATypeHierarchy;
import org.apache.asterix.runtime.evaluators.comparisons.ComparisonHelper;
import org.apache.asterix.runtime.evaluators.functions.CastTypeEvaluator;
import org.apache.asterix.runtime.evaluators.functions.PointableHelper;
import org.apache.asterix.runtime.exceptions.TypeMismatchException;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluator;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.exceptions.SourceLocation;
import org.apache.hyracks.data.std.api.IPointable;
import org.apache.hyracks.data.std.primitive.VoidPointable;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.apache.hyracks.dataflow.common.data.accessors.IFrameTupleReference;

class RecordReplaceEvaluator implements IScalarEvaluator {

    private final IPointable inputRecordPointable = new VoidPointable();
    private final IPointable oldValuePointable = new VoidPointable();
    private final IPointable newValuePointable = new VoidPointable();
    private final ArrayBackedValueStorage resultStorage = new ArrayBackedValueStorage();
    private final DataOutput resultOutput = resultStorage.getDataOutput();
    private final RecordBuilder outRecordBuilder = new RecordBuilder();
    private final VoidPointable existingValuePtr = new VoidPointable();
    private final VoidPointable oldValuePtr = new VoidPointable();
    private final IScalarEvaluator eval0;
    private final IScalarEvaluator eval1;
    private final IScalarEvaluator eval2;
    private final ARecordVisitablePointable openRecordPointable;
    private final CastTypeEvaluator inputRecordCaster;
    private final CastTypeEvaluator newValueRecordCaster;
    private final SourceLocation sourceLoc;
    private final ComparisonHelper comparisonHelper;

    RecordReplaceEvaluator(SourceLocation sourceLoc, IScalarEvaluator eval0, IScalarEvaluator eval1,
            IScalarEvaluator eval2, IAType[] argTypes) {
        this.sourceLoc = sourceLoc;
        this.eval0 = eval0;
        this.eval1 = eval1;
        this.eval2 = eval2;
        openRecordPointable = new ARecordVisitablePointable(DefaultOpenFieldType.NESTED_OPEN_RECORD_TYPE);
        inputRecordCaster = new CastTypeEvaluator(BuiltinType.ANY, argTypes[0], eval0);
        newValueRecordCaster = new CastTypeEvaluator(BuiltinType.ANY, argTypes[2], eval2);
        comparisonHelper = new ComparisonHelper(sourceLoc);
    }

    @Override
    public void evaluate(IFrameTupleReference tuple, IPointable result) throws HyracksDataException {
        resultStorage.reset();
        eval0.evaluate(tuple, inputRecordPointable);
        eval1.evaluate(tuple, oldValuePointable);
        eval2.evaluate(tuple, newValuePointable);
        if (containsMissing(inputRecordPointable, oldValuePointable, newValuePointable)) {
            writeTypeTag(ATypeTag.SERIALIZED_MISSING_TYPE_TAG);
            result.set(resultStorage);
            return;
        }
        final ATypeTag inputObjectType = PointableHelper.getTypeTag(inputRecordPointable);
        final ATypeTag oldValueType = PointableHelper.getTypeTag(oldValuePointable);
        if (inputObjectType != ATypeTag.OBJECT || oldValueType == ATypeTag.NULL) {
            writeTypeTag(ATypeTag.SERIALIZED_NULL_TYPE_TAG);
            result.set(resultStorage);
            return;
        }
        if (oldValueType.isDerivedType()) {
            throw new TypeMismatchException(sourceLoc, BuiltinFunctions.RECORD_REPLACE, 1, oldValueType.serialize(),
                    "primitive");
        }
        inputRecordCaster.evaluate(tuple, inputRecordPointable);
        final ATypeTag newValueType = PointableHelper.getTypeTag(newValuePointable);
        if (newValueType.isDerivedType()) {
            newValueRecordCaster.evaluate(tuple, newValuePointable);
        }
        resultStorage.reset();
        buildOutputRecord(oldValueType);
        result.set(resultStorage);
    }

    private void buildOutputRecord(ATypeTag oldValueTypeTag) throws HyracksDataException {
        openRecordPointable.set(inputRecordPointable);
        outRecordBuilder.reset(DefaultOpenFieldType.NESTED_OPEN_RECORD_TYPE);
        outRecordBuilder.init();
        final List<IVisitablePointable> fieldNames = openRecordPointable.getFieldNames();
        final List<IVisitablePointable> fieldValues = openRecordPointable.getFieldValues();
        for (int i = 0, fieldCount = fieldNames.size(); i < fieldCount; i++) {
            final IVisitablePointable fieldName = fieldNames.get(i);
            final IVisitablePointable fieldValue = fieldValues.get(i);
            final ATypeTag existingValueTypeTag = PointableHelper.getTypeTag(fieldValue);
            if (isEqual(existingValueTypeTag, fieldValue, oldValueTypeTag, oldValuePointable)) {
                outRecordBuilder.addField(fieldName, newValuePointable);
            } else {
                outRecordBuilder.addField(fieldName, fieldValue);
            }
        }
        outRecordBuilder.write(resultOutput, true);
    }

    private boolean containsMissing(IPointable... pointables) {
        for (int i = 0; i < pointables.length; i++) {
            if (PointableHelper.getTypeTag(pointables[i]) == ATypeTag.MISSING) {
                return true;
            }
        }
        return false;
    }

    private void writeTypeTag(byte typeTag) throws HyracksDataException {
        try {
            resultOutput.writeByte(typeTag);
        } catch (IOException e) {
            throw HyracksDataException.create(e);
        }
    }

    private boolean isEqual(ATypeTag typeTag1, IPointable value1, ATypeTag typeTag2, IPointable value2)
            throws HyracksDataException {
        if (!ATypeHierarchy.isCompatible(typeTag1, typeTag2)) {
            return false;
        }
        setValuePointer(value1, existingValuePtr);
        setValuePointer(value2, oldValuePtr);
        return comparisonHelper.compare(typeTag1, typeTag2, existingValuePtr, oldValuePtr) == 0;
    }

    private static void setValuePointer(IPointable src, IPointable value) {
        value.set(src.getByteArray(), src.getStartOffset() + 1, src.getLength() - 1);
    }
}
