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

import org.apache.asterix.om.pointables.ARecordVisitablePointable;
import org.apache.asterix.om.pointables.base.DefaultOpenFieldType;
import org.apache.asterix.om.pointables.base.IVisitablePointable;
import org.apache.asterix.om.pointables.cast.ACastVisitor;
import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.IAType;
import org.apache.asterix.runtime.evaluators.functions.PointableHelper;
import org.apache.hyracks.algebricks.common.utils.Triple;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluator;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.api.IPointable;
import org.apache.hyracks.data.std.primitive.VoidPointable;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.apache.hyracks.dataflow.common.data.accessors.IFrameTupleReference;

class RecordUnwrapEvaluator implements IScalarEvaluator {

    private final IPointable inputRecordPointable = new VoidPointable();
    private final IScalarEvaluator eval0;
    private ARecordVisitablePointable inputRecordVisitable;
    private ARecordVisitablePointable openRecordVisitablePointable;
    private boolean requiresCast = false;
    private ACastVisitor castVisitor;
    private Triple<IVisitablePointable, IAType, Boolean> castVisitorArg;
    private final ArrayBackedValueStorage resultStorage = new ArrayBackedValueStorage();
    private final DataOutput resultOutput = resultStorage.getDataOutput();

    RecordUnwrapEvaluator(IScalarEvaluator eval0, ARecordType recordType) {
        this.eval0 = eval0;
        openRecordVisitablePointable = new ARecordVisitablePointable(DefaultOpenFieldType.NESTED_OPEN_RECORD_TYPE);
        if (recordType != null) {
            inputRecordVisitable = new ARecordVisitablePointable(recordType);
            if (hasDerivedType(recordType.getFieldTypes())) {
                requiresCast = true;
                castVisitor = new ACastVisitor();
                castVisitorArg = new Triple<>(openRecordVisitablePointable,
                        openRecordVisitablePointable.getInputRecordType(), Boolean.FALSE);
            }
        }
    }

    @Override
    public void evaluate(IFrameTupleReference tuple, IPointable result) throws HyracksDataException {
        resultStorage.reset();
        eval0.evaluate(tuple, inputRecordPointable);

        if (PointableHelper.checkAndSetMissingOrNull(result, inputRecordPointable)) {
            return;
        }

        final byte[] data = inputRecordPointable.getByteArray();
        final int offset = inputRecordPointable.getStartOffset();
        final byte typeTag = data[offset];
        if (typeTag != ATypeTag.SERIALIZED_RECORD_TYPE_TAG) {
            PointableHelper.setNull(result);
            return;
        }
        final ARecordVisitablePointable inputRecordVisitablePointable = getInputRecordVisitablePointable();
        final List<IVisitablePointable> recValues = inputRecordVisitablePointable.getFieldValues();
        if (recValues.size() != 1) {
            PointableHelper.setNull(result);
            return;
        }
        writeValue(recValues.get(0));
        result.set(resultStorage);
    }

    private boolean hasDerivedType(IAType[] types) {
        for (IAType type : types) {
            if (type.getTypeTag().isDerivedType()) {
                return true;
            }
        }
        return false;
    }

    private ARecordVisitablePointable getInputRecordVisitablePointable() throws HyracksDataException {
        inputRecordVisitable.set(inputRecordPointable);
        if (requiresCast) {
            return castToOpenRecord();
        }
        return inputRecordVisitable;
    }

    private ARecordVisitablePointable castToOpenRecord() throws HyracksDataException {
        inputRecordVisitable.accept(castVisitor, castVisitorArg);
        return openRecordVisitablePointable;
    }

    private void writeValue(IVisitablePointable value) throws HyracksDataException {
        try {
            resultOutput.write(value.getByteArray(), value.getStartOffset(), value.getLength());
        } catch (IOException e) {
            throw HyracksDataException.create(e);
        }
    }
}
