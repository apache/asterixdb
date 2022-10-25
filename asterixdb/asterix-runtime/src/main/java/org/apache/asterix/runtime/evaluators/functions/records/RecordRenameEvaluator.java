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

import java.io.IOException;
import java.util.List;

import org.apache.asterix.om.pointables.base.IVisitablePointable;
import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.runtime.evaluators.functions.PointableHelper;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluator;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.api.IPointable;
import org.apache.hyracks.data.std.primitive.VoidPointable;
import org.apache.hyracks.dataflow.common.data.accessors.IFrameTupleReference;

class RecordRenameEvaluator extends AbstractRecordFunctionEvaluator {
    private final IScalarEvaluator eval0;
    private final IScalarEvaluator eval1;
    private final IScalarEvaluator eval2;
    private final IPointable oldFieldNamePointable = new VoidPointable();

    RecordRenameEvaluator(IScalarEvaluator eval0, IScalarEvaluator eval1, IScalarEvaluator eval2,
            ARecordType outRecType, ARecordType inRecType) {
        super(outRecType, inRecType);
        this.eval0 = eval0;
        this.eval1 = eval1;
        this.eval2 = eval2;
    }

    @Override
    public void evaluate(IFrameTupleReference tuple, IPointable result) throws HyracksDataException {
        resultStorage.reset();
        eval0.evaluate(tuple, inputPointable);
        eval1.evaluate(tuple, oldFieldNamePointable);
        eval2.evaluate(tuple, newFieldNamePointable);
        if (PointableHelper.checkAndSetMissingOrNull(result, inputPointable, oldFieldNamePointable,
                newFieldNamePointable)) {
            return;
        }

        // Check the type of our first argument.
        byte[] data = inputPointable.getByteArray();
        int offset = inputPointable.getStartOffset();
        byte typeTag = data[offset];
        if (typeTag != ATypeTag.SERIALIZED_RECORD_TYPE_TAG) {
            PointableHelper.setNull(result);
            return;
        }

        // Check the type of our second argument.
        data = oldFieldNamePointable.getByteArray();
        offset = oldFieldNamePointable.getStartOffset();
        typeTag = data[offset];
        if (typeTag != ATypeTag.SERIALIZED_STRING_TYPE_TAG) {
            PointableHelper.setNull(result);
            return;
        }

        // Check the type of our third argument.
        data = newFieldNamePointable.getByteArray();
        offset = newFieldNamePointable.getStartOffset();
        typeTag = data[offset];
        if (typeTag != ATypeTag.SERIALIZED_STRING_TYPE_TAG) {
            PointableHelper.setNull(result);
            return;
        }

        try {
            outRecordBuilder.reset(outRecType);
            outputRecordTypeInfo.reset(outRecType);
            if (inputRecordPointable == null) {
                inputRecordPointable = pointableAllocator.allocateRecordValue(inRecType);
            }
            inputRecordPointable.set(inputPointable);
            final List<IVisitablePointable> fieldNames = inputRecordPointable.getFieldNames();
            final List<IVisitablePointable> fieldValues = inputRecordPointable.getFieldValues();
            for (int i = 0, fieldCount = fieldNames.size(); i < fieldCount; i++) {
                final IVisitablePointable fieldName = fieldNames.get(i);
                final IVisitablePointable fieldValue = fieldValues.get(i);
                if (!PointableHelper.isEqual(fieldName, oldFieldNamePointable, stringBinaryComparator)) {
                    addField(fieldName, fieldValue);
                } else {
                    addField(newFieldNamePointable, fieldValue);
                }
            }
            outRecordBuilder.write(resultOutput, true);
        } catch (IOException e) {
            throw HyracksDataException.create(e);
        }
        result.set(resultStorage);
    }

    private void addField(IPointable fieldName, IPointable fieldValue) throws HyracksDataException {
        int pos = outputRecordTypeInfo.getFieldIndex(fieldName.getByteArray(), fieldName.getStartOffset() + 1,
                fieldName.getLength() - 1);
        if (pos >= 0) {
            outRecordBuilder.addField(pos, fieldValue);
        } else {
            outRecordBuilder.addField(fieldName, fieldValue);
        }
    }
}
