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

import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.runtime.evaluators.functions.PointableHelper;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluator;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.api.IPointable;
import org.apache.hyracks.dataflow.common.data.accessors.IFrameTupleReference;

abstract class AbstractRecordAddPutEvaluator extends AbstractRecordFunctionEvaluator {
    private final IScalarEvaluator eval0;
    private final IScalarEvaluator eval1;
    private final IScalarEvaluator eval2;
    protected boolean newFieldValueIsMissing = false;

    AbstractRecordAddPutEvaluator(IScalarEvaluator eval0, IScalarEvaluator eval1, IScalarEvaluator eval2,
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
        eval1.evaluate(tuple, newFieldNamePointable);
        eval2.evaluate(tuple, newFieldValuePointable);
        ATypeTag inputTypeTag = PointableHelper.getTypeTag(inputPointable);
        ATypeTag newFieldNameTypeTag = PointableHelper.getTypeTag(newFieldNamePointable);
        if (inputTypeTag == ATypeTag.MISSING || newFieldNameTypeTag == ATypeTag.MISSING) {
            PointableHelper.setMissing(result);
            return;
        }
        if (inputTypeTag != ATypeTag.OBJECT || newFieldNameTypeTag != ATypeTag.STRING) {
            PointableHelper.setNull(result);
            return;
        }
        newFieldValueIsMissing = PointableHelper.getTypeTag(newFieldValuePointable) == ATypeTag.MISSING;
        outputRecordTypeInfo.reset(outRecType);
        if (inputRecordPointable == null) {
            inputRecordPointable = pointableAllocator.allocateRecordValue(inRecType);
        }
        buildOutputRecord(result);
        result.set(resultStorage);
    }

    protected abstract void buildOutputRecord(IPointable result) throws HyracksDataException;

    protected void addField(IPointable fieldName, IPointable fieldValue) throws HyracksDataException {
        int pos = outputRecordTypeInfo.getFieldIndex(fieldName.getByteArray(), fieldName.getStartOffset() + 1,
                fieldName.getLength() - 1);
        if (pos >= 0) {
            outRecordBuilder.addField(pos, fieldValue);
        } else {
            outRecordBuilder.addField(fieldName, fieldValue);
        }
    }
}
