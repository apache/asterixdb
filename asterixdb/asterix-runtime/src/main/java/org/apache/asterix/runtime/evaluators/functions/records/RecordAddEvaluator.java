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

import java.util.List;

import org.apache.asterix.om.pointables.base.DefaultOpenFieldType;
import org.apache.asterix.om.pointables.base.IVisitablePointable;
import org.apache.asterix.om.types.IAType;
import org.apache.asterix.runtime.evaluators.functions.PointableHelper;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluator;
import org.apache.hyracks.api.exceptions.HyracksDataException;

class RecordAddEvaluator extends AbstractRecordAddPutEvaluator {

    RecordAddEvaluator(IScalarEvaluator eval0, IScalarEvaluator eval1, IScalarEvaluator eval2, IAType[] argTypes) {
        super(eval0, eval1, eval2, argTypes);
    }

    @Override
    protected void buildOutputRecord() throws HyracksDataException {
        resultStorage.reset();
        outRecordBuilder.reset(DefaultOpenFieldType.NESTED_OPEN_RECORD_TYPE);
        outRecordBuilder.init();
        inputOpenRecordPointable.set(inputRecordPointable);
        final List<IVisitablePointable> fieldNames = inputOpenRecordPointable.getFieldNames();
        final List<IVisitablePointable> fieldValues = inputOpenRecordPointable.getFieldValues();
        boolean newFieldFound = false;
        for (int i = 0, fieldCount = fieldNames.size(); i < fieldCount; i++) {
            final IVisitablePointable fieldName = fieldNames.get(i);
            if (PointableHelper.isEqual(fieldName, newFieldNamePointable, stringBinaryComparator)) {
                newFieldFound = true;
            }
            outRecordBuilder.addField(fieldName, fieldValues.get(i));
        }
        if (!newFieldValueIsMissing && !newFieldFound) {
            outRecordBuilder.addField(newFieldNamePointable, newFieldValuePointable);
        }
        outRecordBuilder.write(resultOutput, true);
    }
}
