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

import org.apache.asterix.builders.RecordBuilder;
import org.apache.asterix.om.pointables.ARecordVisitablePointable;
import org.apache.asterix.om.pointables.base.DefaultOpenFieldType;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.asterix.om.types.IAType;
import org.apache.asterix.runtime.evaluators.functions.CastTypeEvaluator;
import org.apache.asterix.runtime.evaluators.functions.PointableHelper;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluator;
import org.apache.hyracks.api.dataflow.value.IBinaryComparator;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.accessors.UTF8StringBinaryComparatorFactory;
import org.apache.hyracks.data.std.api.IPointable;
import org.apache.hyracks.data.std.primitive.UTF8StringPointable;
import org.apache.hyracks.data.std.primitive.VoidPointable;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.apache.hyracks.dataflow.common.data.accessors.IFrameTupleReference;

abstract class AbstractRecordAddPutEvaluator implements IScalarEvaluator {

    private final CastTypeEvaluator inputRecordCaster;
    private final CastTypeEvaluator argRecordCaster;
    private final IScalarEvaluator eval0;
    private final IScalarEvaluator eval1;
    private final IScalarEvaluator eval2;
    final ArrayBackedValueStorage resultStorage = new ArrayBackedValueStorage();
    final DataOutput resultOutput = resultStorage.getDataOutput();
    final IPointable inputRecordPointable = new VoidPointable();
    final UTF8StringPointable newFieldNamePointable = new UTF8StringPointable();
    final IPointable newFieldValuePointable = new VoidPointable();
    final IBinaryComparator stringBinaryComparator =
            UTF8StringBinaryComparatorFactory.INSTANCE.createBinaryComparator();
    final RecordBuilder outRecordBuilder = new RecordBuilder();
    final ARecordVisitablePointable inputOpenRecordPointable;
    boolean newFieldValueIsMissing = false;

    AbstractRecordAddPutEvaluator(IScalarEvaluator eval0, IScalarEvaluator eval1, IScalarEvaluator eval2,
            IAType[] argTypes) {
        this.eval0 = eval0;
        this.eval1 = eval1;
        this.eval2 = eval2;
        inputOpenRecordPointable = new ARecordVisitablePointable(DefaultOpenFieldType.NESTED_OPEN_RECORD_TYPE);
        inputRecordCaster = new CastTypeEvaluator(BuiltinType.ANY, argTypes[0], eval0);
        argRecordCaster = new CastTypeEvaluator(BuiltinType.ANY, argTypes[2], eval2);
    }

    @Override
    public void evaluate(IFrameTupleReference tuple, IPointable result) throws HyracksDataException {
        resultStorage.reset();
        eval0.evaluate(tuple, inputRecordPointable);
        eval1.evaluate(tuple, newFieldNamePointable);
        eval2.evaluate(tuple, newFieldValuePointable);
        if (containsMissing(inputRecordPointable, newFieldNamePointable)) {
            writeTypeTag(ATypeTag.SERIALIZED_MISSING_TYPE_TAG);
            result.set(resultStorage);
            return;
        }
        final ATypeTag inputObjectType = PointableHelper.getTypeTag(inputRecordPointable);
        final ATypeTag newFieldNameValueType = PointableHelper.getTypeTag(newFieldNamePointable);
        if (inputObjectType != ATypeTag.OBJECT || newFieldNameValueType != ATypeTag.STRING) {
            PointableHelper.setNull(result);
            return;
        }
        inputRecordCaster.evaluate(tuple, inputRecordPointable);
        final ATypeTag newFieldValueTag = PointableHelper.getTypeTag(newFieldValuePointable);
        if (newFieldValueTag.isDerivedType()) {
            argRecordCaster.evaluate(tuple, newFieldValuePointable);
        }
        newFieldValueIsMissing = newFieldValueTag == ATypeTag.MISSING;
        buildOutputRecord();
        result.set(resultStorage);
    }

    protected abstract void buildOutputRecord() throws HyracksDataException;

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
}
