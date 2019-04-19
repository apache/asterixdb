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

package org.apache.asterix.runtime.evaluators.functions;

import org.apache.asterix.om.pointables.PointableAllocator;
import org.apache.asterix.om.pointables.base.DefaultOpenFieldType;
import org.apache.asterix.om.pointables.base.IVisitablePointable;
import org.apache.asterix.om.pointables.cast.ACastVisitor;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.asterix.om.types.IAType;
import org.apache.hyracks.algebricks.common.utils.Triple;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluator;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.api.IPointable;
import org.apache.hyracks.data.std.primitive.VoidPointable;
import org.apache.hyracks.dataflow.common.data.accessors.IFrameTupleReference;

public class CastTypeEvaluator implements IScalarEvaluator {

    private IScalarEvaluator argEvaluator;
    private final IPointable argPointable = new VoidPointable();
    private final PointableAllocator allocator = new PointableAllocator();
    private IVisitablePointable inputPointable;
    private IVisitablePointable resultPointable;
    private final ACastVisitor castVisitor = createCastVisitor();
    private final Triple<IVisitablePointable, IAType, Boolean> arg = new Triple<>(null, null, null);

    public CastTypeEvaluator() {
        // reset() should be called after using this constructor before calling any method
    }

    public CastTypeEvaluator(IAType reqType, IAType inputType, IScalarEvaluator argEvaluator) {
        resetAndAllocate(reqType, inputType, argEvaluator);
    }

    public void resetAndAllocate(IAType reqType, IAType inputType, IScalarEvaluator argEvaluator) {
        this.argEvaluator = argEvaluator;
        this.inputPointable = allocatePointable(inputType, reqType);
        this.resultPointable = allocatePointable(reqType, inputType);
        this.arg.first = resultPointable;
        this.arg.second = reqType;
        this.arg.third = Boolean.FALSE;
    }

    protected ACastVisitor createCastVisitor() {
        return new ACastVisitor();
    }

    @Override
    public void evaluate(IFrameTupleReference tuple, IPointable result) throws HyracksDataException {
        argEvaluator.evaluate(tuple, argPointable);

        if (PointableHelper.checkAndSetMissingOrNull(result, argPointable)) {
            return;
        }

        inputPointable.set(argPointable);
        cast(result);
    }

    protected void cast(IPointable result) throws HyracksDataException {
        inputPointable.accept(castVisitor, arg);
        result.set(resultPointable);
    }

    // TODO: refactor in a better way
    protected void cast(IPointable argPointable, IPointable result) throws HyracksDataException {
        inputPointable.set(argPointable);
        cast(result);
    }

    // Allocates the result pointable.
    private IVisitablePointable allocatePointable(IAType typeForPointable, IAType typeForOtherSide) {
        if (!typeForPointable.equals(BuiltinType.ANY)) {
            return allocator.allocateFieldValue(typeForPointable);
        }
        return allocatePointableForAny(typeForOtherSide);
    }

    // Allocates an input or result pointable if the input or required type is ANY.
    private IVisitablePointable allocatePointableForAny(IAType type) {
        ATypeTag tag = type.getTypeTag();
        switch (tag) {
            case OBJECT:
                return allocator.allocateFieldValue(DefaultOpenFieldType.NESTED_OPEN_RECORD_TYPE);
            case ARRAY:
                return allocator.allocateFieldValue(DefaultOpenFieldType.NESTED_OPEN_AORDERED_LIST_TYPE);
            case MULTISET:
                return allocator.allocateFieldValue(DefaultOpenFieldType.NESTED_OPEN_AUNORDERED_LIST_TYPE);
            default:
                return allocator.allocateFieldValue(null);
        }
    }

    public void deallocatePointables() {
        allocator.reset();
    }
}
