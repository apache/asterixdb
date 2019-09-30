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

package org.apache.asterix.runtime.evaluators.functions.bitwise;

import org.apache.asterix.om.exceptions.ExceptionUtil;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.hierachy.ATypeHierarchy;
import org.apache.asterix.runtime.evaluators.functions.AbstractScalarEval;
import org.apache.asterix.runtime.evaluators.functions.PointableHelper;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import org.apache.hyracks.algebricks.runtime.base.IEvaluatorContext;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluator;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluatorFactory;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.exceptions.SourceLocation;
import org.apache.hyracks.data.std.api.IPointable;
import org.apache.hyracks.data.std.primitive.VoidPointable;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.apache.hyracks.dataflow.common.data.accessors.IFrameTupleReference;

/**
 * This function receives one argument, representing the value. This evaluator is used by the Bitwise NOT and COUNT
 * functions.
 * This function can be applied only to int64 type numeric values.
 *
 * The function has the following behavior:
 * - If the argument is missing or null, then missing or null is returned, respectively.
 * - If the argument is int64, then int32 or int64 is returned, depending on the calling function.
 * - If the argument is float or double, then int32 or int64 or null is returned, depending on the argument value.
 * - If the argument is any other type, then null is returned.
 */

abstract class AbstractBitSingleValueEvaluator extends AbstractScalarEval {

    // Result members
    final ArrayBackedValueStorage resultStorage = new ArrayBackedValueStorage();

    // Evaluators and Pointables
    private final IScalarEvaluator valueEvaluator;
    private final IPointable valuePointable = VoidPointable.FACTORY.createPointable();

    private final IEvaluatorContext context;

    AbstractBitSingleValueEvaluator(IEvaluatorContext context, IScalarEvaluatorFactory[] argEvaluatorFactories,
            FunctionIdentifier functionIdentifier, SourceLocation sourceLocation) throws HyracksDataException {
        super(sourceLocation, functionIdentifier);

        this.context = context;

        // Evaluator
        valueEvaluator = argEvaluatorFactories[0].createScalarEvaluator(context);
    }

    // Abstract methods
    abstract void applyBitwiseOperation(long value);

    abstract void writeResult(IPointable result) throws HyracksDataException;

    @Override
    public void evaluate(IFrameTupleReference tuple, IPointable result) throws HyracksDataException {
        valueEvaluator.evaluate(tuple, valuePointable);
        if (PointableHelper.checkAndSetMissingOrNull(result, valuePointable)) {
            return;
        }

        byte[] bytes = valuePointable.getByteArray();
        int startOffset = valuePointable.getStartOffset();

        // Validity check
        if (!PointableHelper.isValidLongValue(bytes, startOffset, true)) {
            ExceptionUtil.warnTypeMismatch(context, sourceLoc, functionIdentifier, bytes[startOffset], 0,
                    ATypeTag.BIGINT);
            PointableHelper.setNull(result);
            return;
        }

        long longValue = ATypeHierarchy.getLongValue(functionIdentifier.getName(), 0, bytes, startOffset);
        applyBitwiseOperation(longValue);

        writeResult(result);
    }
}
