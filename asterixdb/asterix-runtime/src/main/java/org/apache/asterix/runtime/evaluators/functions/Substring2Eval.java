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

import java.io.DataOutput;
import java.io.IOException;

import org.apache.asterix.common.annotations.MissingNullInOutFunction;
import org.apache.asterix.om.base.AMutableInt32;
import org.apache.asterix.om.exceptions.ExceptionUtil;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.runtime.evaluators.common.ArgumentUtils;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluator;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluatorFactory;
import org.apache.hyracks.api.context.IEvaluatorContext;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.exceptions.SourceLocation;
import org.apache.hyracks.data.std.api.IPointable;
import org.apache.hyracks.data.std.primitive.UTF8StringPointable;
import org.apache.hyracks.data.std.primitive.VoidPointable;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.apache.hyracks.data.std.util.GrowableArray;
import org.apache.hyracks.data.std.util.UTF8StringBuilder;
import org.apache.hyracks.dataflow.common.data.accessors.IFrameTupleReference;

@MissingNullInOutFunction
class Substring2Eval extends AbstractScalarEval {

    private final IEvaluatorContext context;

    private final ArrayBackedValueStorage resultStorage = new ArrayBackedValueStorage();
    private final DataOutput out = resultStorage.getDataOutput();
    private final IPointable argString = new VoidPointable();
    private final IPointable argStart = new VoidPointable();
    private final IScalarEvaluator evalString;
    private final IScalarEvaluator evalStart;
    private final GrowableArray array = new GrowableArray();
    private final UTF8StringBuilder builder = new UTF8StringBuilder();
    private final UTF8StringPointable string = new UTF8StringPointable();
    private final AMutableInt32 mutableInt = new AMutableInt32(0);

    private final int baseOffset;

    Substring2Eval(IEvaluatorContext context, IScalarEvaluatorFactory[] args, FunctionIdentifier functionIdentifier,
            SourceLocation sourceLoc, int baseOffset) throws HyracksDataException {
        super(sourceLoc, functionIdentifier);
        this.context = context;
        this.baseOffset = baseOffset;
        evalString = args[0].createScalarEvaluator(context);
        evalStart = args[1].createScalarEvaluator(context);
    }

    @Override
    public void evaluate(IFrameTupleReference tuple, IPointable result) throws HyracksDataException {
        resultStorage.reset();
        evalString.evaluate(tuple, argString);
        evalStart.evaluate(tuple, argStart);

        if (PointableHelper.checkAndSetMissingOrNull(result, argString, argStart)) {
            return;
        }

        byte[] bytes = argStart.getByteArray();
        int offset = argStart.getStartOffset();
        // check that the int argument is numeric without fractions (in case arg is double or float)
        if (!ArgumentUtils.setInteger(context, srcLoc, funID, 1, bytes, offset, mutableInt)) {
            PointableHelper.setNull(result);
            return;
        }
        int start = mutableInt.getIntegerValue();
        bytes = argString.getByteArray();
        offset = argString.getStartOffset();
        int len = argString.getLength();
        if (bytes[offset] != ATypeTag.SERIALIZED_STRING_TYPE_TAG) {
            PointableHelper.setNull(result);
            ExceptionUtil.warnTypeMismatch(context, srcLoc, funID, bytes[offset], 0, ATypeTag.STRING);
            return;
        }
        string.set(bytes, offset + 1, len - 1);
        array.reset();
        try {
            int actualStart = start >= 0 ? Math.max(start - baseOffset, 0) : string.getStringLength() + start;
            boolean success = UTF8StringPointable.substr(string, actualStart, Integer.MAX_VALUE, builder, array);
            if (success) {
                out.writeByte(ATypeTag.SERIALIZED_STRING_TYPE_TAG);
                out.write(array.getByteArray(), 0, array.getLength());
                result.set(resultStorage);
            } else {
                PointableHelper.setNull(result);
            }
        } catch (IOException e) {
            throw HyracksDataException.create(e);
        }
    }
}
