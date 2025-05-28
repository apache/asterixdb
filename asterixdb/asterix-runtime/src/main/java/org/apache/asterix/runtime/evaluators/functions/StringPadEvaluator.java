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
import org.apache.hyracks.util.string.UTF8StringUtil;

public abstract class StringPadEvaluator extends AbstractScalarEval {

    private final IEvaluatorContext ctx;
    private final IScalarEvaluator inStrEval;
    private final IScalarEvaluator lenEval;
    private final IScalarEvaluator padStrEval;

    private final UTF8StringPointable inStrUtf8Ptr = new UTF8StringPointable();
    private final UTF8StringPointable padStrUtf8Ptr = new UTF8StringPointable();
    private final IPointable inStrArg = new VoidPointable();
    private final IPointable lenArg = new VoidPointable();
    private final IPointable padStrArg = new VoidPointable();
    private final AMutableInt32 mutableInt = new AMutableInt32(0);

    private final ArrayBackedValueStorage resultStorage = new ArrayBackedValueStorage();
    private final DataOutput out = resultStorage.getDataOutput();
    private final GrowableArray builderInternalArray = new GrowableArray();
    private final UTF8StringBuilder builder = new UTF8StringBuilder();
    private final boolean lpad;

    public StringPadEvaluator(IEvaluatorContext ctx, IScalarEvaluatorFactory inStrEvalFact,
            IScalarEvaluatorFactory lenEvalFact, IScalarEvaluatorFactory padStrEvalFact, FunctionIdentifier funcID,
            SourceLocation srcLoc, boolean lpad) throws HyracksDataException {
        super(srcLoc, funcID);
        this.lpad = lpad;
        this.inStrEval = inStrEvalFact.createScalarEvaluator(ctx);
        this.lenEval = lenEvalFact.createScalarEvaluator(ctx);
        this.padStrEval = padStrEvalFact.createScalarEvaluator(ctx);
        this.ctx = ctx;
    }

    @Override
    public void evaluate(IFrameTupleReference tuple, IPointable result) throws HyracksDataException {
        inStrEval.evaluate(tuple, inStrArg);
        lenEval.evaluate(tuple, lenArg);
        padStrEval.evaluate(tuple, padStrArg);

        if (PointableHelper.checkAndSetMissingOrNull(result, inStrArg, lenArg, padStrArg)) {
            return;
        }

        byte[] inStrBytes = inStrArg.getByteArray();
        int inStrStart = inStrArg.getStartOffset();
        if (inStrBytes[inStrStart] != ATypeTag.SERIALIZED_STRING_TYPE_TAG) {
            PointableHelper.setNull(result);
            ExceptionUtil.warnTypeMismatch(ctx, srcLoc, funID, inStrBytes[inStrStart], 0, ATypeTag.STRING);
            return;
        }

        byte[] lenBytes = lenArg.getByteArray();
        int lenStart = lenArg.getStartOffset();
        if (!ArgumentUtils.setInteger(ctx, srcLoc, funID, 1, lenBytes, lenStart, mutableInt)) {
            PointableHelper.setNull(result);
            return;
        }
        // Desired length of the string in Unicode code points
        int targetNumCodePoints = mutableInt.getIntegerValue();
        if (targetNumCodePoints < 0) {
            PointableHelper.setNull(result);
            ExceptionUtil.warnNegativeValue(ctx, srcLoc, funID, 1, targetNumCodePoints);
            return;
        }

        // TODO: usually this will be a constant. if constant, the code could be optimized
        byte[] padStrBytes = padStrArg.getByteArray();
        int padStrStart = padStrArg.getStartOffset();
        if (padStrBytes[padStrStart] != ATypeTag.SERIALIZED_STRING_TYPE_TAG) {
            PointableHelper.setNull(result);
            ExceptionUtil.warnTypeMismatch(ctx, srcLoc, funID, padStrBytes[padStrStart], 2, ATypeTag.STRING);
            return;
        }

        int inStrCodePointsNum = UTF8StringUtil.getNumCodePoint(inStrBytes, inStrStart + 1);
        if (inStrCodePointsNum == targetNumCodePoints) {
            result.set(inStrArg);
            return;
        }
        try {
            builderInternalArray.reset();
            builder.reset(builderInternalArray, targetNumCodePoints);
            // skip the type tag byte
            inStrUtf8Ptr.set(inStrBytes, inStrStart + 1, inStrArg.getLength() - 1);
            padStrUtf8Ptr.set(padStrBytes, padStrStart + 1, padStrArg.getLength() - 1);

            process(targetNumCodePoints, inStrCodePointsNum);
            builder.finish();

            resultStorage.reset();
            out.writeByte(ATypeTag.SERIALIZED_STRING_TYPE_TAG);
            out.write(builderInternalArray.getByteArray(), 0, builderInternalArray.getLength());
            //result -> type tag, len, actual data
            result.set(resultStorage);
        } catch (IOException e) {
            throw HyracksDataException.create(e);
        }
    }

    public void process(int targetNumCodePoints, int inStrCodePointsNum) throws IOException {
        if (targetNumCodePoints < inStrCodePointsNum) {
            truncate(targetNumCodePoints);
        } else {
            int numCodePointsToPad = targetNumCodePoints - inStrCodePointsNum;
            if (lpad) {
                // For LPAD: first, append the required padding characters.
                appendPaddingCodepoints(numCodePointsToPad);
                // Then, append the codepoints of the original string.
                appendCodePoints();
            } else {
                appendCodePoints();
                appendPaddingCodepoints(numCodePointsToPad);
            }
        }
    }

    private void truncate(int targetNumCodePoints) throws IOException {
        UTF8StringPointable.append(inStrUtf8Ptr, targetNumCodePoints, builder, builderInternalArray);
    }

    private void appendCodePoints() throws IOException {
        builder.appendUtf8StringPointable(inStrUtf8Ptr);
    }

    private void appendPaddingCodepoints(int numCodePointsToPad) throws IOException {
        UTF8StringPointable.append(padStrUtf8Ptr, numCodePointsToPad, builder, builderInternalArray);
    }
}
