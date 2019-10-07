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

import org.apache.asterix.om.base.AMutableInt32;
import org.apache.asterix.om.exceptions.ExceptionUtil;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.runtime.evaluators.common.ArgumentUtils;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import org.apache.hyracks.algebricks.runtime.base.IEvaluatorContext;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluator;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluatorFactory;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.exceptions.SourceLocation;
import org.apache.hyracks.data.std.api.IPointable;
import org.apache.hyracks.data.std.primitive.UTF8StringPointable;
import org.apache.hyracks.data.std.primitive.VoidPointable;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.apache.hyracks.dataflow.common.data.accessors.IFrameTupleReference;

public abstract class AbstractStringStringStringIntEval implements IScalarEvaluator {

    private final IEvaluatorContext ctx;
    // Argument evaluators.
    private final IScalarEvaluator eval0;
    private final IScalarEvaluator eval1;
    private final IScalarEvaluator eval2;
    private final IScalarEvaluator eval3;

    // Argument pointables.
    final IPointable argPtrFirst = new VoidPointable();
    final IPointable argPtrSecond = new VoidPointable();
    final IPointable argPtrThird = new VoidPointable();
    final IPointable argPtrFourth = new VoidPointable();
    private final UTF8StringPointable strPtr1st = new UTF8StringPointable();
    private final UTF8StringPointable strPtr2nd = new UTF8StringPointable();
    private final UTF8StringPointable strPtr3rd = new UTF8StringPointable();
    private final AMutableInt32 mutableInt = new AMutableInt32(0);

    // For outputting results.
    ArrayBackedValueStorage resultStorage = new ArrayBackedValueStorage();
    DataOutput dout = resultStorage.getDataOutput();

    // Function ID, for error reporting.
    protected final FunctionIdentifier funcID;
    protected final SourceLocation sourceLoc;

    AbstractStringStringStringIntEval(IEvaluatorContext context, IScalarEvaluatorFactory eval0,
            IScalarEvaluatorFactory eval1, IScalarEvaluatorFactory eval2, IScalarEvaluatorFactory eval3,
            FunctionIdentifier funcID, SourceLocation sourceLoc) throws HyracksDataException {
        this.ctx = context;
        this.sourceLoc = sourceLoc;
        this.eval0 = eval0.createScalarEvaluator(context);
        this.eval1 = eval1.createScalarEvaluator(context);
        this.eval2 = eval2.createScalarEvaluator(context);
        this.eval3 = eval3.createScalarEvaluator(context);
        this.funcID = funcID;
    }

    @SuppressWarnings("unchecked")
    @Override
    public void evaluate(IFrameTupleReference tuple, IPointable result) throws HyracksDataException {
        // Gets the arguments
        eval0.evaluate(tuple, argPtrFirst);
        eval1.evaluate(tuple, argPtrSecond);
        eval2.evaluate(tuple, argPtrThird);
        eval3.evaluate(tuple, argPtrFourth);

        if (PointableHelper.checkAndSetMissingOrNull(result, argPtrFirst, argPtrSecond, argPtrThird, argPtrFourth)) {
            return;
        }

        byte[] bytes0 = argPtrFirst.getByteArray();
        int start0 = argPtrFirst.getStartOffset();
        int len0 = argPtrFirst.getLength();

        byte[] bytes1 = argPtrSecond.getByteArray();
        int start1 = argPtrSecond.getStartOffset();
        int len1 = argPtrSecond.getLength();

        byte[] bytes2 = argPtrThird.getByteArray();
        int start2 = argPtrThird.getStartOffset();
        int len2 = argPtrThird.getLength();

        byte[] bytes3 = argPtrFourth.getByteArray();
        int start3 = argPtrFourth.getStartOffset();

        // Type check.
        if (!checkStringsAndWarn(bytes0[start0], bytes1[start1], bytes2[start2])) {
            PointableHelper.setNull(result);
            return;
        }
        // check that the int argument is numeric without fractions (in case arg is double or float)
        if (!ArgumentUtils.checkWarnOrSetInteger(ctx, sourceLoc, funcID, 3, bytes3, start3, mutableInt)) {
            PointableHelper.setNull(result);
            return;
        }
        int int4th = mutableInt.getIntegerValue();
        // Sets argument UTF8Pointables.
        strPtr1st.set(bytes0, start0 + 1, len0 - 1);
        strPtr2nd.set(bytes1, start1 + 1, len1 - 1);
        strPtr3rd.set(bytes2, start2 + 1, len2 - 1);

        // Resets the output storage.
        resultStorage.reset();
        // The actual processing.
        process(strPtr1st, strPtr2nd, strPtr3rd, int4th, result);
    }

    /**
     * The actual processing of a string function.
     *
     * @param first
     *            , the first argument.
     * @param second
     *            , the second argument.
     * @param third
     *            , the third argument.
     * @param fourth
     *            , the fourth argument.
     * @param resultPointable
     *            , the result.
     * @throws HyracksDataException
     */
    protected abstract void process(UTF8StringPointable first, UTF8StringPointable second, UTF8StringPointable third,
            int fourth, IPointable resultPointable) throws HyracksDataException;

    /** Checks the arguments expected to be strings returning {@code false} if they are not and issuing a warning */
    private boolean checkStringsAndWarn(byte actualType0, byte actualType1, byte actualType2) {
        return checkAndWarn(0, actualType0) && checkAndWarn(1, actualType1) && checkAndWarn(2, actualType2);
    }

    private boolean checkAndWarn(int argIdx, byte actualType) {
        if (actualType != ATypeTag.SERIALIZED_STRING_TYPE_TAG) {
            ExceptionUtil.warnTypeMismatch(ctx, sourceLoc, funcID, actualType, argIdx, ATypeTag.STRING);
            return false;
        }
        return true;
    }
}
