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

import org.apache.asterix.om.exceptions.ExceptionUtil;
import org.apache.asterix.om.types.ATypeTag;
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

abstract class AbstractTripleStringEval implements IScalarEvaluator {

    private final IEvaluatorContext ctx;
    // Argument evaluators.
    private IScalarEvaluator eval0;
    private IScalarEvaluator eval1;
    private IScalarEvaluator eval2;

    // Argument pointables.
    final IPointable argPtrFirst = new VoidPointable();
    final IPointable argPtrSecond = new VoidPointable();
    final IPointable argPtrThird = new VoidPointable();
    private final UTF8StringPointable strPtr1st = new UTF8StringPointable();
    private final UTF8StringPointable strPtr2nd = new UTF8StringPointable();
    private final UTF8StringPointable strPtr3rd = new UTF8StringPointable();

    // For outputting results.
    ArrayBackedValueStorage resultStorage = new ArrayBackedValueStorage();
    DataOutput dout = resultStorage.getDataOutput();

    // Function ID, for error reporting.
    protected final FunctionIdentifier funcID;
    protected final SourceLocation sourceLoc;

    AbstractTripleStringEval(IEvaluatorContext context, IScalarEvaluatorFactory eval0, IScalarEvaluatorFactory eval1,
            IScalarEvaluatorFactory eval2, FunctionIdentifier funcID, SourceLocation sourceLoc)
            throws HyracksDataException {
        this.eval0 = eval0.createScalarEvaluator(context);
        this.eval1 = eval1.createScalarEvaluator(context);
        this.eval2 = eval2.createScalarEvaluator(context);
        this.funcID = funcID;
        this.sourceLoc = sourceLoc;
        this.ctx = context;
    }

    @SuppressWarnings("unchecked")
    @Override
    public void evaluate(IFrameTupleReference tuple, IPointable result) throws HyracksDataException {
        // Gets the arguments
        eval0.evaluate(tuple, argPtrFirst);
        eval1.evaluate(tuple, argPtrSecond);
        eval2.evaluate(tuple, argPtrThird);

        if (PointableHelper.checkAndSetMissingOrNull(result, argPtrFirst, argPtrSecond, argPtrThird)) {
            return;
        }

        // Arguments
        byte[] bytes0 = argPtrFirst.getByteArray();
        int start0 = argPtrFirst.getStartOffset();
        int len0 = argPtrFirst.getLength();
        byte[] bytes1 = argPtrSecond.getByteArray();
        int start1 = argPtrSecond.getStartOffset();
        int len1 = argPtrSecond.getLength();
        byte[] bytes2 = argPtrThird.getByteArray();
        int start2 = argPtrThird.getStartOffset();
        int len2 = argPtrThird.getLength();

        // Type check.
        if (bytes0[start0] != ATypeTag.SERIALIZED_STRING_TYPE_TAG) {
            PointableHelper.setNull(result);
            ExceptionUtil.warnTypeMismatch(ctx, sourceLoc, funcID, bytes0[start0], 0, ATypeTag.STRING);
            return;
        }
        if (bytes1[start1] != ATypeTag.SERIALIZED_STRING_TYPE_TAG) {
            PointableHelper.setNull(result);
            ExceptionUtil.warnTypeMismatch(ctx, sourceLoc, funcID, bytes1[start1], 1, ATypeTag.STRING);
            return;
        }
        if (bytes2[start2] != ATypeTag.SERIALIZED_STRING_TYPE_TAG) {
            PointableHelper.setNull(result);
            ExceptionUtil.warnTypeMismatch(ctx, sourceLoc, funcID, bytes2[start2], 2, ATypeTag.STRING);
            return;
        }

        // Sets argument UTF8Pointables.
        strPtr1st.set(bytes0, start0 + 1, len0 - 1);
        strPtr2nd.set(bytes1, start1 + 1, len1 - 1);
        strPtr3rd.set(bytes2, start2 + 1, len2 - 1);

        // Resets the output storage.
        resultStorage.reset();
        // The actual processing.
        process(strPtr1st, strPtr2nd, strPtr3rd, result);
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
     * @param resultPointable
     *            , the result.
     * @throws HyracksDataException
     */
    protected abstract void process(UTF8StringPointable first, UTF8StringPointable second, UTF8StringPointable third,
            IPointable resultPointable) throws HyracksDataException;
}
