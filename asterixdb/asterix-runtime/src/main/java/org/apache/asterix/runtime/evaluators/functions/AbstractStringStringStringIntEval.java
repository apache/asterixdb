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

import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.hierachy.ATypeHierarchy;
import org.apache.asterix.runtime.exceptions.TypeMismatchException;
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
    // Argument evaluators.
    private IScalarEvaluator eval0;
    private IScalarEvaluator eval1;
    private IScalarEvaluator eval2;
    private IScalarEvaluator eval3;

    // Argument pointables.
    final IPointable argPtrFirst = new VoidPointable();
    final IPointable argPtrSecond = new VoidPointable();
    final IPointable argPtrThird = new VoidPointable();
    final IPointable argPtrFourth = new VoidPointable();
    private final UTF8StringPointable strPtr1st = new UTF8StringPointable();
    private final UTF8StringPointable strPtr2nd = new UTF8StringPointable();
    private final UTF8StringPointable strPtr3rd = new UTF8StringPointable();

    // For outputting results.
    ArrayBackedValueStorage resultStorage = new ArrayBackedValueStorage();
    DataOutput dout = resultStorage.getDataOutput();

    // Function ID, for error reporting.
    protected final FunctionIdentifier funcID;
    protected final SourceLocation sourceLoc;

    AbstractStringStringStringIntEval(IEvaluatorContext context, IScalarEvaluatorFactory eval0,
            IScalarEvaluatorFactory eval1, IScalarEvaluatorFactory eval2, IScalarEvaluatorFactory eval3,
            FunctionIdentifier funcID, SourceLocation sourceLoc) throws HyracksDataException {
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
        if (bytes0[start0] != ATypeTag.SERIALIZED_STRING_TYPE_TAG) {
            throw new TypeMismatchException(sourceLoc, funcID, 0, bytes0[start0], ATypeTag.SERIALIZED_STRING_TYPE_TAG);
        }
        if (bytes1[start1] != ATypeTag.SERIALIZED_STRING_TYPE_TAG) {
            throw new TypeMismatchException(sourceLoc, funcID, 1, bytes1[start1], ATypeTag.SERIALIZED_STRING_TYPE_TAG);
        }
        if (bytes2[start2] != ATypeTag.SERIALIZED_STRING_TYPE_TAG) {
            throw new TypeMismatchException(sourceLoc, funcID, 2, bytes2[start2], ATypeTag.SERIALIZED_STRING_TYPE_TAG);
        }
        if (bytes3[start3] != ATypeTag.SERIALIZED_INT8_TYPE_TAG && bytes3[start3] != ATypeTag.SERIALIZED_INT16_TYPE_TAG
                && bytes3[start3] != ATypeTag.SERIALIZED_INT32_TYPE_TAG
                && bytes3[start3] != ATypeTag.SERIALIZED_INT64_TYPE_TAG) {
            throw new TypeMismatchException(sourceLoc, funcID, 3, bytes3[start3], ATypeTag.SERIALIZED_INT8_TYPE_TAG,
                    ATypeTag.SERIALIZED_INT16_TYPE_TAG, ATypeTag.SERIALIZED_INT32_TYPE_TAG,
                    ATypeTag.SERIALIZED_INT64_TYPE_TAG);
        }

        // Sets argument UTF8Pointables.
        strPtr1st.set(bytes0, start0 + 1, len0 - 1);
        strPtr2nd.set(bytes1, start1 + 1, len1 - 1);
        strPtr3rd.set(bytes2, start2 + 1, len2 - 1);

        long int4th = ATypeHierarchy.getLongValue(funcID.getName(), 3, bytes3, start3);

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
            long fourth, IPointable resultPointable) throws HyracksDataException;
}
