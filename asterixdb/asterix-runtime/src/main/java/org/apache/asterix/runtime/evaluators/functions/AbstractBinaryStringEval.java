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

public abstract class AbstractBinaryStringEval implements IScalarEvaluator {

    private final IEvaluatorContext ctx;
    // Argument evaluators.
    private final IScalarEvaluator evalLeft;
    private final IScalarEvaluator evalRight;

    // Argument pointables.
    private final IPointable argPtrLeft = new VoidPointable();
    private final IPointable argPtrSecond = new VoidPointable();
    private final UTF8StringPointable leftPtr = new UTF8StringPointable();
    private final UTF8StringPointable rightPtr = new UTF8StringPointable();

    // For results.
    protected final ArrayBackedValueStorage resultStorage = new ArrayBackedValueStorage();
    protected final DataOutput dataOutput = resultStorage.getDataOutput();

    // Function ID, for error reporting.
    private final FunctionIdentifier funcID;
    private final SourceLocation sourceLoc;

    public AbstractBinaryStringEval(IEvaluatorContext context, IScalarEvaluatorFactory evalLeftFactory,
            IScalarEvaluatorFactory evalRightFactory, FunctionIdentifier funcID, SourceLocation sourceLoc)
            throws HyracksDataException {
        this.sourceLoc = sourceLoc;
        this.evalLeft = evalLeftFactory.createScalarEvaluator(context);
        this.evalRight = evalRightFactory.createScalarEvaluator(context);
        this.funcID = funcID;
        this.ctx = context;
    }

    @SuppressWarnings("unchecked")
    @Override
    public void evaluate(IFrameTupleReference tuple, IPointable resultPointable) throws HyracksDataException {
        resultStorage.reset();

        // Gets the arguments
        evalLeft.evaluate(tuple, argPtrLeft);
        evalRight.evaluate(tuple, argPtrSecond);

        if (PointableHelper.checkAndSetMissingOrNull(resultPointable, argPtrLeft, argPtrSecond)) {
            return;
        }

        byte[] bytes0 = argPtrLeft.getByteArray();
        int offset0 = argPtrLeft.getStartOffset();
        int len0 = argPtrLeft.getLength();
        byte[] bytes1 = argPtrSecond.getByteArray();
        int offset1 = argPtrSecond.getStartOffset();
        int len1 = argPtrSecond.getLength();

        // Type check.
        if (bytes0[offset0] != ATypeTag.SERIALIZED_STRING_TYPE_TAG) {
            PointableHelper.setNull(resultPointable);
            ExceptionUtil.warnTypeMismatch(ctx, sourceLoc, funcID, bytes0[offset0], 0, ATypeTag.STRING);
            return;
        }
        if (bytes1[offset1] != ATypeTag.SERIALIZED_STRING_TYPE_TAG) {
            PointableHelper.setNull(resultPointable);
            ExceptionUtil.warnTypeMismatch(ctx, sourceLoc, funcID, bytes1[offset1], 1, ATypeTag.STRING);
            return;
        }

        // Sets StringUTF8Pointables.
        leftPtr.set(bytes0, offset0 + 1, len0 - 1);
        rightPtr.set(bytes1, offset1 + 1, len1 - 1);

        // The actual processing.
        try {
            process(leftPtr, rightPtr, resultPointable);
        } catch (IOException e) {
            throw HyracksDataException.create(e);
        }
    }

    /**
     * The actual processing of a string function.
     *
     * @param left
     *            , the first argument.
     * @param right
     *            , the second argument.
     * @param resultPointable
     *            , the result.
     * @throws IOException
     */
    protected abstract void process(UTF8StringPointable left, UTF8StringPointable right, IPointable resultPointable)
            throws IOException;

}
