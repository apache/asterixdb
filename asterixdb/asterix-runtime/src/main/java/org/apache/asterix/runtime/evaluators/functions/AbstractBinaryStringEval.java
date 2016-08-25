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

import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.EnumDeserializer;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluator;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluatorFactory;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.data.std.api.IPointable;
import org.apache.hyracks.data.std.primitive.UTF8StringPointable;
import org.apache.hyracks.data.std.primitive.VoidPointable;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.apache.hyracks.dataflow.common.data.accessors.IFrameTupleReference;

public abstract class AbstractBinaryStringEval implements IScalarEvaluator {

    // Argument evaluators.
    private IScalarEvaluator evalLeft;
    private IScalarEvaluator evalRight;

    // Argument pointables.
    private IPointable argPtrLeft = new VoidPointable();
    private IPointable argPtrSecond = new VoidPointable();
    private final UTF8StringPointable leftPtr = new UTF8StringPointable();
    private final UTF8StringPointable rightPtr = new UTF8StringPointable();

    // For results.
    protected ArrayBackedValueStorage resultStorage = new ArrayBackedValueStorage();
    protected DataOutput dataOutput = resultStorage.getDataOutput();

    // Function ID, for error reporting.
    private final FunctionIdentifier funcID;

    public AbstractBinaryStringEval(IHyracksTaskContext context, IScalarEvaluatorFactory evalLeftFactory,
            IScalarEvaluatorFactory evalRightFactory, FunctionIdentifier funcID) throws AlgebricksException {
        this.evalLeft = evalLeftFactory.createScalarEvaluator(context);
        this.evalRight = evalRightFactory.createScalarEvaluator(context);
        this.funcID = funcID;
    }

    @SuppressWarnings("unchecked")
    @Override
    public void evaluate(IFrameTupleReference tuple, IPointable resultPointable) throws AlgebricksException {
        resultStorage.reset();

        // Gets the first argument.
        evalLeft.evaluate(tuple, argPtrLeft);
        byte[] bytes0 = argPtrLeft.getByteArray();
        int offset0 = argPtrLeft.getStartOffset();
        int len0 = argPtrLeft.getLength();

        // Gets the second argument.
        evalRight.evaluate(tuple, argPtrSecond);
        byte[] bytes1 = argPtrSecond.getByteArray();
        int offset1 = argPtrSecond.getStartOffset();
        int len1 = argPtrSecond.getLength();

        // Type check.
        if (bytes0[offset0] != ATypeTag.SERIALIZED_STRING_TYPE_TAG
                || bytes1[offset1] != ATypeTag.SERIALIZED_STRING_TYPE_TAG) {
            throw new AlgebricksException(funcID.getName() + ": expects input type STRING, but got "
                    + EnumDeserializer.ATYPETAGDESERIALIZER.deserialize(bytes0[offset0]) + " and "
                    + EnumDeserializer.ATYPETAGDESERIALIZER.deserialize(bytes1[offset1]) + ")!");
        }

        // Sets StringUTF8Pointables.
        leftPtr.set(bytes0, offset0 + 1, len0 - 1);
        rightPtr.set(bytes1, offset1 + 1, len1 - 1);

        // The actual processing.
        try {
            process(leftPtr, rightPtr, resultPointable);
        } catch (IOException e) {
            throw new AlgebricksException(e);
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
