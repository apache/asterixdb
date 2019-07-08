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

import java.io.IOException;

import org.apache.asterix.om.types.ATypeTag;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import org.apache.hyracks.algebricks.runtime.base.IEvaluatorContext;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluatorFactory;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.exceptions.SourceLocation;
import org.apache.hyracks.data.std.api.IPointable;
import org.apache.hyracks.data.std.primitive.UTF8StringPointable;
import org.apache.hyracks.data.std.util.GrowableArray;
import org.apache.hyracks.data.std.util.UTF8StringBuilder;

public abstract class AbstractBinaryStringStringEval extends AbstractBinaryStringEval {

    // For outputting results.
    protected final UTF8StringPointable resultStrPtr = new UTF8StringPointable();
    protected final GrowableArray resultArray = new GrowableArray();
    protected final UTF8StringBuilder resultBuilder = new UTF8StringBuilder();

    public AbstractBinaryStringStringEval(IEvaluatorContext context, IScalarEvaluatorFactory evalLeftFactory,
            IScalarEvaluatorFactory evalRightFactory, FunctionIdentifier funcID, SourceLocation sourceLoc)
            throws HyracksDataException {
        super(context, evalLeftFactory, evalRightFactory, funcID, sourceLoc);
    }

    @Override
    public void process(UTF8StringPointable leftPtr, UTF8StringPointable rightPtr, IPointable resultPointable)
            throws IOException {
        resultArray.reset();
        compute(leftPtr, rightPtr, resultStrPtr);
        writeResult(resultPointable);
    }

    /**
     * Computes a string value from two input strings.
     *
     * @param left
     *            , the first input argument.
     * @param right
     *            , the second input argument.
     * @param resultStrPtr
     *            , a pointable that is supposed to point to the result string.
     * @throws HyracksDataException
     */
    protected abstract void compute(UTF8StringPointable left, UTF8StringPointable right,
            UTF8StringPointable resultStrPtr) throws IOException;

    // Writes the result.
    private void writeResult(IPointable resultPointable) throws IOException {
        dataOutput.writeByte(ATypeTag.SERIALIZED_STRING_TYPE_TAG);
        dataOutput.write(resultStrPtr.getByteArray(), 0, resultStrPtr.getLength());
        resultPointable.set(resultStorage);
    }
}
