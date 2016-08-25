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

abstract class AbstractTripleStringEval implements IScalarEvaluator {

    // Argument evaluators.
    private IScalarEvaluator eval0;
    private IScalarEvaluator eval1;
    private IScalarEvaluator eval2;

    // Argument pointables.
    private IPointable argPtrFirst = new VoidPointable();
    private IPointable argPtrSecond = new VoidPointable();
    private IPointable argPtrThird = new VoidPointable();
    private final UTF8StringPointable strPtr1st = new UTF8StringPointable();
    private final UTF8StringPointable strPtr2nd = new UTF8StringPointable();
    private final UTF8StringPointable strPtr3rd = new UTF8StringPointable();

    // For outputting results.
    ArrayBackedValueStorage resultStorage = new ArrayBackedValueStorage();
    DataOutput dout = resultStorage.getDataOutput();

    // Function ID, for error reporting.
    private final FunctionIdentifier funcID;

    AbstractTripleStringEval(IHyracksTaskContext context, IScalarEvaluatorFactory eval0, IScalarEvaluatorFactory eval1,
            IScalarEvaluatorFactory eval2, FunctionIdentifier funcID) throws AlgebricksException {
        this.eval0 = eval0.createScalarEvaluator(context);
        this.eval1 = eval1.createScalarEvaluator(context);
        this.eval2 = eval2.createScalarEvaluator(context);
        this.funcID = funcID;
    }

    @SuppressWarnings("unchecked")
    @Override
    public void evaluate(IFrameTupleReference tuple, IPointable result) throws AlgebricksException {
        // Gets the first argument.
        eval0.evaluate(tuple, argPtrFirst);
        byte[] bytes0 = argPtrFirst.getByteArray();
        int start0 = argPtrFirst.getStartOffset();
        int len0 = argPtrFirst.getLength();

        // Gets the second argument.
        eval1.evaluate(tuple, argPtrSecond);
        byte[] bytes1 = argPtrSecond.getByteArray();
        int start1 = argPtrSecond.getStartOffset();
        int len1 = argPtrSecond.getLength();

        // Gets the third argument.
        eval2.evaluate(tuple, argPtrThird);
        byte[] bytes2 = argPtrThird.getByteArray();
        int start2 = argPtrThird.getStartOffset();
        int len2 = argPtrThird.getLength();

        // Type check.
        if (bytes0[start0] != ATypeTag.SERIALIZED_STRING_TYPE_TAG
                || bytes1[start1] != ATypeTag.SERIALIZED_STRING_TYPE_TAG
                || bytes2[start2] != ATypeTag.SERIALIZED_STRING_TYPE_TAG) {
            throw new AlgebricksException(funcID.getName() + ": expects iput type (STRING, STRING, STRING) but got ("
                    + EnumDeserializer.ATYPETAGDESERIALIZER.deserialize(bytes0[start0]) + ", "
                    + EnumDeserializer.ATYPETAGDESERIALIZER.deserialize(bytes1[start1]) + ", "
                    + EnumDeserializer.ATYPETAGDESERIALIZER.deserialize(bytes2[start2]) + ")");
        }

        // Sets argument UTF8Pointables.
        strPtr1st.set(bytes0, start0 + 1, len0 - 1);
        strPtr2nd.set(bytes1, start1 + 1, len1 - 1);
        strPtr3rd.set(bytes2, start2 + 1, len2 - 1);

        // Resets the output storage.
        resultStorage.reset();
        // The actual processing.
        try {
            process(strPtr1st, strPtr2nd, strPtr3rd, result);
        } catch (IOException e) {
            throw new AlgebricksException(e);
        }
    }

    /**
     * The actual processing of a string function.
     *
     * @param first
     *            , the first argument.
     * @param second
     *            , the second argument.
     * @param third
     *            , the second argument.
     * @param resultPointable
     *            , the result.
     * @throws IOException
     */
    protected abstract void process(UTF8StringPointable first, UTF8StringPointable second, UTF8StringPointable third,
            IPointable resultPointable) throws IOException;

}
