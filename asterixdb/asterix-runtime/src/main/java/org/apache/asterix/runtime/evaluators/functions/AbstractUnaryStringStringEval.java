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
import org.apache.hyracks.data.std.util.GrowableArray;
import org.apache.hyracks.data.std.util.UTF8StringBuilder;
import org.apache.hyracks.dataflow.common.data.accessors.IFrameTupleReference;

abstract class AbstractUnaryStringStringEval implements IScalarEvaluator {

    private final IEvaluatorContext ctx;
    // For the argument.
    private final IScalarEvaluator argEval;
    private final VoidPointable argPtr = new VoidPointable();
    private final UTF8StringPointable stringPtr = new UTF8StringPointable();

    // For writing results.
    final GrowableArray resultArray = new GrowableArray();
    final UTF8StringBuilder resultBuilder = new UTF8StringBuilder();
    private final ArrayBackedValueStorage resultStorage = new ArrayBackedValueStorage();
    private final DataOutput dataOutput = resultStorage.getDataOutput();
    private final FunctionIdentifier funcID;
    protected final SourceLocation sourceLoc;

    AbstractUnaryStringStringEval(IEvaluatorContext context, IScalarEvaluatorFactory argEvalFactory,
            FunctionIdentifier funcID, SourceLocation sourceLoc) throws HyracksDataException {
        this.argEval = argEvalFactory.createScalarEvaluator(context);
        this.funcID = funcID;
        this.sourceLoc = sourceLoc;
        this.ctx = context;
    }

    @Override
    public void evaluate(IFrameTupleReference tuple, IPointable resultPointable) throws HyracksDataException {
        resultStorage.reset();
        argEval.evaluate(tuple, argPtr);

        if (PointableHelper.checkAndSetMissingOrNull(resultPointable, argPtr)) {
            return;
        }

        byte[] argBytes = argPtr.getByteArray();
        int offset = argPtr.getStartOffset();
        byte inputTypeTag = argBytes[offset];
        if (inputTypeTag != ATypeTag.SERIALIZED_STRING_TYPE_TAG) {
            PointableHelper.setNull(resultPointable);
            ExceptionUtil.warnTypeMismatch(ctx, sourceLoc, funcID, argBytes[offset], 0, ATypeTag.STRING);
            return;
        }
        stringPtr.set(argBytes, offset + 1, argPtr.getLength() - 1);
        resultArray.reset();
        try {
            process(stringPtr, resultPointable);
            writeResult(resultPointable);
        } catch (IOException e) {
            throw HyracksDataException.create(e);
        }
    }

    /**
     * Processes an input UTF8 string.
     *
     * @param inputString
     *            , the input string.
     * @param resultPointable
     *            , a pointable that is supposed to point to the result.
     */
    abstract void process(UTF8StringPointable inputString, IPointable resultPointable) throws IOException;

    // Writes the result.
    void writeResult(IPointable resultPointable) throws IOException {
        dataOutput.writeByte(ATypeTag.SERIALIZED_STRING_TYPE_TAG);
        dataOutput.write(resultArray.getByteArray(), 0, resultArray.getLength());
        resultPointable.set(resultStorage);
    }
}
