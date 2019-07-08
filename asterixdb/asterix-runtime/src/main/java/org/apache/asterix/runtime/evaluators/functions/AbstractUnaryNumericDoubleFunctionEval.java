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

import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import org.apache.hyracks.algebricks.runtime.base.IEvaluatorContext;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluatorFactory;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.exceptions.SourceLocation;
import org.apache.hyracks.data.std.api.IPointable;

/**
 * An abstract class for functions that take a double value as the input and output a double value.
 */
abstract class AbstractUnaryNumericDoubleFunctionEval extends AbstractUnaryNumericFunctionEval {

    public AbstractUnaryNumericDoubleFunctionEval(IEvaluatorContext context, IScalarEvaluatorFactory argEvalFactory,
            FunctionIdentifier funcID, SourceLocation sourceLoc) throws HyracksDataException {
        super(context, argEvalFactory, funcID, sourceLoc);
    }

    @Override
    protected void processInt8(byte arg, IPointable resultPointable) throws HyracksDataException {
        processDouble(arg, resultPointable);
    }

    @Override
    protected void processInt16(short arg, IPointable resultPointable) throws HyracksDataException {
        processDouble(arg, resultPointable);
    }

    @Override
    protected void processInt32(int arg, IPointable resultPointable) throws HyracksDataException {
        processDouble(arg, resultPointable);
    }

    @Override
    protected void processInt64(long arg, IPointable resultPointable) throws HyracksDataException {
        processDouble(arg, resultPointable);
    }

    @Override
    protected void processFloat(float arg, IPointable resultPointable) throws HyracksDataException {
        processDouble(arg, resultPointable);
    }

    @Override
    protected abstract void processDouble(double arg, IPointable resultPointable) throws HyracksDataException;
}
