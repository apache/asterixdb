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
/*
 * Numeric function Round half to even
 * Author : Xiaoyu Ma@UC Irvine
 * 01/30/2012
 */
package org.apache.asterix.runtime.evaluators.functions;

import org.apache.asterix.common.annotations.MissingNullInOutFunction;
import org.apache.asterix.om.functions.BuiltinFunctions;
import org.apache.asterix.om.functions.IFunctionDescriptorFactory;
import org.apache.asterix.runtime.evaluators.base.AbstractScalarFunctionDynamicDescriptor;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluator;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluatorFactory;
import org.apache.hyracks.api.context.IEvaluatorContext;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.exceptions.SourceLocation;
import org.apache.hyracks.data.std.api.IPointable;

@MissingNullInOutFunction
public class NumericRoundHalfToEvenDescriptor extends AbstractScalarFunctionDynamicDescriptor {
    private static final long serialVersionUID = 1L;
    public static final IFunctionDescriptorFactory FACTORY = NumericRoundHalfToEvenDescriptor::new;

    @Override
    public FunctionIdentifier getIdentifier() {
        return BuiltinFunctions.NUMERIC_ROUND_HALF_TO_EVEN;
    }

    @Override
    public IScalarEvaluatorFactory createEvaluatorFactory(final IScalarEvaluatorFactory[] args) {
        return new IScalarEvaluatorFactory() {
            private static final long serialVersionUID = 1L;

            @Override
            public IScalarEvaluator createScalarEvaluator(final IEvaluatorContext ctx) throws HyracksDataException {
                return new RoundHalfToEvenEvaluator(ctx, args[0], getIdentifier(), sourceLoc);
            }
        };
    }

    private static class RoundHalfToEvenEvaluator extends AbstractUnaryNumericFunctionEval {

        RoundHalfToEvenEvaluator(IEvaluatorContext context, IScalarEvaluatorFactory argEvalFactory,
                FunctionIdentifier funID, SourceLocation sourceLocation) throws HyracksDataException {
            super(context, argEvalFactory, funID, sourceLocation);
        }

        @Override
        protected void processInt8(byte arg, IPointable resultPointable) throws HyracksDataException {
            resultPointable.set(argPtr);
        }

        @Override
        protected void processInt16(short arg, IPointable resultPointable) throws HyracksDataException {
            resultPointable.set(argPtr);
        }

        @Override
        protected void processInt32(int arg, IPointable resultPointable) throws HyracksDataException {
            resultPointable.set(argPtr);
        }

        @Override
        protected void processInt64(long arg, IPointable resultPointable) throws HyracksDataException {
            resultPointable.set(argPtr);
        }

        @Override
        protected void processFloat(float arg, IPointable resultPointable) throws HyracksDataException {
            aFloat.setValue((float) Math.rint(arg));
            serialize(aFloat, floatSerde, resultPointable);
        }

        @Override
        protected void processDouble(double arg, IPointable resultPointable) throws HyracksDataException {
            aDouble.setValue(Math.rint(arg));
            serialize(aDouble, doubleSerde, resultPointable);
        }
    }
}
