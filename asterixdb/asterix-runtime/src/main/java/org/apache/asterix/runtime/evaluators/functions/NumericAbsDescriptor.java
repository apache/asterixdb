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
 * Numeric function Absolute value
 * Author : Xiaoyu Ma@UC Irvine
 * 01/30/2012
 */
package org.apache.asterix.runtime.evaluators.functions;

import org.apache.asterix.common.annotations.MissingNullInOutFunction;
import org.apache.asterix.om.functions.BuiltinFunctions;
import org.apache.asterix.om.functions.IFunctionDescriptor;
import org.apache.asterix.om.functions.IFunctionDescriptorFactory;
import org.apache.asterix.runtime.evaluators.base.AbstractScalarFunctionDynamicDescriptor;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import org.apache.hyracks.algebricks.runtime.base.IEvaluatorContext;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluator;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluatorFactory;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.api.IPointable;

@MissingNullInOutFunction
public class NumericAbsDescriptor extends AbstractScalarFunctionDynamicDescriptor {
    private static final long serialVersionUID = 1L;
    public static final IFunctionDescriptorFactory FACTORY = new IFunctionDescriptorFactory() {
        @Override
        public IFunctionDescriptor createFunctionDescriptor() {
            return new NumericAbsDescriptor();
        }
    };

    @Override
    public FunctionIdentifier getIdentifier() {
        return BuiltinFunctions.NUMERIC_ABS;
    }

    @Override
    public IScalarEvaluatorFactory createEvaluatorFactory(IScalarEvaluatorFactory[] args) {
        return new IScalarEvaluatorFactory() {
            private static final long serialVersionUID = 1L;

            @Override
            public IScalarEvaluator createScalarEvaluator(IEvaluatorContext ctx) throws HyracksDataException {
                return new NumericAbsEvaluator(ctx, args[0]);
            }
        };
    }

    private class NumericAbsEvaluator extends AbstractUnaryNumericFunctionEval {

        NumericAbsEvaluator(IEvaluatorContext context, IScalarEvaluatorFactory argEvalFactory)
                throws HyracksDataException {
            super(context, argEvalFactory, NumericAbsDescriptor.this.getIdentifier(), sourceLoc);
        }

        @Override
        protected void processInt8(byte arg, IPointable resultPointable) throws HyracksDataException {
            if (arg >= 0) {
                resultPointable.set(argPtr);
                return;
            }
            aInt8.setValue((byte) (0 - arg));
            serialize(aInt8, int8Serde, resultPointable);
        }

        @Override
        protected void processInt16(short arg, IPointable resultPointable) throws HyracksDataException {
            if (arg >= 0) {
                resultPointable.set(argPtr);
                return;
            }
            aInt16.setValue((short) (0 - arg));
            serialize(aInt16, int16Serde, resultPointable);
        }

        @Override
        protected void processInt32(int arg, IPointable resultPointable) throws HyracksDataException {
            if (arg >= 0) {
                resultPointable.set(argPtr);
                return;
            }
            aInt32.setValue(0 - arg);
            serialize(aInt32, int32Serde, resultPointable);
        }

        @Override
        protected void processInt64(long arg, IPointable resultPointable) throws HyracksDataException {
            if (arg >= 0) {
                resultPointable.set(argPtr);
                return;
            }
            aInt64.setValue(0L - arg);
            serialize(aInt64, int64Serde, resultPointable);
        }

        @Override
        protected void processFloat(float arg, IPointable resultPointable) throws HyracksDataException {
            if (arg >= 0.0f) {
                resultPointable.set(argPtr);
                return;
            }
            aFloat.setValue(0.0f - arg);
            serialize(aFloat, floatSerde, resultPointable);
        }

        @Override
        protected void processDouble(double arg, IPointable resultPointable) throws HyracksDataException {
            if (arg >= 0.0d) {
                resultPointable.set(argPtr);
                return;
            }
            aDouble.setValue(0.0d - arg);
            serialize(aDouble, doubleSerde, resultPointable);
        }
    }

}
