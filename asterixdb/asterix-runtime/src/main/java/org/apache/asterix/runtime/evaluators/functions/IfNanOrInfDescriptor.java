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

import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.common.exceptions.RuntimeDataException;
import org.apache.asterix.om.functions.BuiltinFunctions;
import org.apache.asterix.om.functions.IFunctionDescriptorFactory;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.runtime.evaluators.base.AbstractScalarFunctionDynamicDescriptor;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import org.apache.hyracks.algebricks.runtime.base.IEvaluatorContext;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluator;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluatorFactory;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.api.IPointable;
import org.apache.hyracks.data.std.primitive.DoublePointable;
import org.apache.hyracks.data.std.primitive.FloatPointable;
import org.apache.hyracks.data.std.primitive.VoidPointable;
import org.apache.hyracks.dataflow.common.data.accessors.IFrameTupleReference;

/**
 * ifnanorinf(arg1, arg2, ...) scans the list of arguments in order and returns the first numeric argument it
 * encounters.
 * If the argument being inspected is missing or NaN/Infinity as determined by the mathematical definition of
 * floating-points, then it skips the argument and inspects the next one. It returns null if:
 * 1. the argument being inspected is not numeric.
 * 2. all the arguments have been inspected and no candidate value has been found.
 *
 * Number of arguments: Min is 2. Max is {@link Short#MAX_VALUE}
 */
public class IfNanOrInfDescriptor extends AbstractScalarFunctionDynamicDescriptor {

    private static final long serialVersionUID = 1L;
    public static final IFunctionDescriptorFactory FACTORY = IfNanOrInfDescriptor::new;

    @Override
    public IScalarEvaluatorFactory createEvaluatorFactory(final IScalarEvaluatorFactory[] args) {
        return new IScalarEvaluatorFactory() {
            private static final long serialVersionUID = 1L;

            @Override
            public IScalarEvaluator createScalarEvaluator(final IEvaluatorContext ctx) throws HyracksDataException {
                return new AbstractIfInfOrNanEval(ctx, args, true) {
                    @Override
                    protected boolean skipDouble(double d) {
                        return Double.isInfinite(d) || Double.isNaN(d);
                    }

                    @Override
                    protected boolean skipFloat(float f) {
                        return Float.isInfinite(f) || Float.isNaN(f);
                    }

                    @Override
                    protected FunctionIdentifier getIdentifier() {
                        return IfNanOrInfDescriptor.this.getIdentifier();
                    }
                };
            }
        };
    }

    @Override
    public FunctionIdentifier getIdentifier() {
        return BuiltinFunctions.IF_NAN_OR_INF;
    }

    public static abstract class AbstractIfInfOrNanEval implements IScalarEvaluator {

        private final IScalarEvaluator[] argEvals;

        private final IPointable argPtr;
        private final boolean skipMissing;

        AbstractIfInfOrNanEval(IEvaluatorContext ctx, IScalarEvaluatorFactory[] args, boolean skipMissing)
                throws HyracksDataException {
            argEvals = new IScalarEvaluator[args.length];
            for (int i = 0; i < argEvals.length; i++) {
                argEvals[i] = args[i].createScalarEvaluator(ctx);
            }
            argPtr = new VoidPointable();
            this.skipMissing = skipMissing;
        }

        @Override
        public void evaluate(IFrameTupleReference tuple, IPointable result) throws HyracksDataException {
            for (int i = 0; i < argEvals.length; i++) {
                argEvals[i].evaluate(tuple, argPtr);
                ATypeTag argTypeTag = PointableHelper.getTypeTag(argPtr);
                if (argTypeTag == null) {
                    throw new RuntimeDataException(ErrorCode.INVALID_FORMAT, getIdentifier(), i);
                }
                switch (argTypeTag) {
                    case DOUBLE:
                        double d = DoublePointable.getDouble(argPtr.getByteArray(), argPtr.getStartOffset() + 1);
                        if (skipDouble(d)) {
                            continue;
                        }
                        result.set(argPtr);
                        return;
                    case FLOAT:
                        float f = FloatPointable.getFloat(argPtr.getByteArray(), argPtr.getStartOffset() + 1);
                        if (skipFloat(f)) {
                            continue;
                        }
                        result.set(argPtr);
                        return;
                    case MISSING:
                        if (skipMissing) {
                            continue;
                        }
                    case BIGINT:
                    case INTEGER:
                    case SMALLINT:
                    case TINYINT:
                        result.set(argPtr);
                        return;
                    default:
                        PointableHelper.setNull(result);
                        return;
                }
            }

            PointableHelper.setNull(result);
        }

        protected abstract FunctionIdentifier getIdentifier();

        protected abstract boolean skipDouble(double d);

        protected abstract boolean skipFloat(float f);
    }
}
