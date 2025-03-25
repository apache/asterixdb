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

import org.apache.asterix.om.functions.BuiltinFunctions;
import org.apache.asterix.om.functions.IFunctionDescriptorFactory;
import org.apache.asterix.runtime.evaluators.base.AbstractScalarFunctionDynamicDescriptor;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluator;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluatorFactory;
import org.apache.hyracks.api.context.IEvaluatorContext;
import org.apache.hyracks.api.exceptions.HyracksDataException;

/**
 * ifinf(arg1, arg2, ...) scans the list of arguments in order and returns the first numeric argument it encounters.
 * If the argument being inspected is infinity as determined by the mathematical definition of floating-points, then
 * it skips the argument and inspects the next one. It returns missing if the argument being inspected is missing.
 * It returns null if:
 * 1. the argument being inspected is not numeric.
 * 2. all the arguments have been inspected and no candidate value has been found.
 *
 * Number of arguments: Min is 2. Max is {@link Short#MAX_VALUE}
 */
public class IfInfDescriptor extends AbstractScalarFunctionDynamicDescriptor {

    private static final long serialVersionUID = 1L;
    public static final IFunctionDescriptorFactory FACTORY = IfInfDescriptor::new;

    @Override
    public IScalarEvaluatorFactory createEvaluatorFactory(final IScalarEvaluatorFactory[] args) {
        return new IScalarEvaluatorFactory() {
            private static final long serialVersionUID = 1L;

            @Override
            public IScalarEvaluator createScalarEvaluator(final IEvaluatorContext ctx) throws HyracksDataException {
                return new IfNanOrInfDescriptor.AbstractIfInfOrNanEval(ctx, args, false) {
                    @Override
                    protected boolean skipDouble(double d) {
                        return Double.isInfinite(d);
                    }

                    @Override
                    protected boolean skipFloat(float f) {
                        return Float.isInfinite(f);
                    }

                    @Override
                    protected FunctionIdentifier getIdentifier() {
                        return IfInfDescriptor.this.getIdentifier();
                    }
                };
            }
        };
    }

    @Override
    public FunctionIdentifier getIdentifier() {
        return BuiltinFunctions.IF_INF;
    }
}
