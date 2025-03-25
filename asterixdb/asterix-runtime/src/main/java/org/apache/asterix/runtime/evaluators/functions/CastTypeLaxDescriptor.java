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

import org.apache.asterix.common.annotations.MissingNullInOutFunction;
import org.apache.asterix.om.functions.BuiltinFunctions;
import org.apache.asterix.om.functions.IFunctionDescriptor;
import org.apache.asterix.om.functions.IFunctionDescriptorFactory;
import org.apache.asterix.om.functions.IFunctionTypeInferer;
import org.apache.asterix.om.typecomputer.impl.TypeComputeUtils;
import org.apache.asterix.om.types.IAType;
import org.apache.asterix.runtime.evaluators.base.AbstractScalarFunctionDynamicDescriptor;
import org.apache.asterix.runtime.functions.FunctionTypeInferers;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluator;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluatorFactory;
import org.apache.hyracks.api.context.IEvaluatorContext;
import org.apache.hyracks.api.exceptions.HyracksDataException;

/**
 * Implements 'lax' cast. It differs from the regular cast as follows:
 * <ul>
 * <li>Numeric type demotion does not fail if the input value is not within bounds for the target type.
 * Instead it returns min/max value of the target type.
 * </li>
 * <li>
 * If there's an error during casting then 'MISSING' is returned.
 * Note that errors from argument evaluation are still propagated.
 * </li>
 * </ul>
 */

@MissingNullInOutFunction
public class CastTypeLaxDescriptor extends AbstractScalarFunctionDynamicDescriptor {

    public static final IFunctionDescriptorFactory FACTORY = new IFunctionDescriptorFactory() {
        @Override
        public IFunctionDescriptor createFunctionDescriptor() {
            return new CastTypeLaxDescriptor();
        }

        @Override
        public IFunctionTypeInferer createFunctionTypeInferer() {
            return new FunctionTypeInferers.CastTypeInferer();
        }
    };

    private static final long serialVersionUID = 1L;
    private IAType reqType;
    private IAType inputType;

    private CastTypeLaxDescriptor() {
    }

    @Override
    public void setImmutableStates(Object... states) {
        reqType = (IAType) states[0];
        inputType = (IAType) states[1];
        // If reqType or inputType is null, or they are the same, it indicates there is a bug in the compiler.
        if (reqType == null || inputType == null || reqType.equals(inputType)) {
            throw new IllegalStateException(
                    "Invalid types for casting, required type " + reqType + ", input type " + inputType);
        }
        // NULLs and MISSINGs are handled by the generated code, therefore we only need to handle actual types here.
        this.reqType = TypeComputeUtils.getActualType(reqType);
        this.inputType = TypeComputeUtils.getActualType(inputType);
    }

    @Override
    public FunctionIdentifier getIdentifier() {
        return BuiltinFunctions.CAST_TYPE_LAX;
    }

    @Override
    public IScalarEvaluatorFactory createEvaluatorFactory(final IScalarEvaluatorFactory[] args) {
        final IScalarEvaluatorFactory recordEvalFactory = args[0];

        return new IScalarEvaluatorFactory() {
            private static final long serialVersionUID = 1L;

            @Override
            public IScalarEvaluator createScalarEvaluator(IEvaluatorContext ctx) throws HyracksDataException {
                IScalarEvaluator argEval = recordEvalFactory.createScalarEvaluator(ctx);
                return new CastTypeLaxEvaluator(reqType, inputType, argEval, sourceLoc);
            }
        };
    }
}
