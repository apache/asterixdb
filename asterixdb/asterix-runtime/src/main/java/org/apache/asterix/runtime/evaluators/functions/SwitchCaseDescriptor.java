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
import org.apache.asterix.om.functions.IFunctionDescriptor;
import org.apache.asterix.om.functions.IFunctionDescriptorFactory;
import org.apache.asterix.runtime.evaluators.base.AbstractScalarFunctionDynamicDescriptor;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import org.apache.hyracks.algebricks.runtime.base.IEvaluatorContext;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluator;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluatorFactory;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.api.IPointable;
import org.apache.hyracks.data.std.primitive.VoidPointable;
import org.apache.hyracks.dataflow.common.data.accessors.IFrameTupleReference;

public class SwitchCaseDescriptor extends AbstractScalarFunctionDynamicDescriptor {

    private static final long serialVersionUID = 1L;
    public static final IFunctionDescriptorFactory FACTORY = new IFunctionDescriptorFactory() {
        @Override
        public IFunctionDescriptor createFunctionDescriptor() {
            return new SwitchCaseDescriptor();
        }
    };

    @Override
    public FunctionIdentifier getIdentifier() {
        return BuiltinFunctions.SWITCH_CASE;
    }

    @Override
    public IScalarEvaluatorFactory createEvaluatorFactory(final IScalarEvaluatorFactory[] args) {
        return new IScalarEvaluatorFactory() {
            private static final long serialVersionUID = 1L;

            @Override
            public IScalarEvaluator createScalarEvaluator(final IEvaluatorContext ctx) throws HyracksDataException {
                final IPointable condPtr = new VoidPointable();
                final IPointable casePtr = new VoidPointable();
                final IPointable argPtr = new VoidPointable();

                final IScalarEvaluator[] evals = new IScalarEvaluator[args.length];
                // condition
                evals[0] = args[0].createScalarEvaluator(ctx);
                // case value
                for (int i = 1; i < evals.length - 1; i += 2) {
                    evals[i] = args[i].createScalarEvaluator(ctx);
                }
                // case expression
                for (int i = 2; i < evals.length - 1; i += 2) {
                    evals[i] = args[i].createScalarEvaluator(ctx);
                }
                // default expression
                evals[evals.length - 1] = args[evals.length - 1].createScalarEvaluator(ctx);

                return new IScalarEvaluator() {

                    @Override
                    public void evaluate(IFrameTupleReference tuple, IPointable result) throws HyracksDataException {
                        int n = args.length;
                        evals[0].evaluate(tuple, condPtr);
                        for (int i = 1; i < n; i += 2) {
                            evals[i].evaluate(tuple, casePtr);
                            if (equals(condPtr, casePtr)) {
                                evals[i + 1].evaluate(tuple, argPtr);
                                result.set(argPtr);
                                return;
                            }
                        }
                        // the default case
                        evals[n - 1].evaluate(tuple, argPtr);
                        result.set(argPtr);
                    }

                    private boolean equals(IPointable out1, IPointable out2) {
                        if (out1.getLength() != out2.getLength()) {
                            return false;
                        }
                        byte[] data1 = out1.getByteArray();
                        byte[] data2 = out2.getByteArray();
                        for (int i = out1.getStartOffset(), j = out2.getStartOffset(), k = 0; k < out1
                                .getLength(); ++i, ++j, ++k) {
                            if (data1[i] != data2[j]) {
                                return false;
                            }
                        }
                        return true;
                    }
                };
            }
        };
    }

}
