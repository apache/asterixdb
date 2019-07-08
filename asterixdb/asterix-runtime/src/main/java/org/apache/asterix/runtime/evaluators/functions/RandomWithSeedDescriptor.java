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
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.runtime.evaluators.base.AbstractScalarFunctionDynamicDescriptor;
import org.apache.asterix.runtime.evaluators.functions.utils.RandomHelper;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import org.apache.hyracks.algebricks.runtime.base.IEvaluatorContext;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluator;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluatorFactory;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.api.IPointable;
import org.apache.hyracks.data.std.primitive.VoidPointable;
import org.apache.hyracks.dataflow.common.data.accessors.IFrameTupleReference;

@MissingNullInOutFunction
public class RandomWithSeedDescriptor extends AbstractScalarFunctionDynamicDescriptor {
    private static final long serialVersionUID = 1L;

    public static final IFunctionDescriptorFactory FACTORY = new IFunctionDescriptorFactory() {
        @Override
        public IFunctionDescriptor createFunctionDescriptor() {
            return new RandomWithSeedDescriptor();
        }
    };

    @Override
    public IScalarEvaluatorFactory createEvaluatorFactory(IScalarEvaluatorFactory[] args) {
        return new IScalarEvaluatorFactory() {
            private static final long serialVersionUID = 1L;

            @Override
            public IScalarEvaluator createScalarEvaluator(IEvaluatorContext ctx) throws HyracksDataException {
                return new IScalarEvaluator() {
                    private final IScalarEvaluator eval0 = args[0].createScalarEvaluator(ctx);
                    private final IPointable arg0 = new VoidPointable();

                    private final RandomHelper randomHelper = new RandomHelper(true);

                    @Override
                    public void evaluate(IFrameTupleReference tuple, IPointable resultPointable)
                            throws HyracksDataException {
                        eval0.evaluate(tuple, arg0);

                        if (PointableHelper.checkAndSetMissingOrNull(resultPointable, arg0)) {
                            return;
                        }

                        byte[] bytes = arg0.getByteArray();
                        int offset = arg0.getStartOffset();
                        ATypeTag tt = ATypeTag.VALUE_TYPE_MAPPING[bytes[offset]];
                        switch (tt) {
                            case TINYINT:
                            case SMALLINT:
                            case INTEGER:
                            case BIGINT:
                            case FLOAT:
                            case DOUBLE:
                                randomHelper.setSeed(bytes, offset + 1, arg0.getLength() - 1);
                                randomHelper.nextDouble(resultPointable);
                                break;
                            default:
                                PointableHelper.setNull(resultPointable);
                                break;
                        }
                    }
                };
            }
        };
    }

    @Override
    public FunctionIdentifier getIdentifier() {
        return BuiltinFunctions.RANDOM_WITH_SEED;
    }
}
