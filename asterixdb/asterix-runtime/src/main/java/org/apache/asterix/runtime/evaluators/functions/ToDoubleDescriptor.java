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

import java.io.IOException;

import org.apache.asterix.common.annotations.MissingNullInOutFunction;
import org.apache.asterix.dataflow.data.nontagged.serde.ABooleanSerializerDeserializer;
import org.apache.asterix.om.functions.BuiltinFunctions;
import org.apache.asterix.om.functions.IFunctionDescriptor;
import org.apache.asterix.om.functions.IFunctionDescriptorFactory;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.hierachy.ATypeHierarchy;
import org.apache.asterix.om.types.hierachy.ITypeConvertComputer;
import org.apache.asterix.runtime.evaluators.base.AbstractScalarFunctionDynamicDescriptor;
import org.apache.asterix.runtime.evaluators.constructors.AbstractDoubleConstructorEvaluator;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import org.apache.hyracks.algebricks.runtime.base.IEvaluatorContext;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluator;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluatorFactory;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.api.IPointable;

@MissingNullInOutFunction
public class ToDoubleDescriptor extends AbstractScalarFunctionDynamicDescriptor {
    private static final long serialVersionUID = 1L;
    public static final IFunctionDescriptorFactory FACTORY = new IFunctionDescriptorFactory() {
        @Override
        public IFunctionDescriptor createFunctionDescriptor() {
            return new ToDoubleDescriptor();
        }
    };

    @Override
    public IScalarEvaluatorFactory createEvaluatorFactory(final IScalarEvaluatorFactory[] args) {
        return new IScalarEvaluatorFactory() {
            private static final long serialVersionUID = 1L;

            @Override
            public IScalarEvaluator createScalarEvaluator(final IEvaluatorContext ctx) throws HyracksDataException {
                return new AbstractDoubleConstructorEvaluator(args[0].createScalarEvaluator(ctx), sourceLoc) {
                    @Override
                    protected void evaluateImpl(IPointable result) throws IOException {
                        byte[] bytes = inputArg.getByteArray();
                        int offset = inputArg.getStartOffset();
                        ATypeTag tt = ATypeTag.VALUE_TYPE_MAPPING[bytes[offset]];
                        switch (tt) {
                            case BOOLEAN:
                                boolean b = ABooleanSerializerDeserializer.getBoolean(bytes, offset + 1);
                                aDouble.setValue(b ? 1 : 0);
                                DOUBLE_SERDE.serialize(aDouble, out);
                                result.set(resultStorage);
                                break;

                            case TINYINT:
                            case SMALLINT:
                            case INTEGER:
                            case BIGINT:
                            case FLOAT:
                                ITypeConvertComputer tcc = ATypeHierarchy.getTypePromoteComputer(tt, ATypeTag.DOUBLE);
                                tcc.convertType(bytes, offset + 1, inputArg.getLength() - 1, out);
                                result.set(resultStorage);
                                break;

                            case ARRAY:
                            case MULTISET:
                            case OBJECT:
                                PointableHelper.setNull(result);
                                break;

                            default:
                                super.evaluateImpl(result);
                                break;
                        }
                    }

                    @Override
                    protected void handleUparseableString(IPointable result) {
                        PointableHelper.setNull(result);
                    }

                    @Override
                    protected FunctionIdentifier getIdentifier() {
                        return ToDoubleDescriptor.this.getIdentifier();
                    }
                };
            }
        };
    }

    @Override
    public FunctionIdentifier getIdentifier() {
        return BuiltinFunctions.TO_DOUBLE;
    }
}
