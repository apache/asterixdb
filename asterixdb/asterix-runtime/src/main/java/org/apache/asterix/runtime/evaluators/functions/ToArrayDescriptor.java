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

import org.apache.asterix.builders.OrderedListBuilder;
import org.apache.asterix.common.annotations.MissingNullInOutFunction;
import org.apache.asterix.om.functions.BuiltinFunctions;
import org.apache.asterix.om.functions.IFunctionDescriptor;
import org.apache.asterix.om.functions.IFunctionDescriptorFactory;
import org.apache.asterix.om.functions.IFunctionTypeInferer;
import org.apache.asterix.om.typecomputer.impl.TypeComputeUtils;
import org.apache.asterix.om.types.AOrderedListType;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.IAType;
import org.apache.asterix.runtime.evaluators.base.AbstractScalarFunctionDynamicDescriptor;
import org.apache.asterix.runtime.functions.FunctionTypeInferers;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import org.apache.hyracks.algebricks.runtime.base.IEvaluatorContext;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluator;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluatorFactory;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.api.IPointable;
import org.apache.hyracks.data.std.primitive.VoidPointable;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.apache.hyracks.dataflow.common.data.accessors.IFrameTupleReference;

@MissingNullInOutFunction
public class ToArrayDescriptor extends AbstractScalarFunctionDynamicDescriptor {
    public static final IFunctionDescriptorFactory FACTORY = new IFunctionDescriptorFactory() {
        @Override
        public IFunctionDescriptor createFunctionDescriptor() {
            return new ToArrayDescriptor();
        }

        @Override
        public IFunctionTypeInferer createFunctionTypeInferer() {
            return FunctionTypeInferers.SET_EXPRESSION_TYPE;
        }
    };

    private static final long serialVersionUID = 1L;
    private AOrderedListType oltype;

    @Override
    public void setImmutableStates(Object... states) {
        oltype = TypeComputeUtils.extractOrderedListType((IAType) states[0]);
    }

    @Override
    public IScalarEvaluatorFactory createEvaluatorFactory(final IScalarEvaluatorFactory[] args) {
        return new IScalarEvaluatorFactory() {
            private static final long serialVersionUID = 1L;

            @Override
            public IScalarEvaluator createScalarEvaluator(final IEvaluatorContext ctx) throws HyracksDataException {

                return new IScalarEvaluator() {

                    private final IScalarEvaluator eval0 = args[0].createScalarEvaluator(ctx);
                    private final IPointable arg0 = new VoidPointable();
                    private final OrderedListBuilder listBuilder = new OrderedListBuilder();
                    private final ArrayBackedValueStorage resultStorage = new ArrayBackedValueStorage();
                    private final DataOutput out = resultStorage.getDataOutput();

                    @Override
                    public void evaluate(IFrameTupleReference tuple, IPointable resultPointable)
                            throws HyracksDataException {
                        eval0.evaluate(tuple, arg0);

                        if (PointableHelper.checkAndSetMissingOrNull(resultPointable, arg0)) {
                            return;
                        }

                        byte[] data = arg0.getByteArray();
                        int offset = arg0.getStartOffset();
                        if (data[offset] == ATypeTag.SERIALIZED_ORDEREDLIST_TYPE_TAG) {
                            resultPointable.set(arg0);
                        } else if (data[offset] == ATypeTag.SERIALIZED_UNORDEREDLIST_TYPE_TAG) {
                            try {
                                resultStorage.reset();
                                out.writeByte(ATypeTag.SERIALIZED_ORDEREDLIST_TYPE_TAG);
                                out.write(data, offset + 1, arg0.getLength() - 1);
                                resultPointable.set(resultStorage);
                            } catch (IOException e) {
                                throw HyracksDataException.create(e);
                            }
                        } else {
                            resultStorage.reset();
                            listBuilder.reset(oltype);
                            listBuilder.addItem(arg0);
                            listBuilder.write(out, true);
                            resultPointable.set(resultStorage);
                        }
                    }
                };
            }
        };
    }

    @Override
    public FunctionIdentifier getIdentifier() {
        return BuiltinFunctions.TO_ARRAY;
    }
}
