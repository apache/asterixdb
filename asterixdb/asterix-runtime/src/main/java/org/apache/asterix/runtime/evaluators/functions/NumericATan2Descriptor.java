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

import static org.apache.asterix.om.types.ATypeTag.VALUE_TYPE_MAPPING;
import static org.apache.asterix.runtime.evaluators.common.ArgumentUtils.NUMERIC_TYPES;

import java.io.DataOutput;

import org.apache.asterix.common.annotations.MissingNullInOutFunction;
import org.apache.asterix.formats.nontagged.SerializerDeserializerProvider;
import org.apache.asterix.om.base.AMutableDouble;
import org.apache.asterix.om.exceptions.ExceptionUtil;
import org.apache.asterix.om.functions.BuiltinFunctions;
import org.apache.asterix.om.functions.IFunctionDescriptorFactory;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.asterix.om.types.hierachy.ATypeHierarchy;
import org.apache.asterix.runtime.evaluators.base.AbstractScalarFunctionDynamicDescriptor;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluator;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluatorFactory;
import org.apache.hyracks.api.context.IEvaluatorContext;
import org.apache.hyracks.api.dataflow.value.ISerializerDeserializer;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.api.IPointable;
import org.apache.hyracks.data.std.primitive.VoidPointable;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.apache.hyracks.dataflow.common.data.accessors.IFrameTupleReference;

@MissingNullInOutFunction
public class NumericATan2Descriptor extends AbstractScalarFunctionDynamicDescriptor {

    private static final long serialVersionUID = 1L;
    public static final IFunctionDescriptorFactory FACTORY = NumericATan2Descriptor::new;

    @Override
    public FunctionIdentifier getIdentifier() {
        return BuiltinFunctions.NUMERIC_ATAN2;
    }

    @Override
    public IScalarEvaluatorFactory createEvaluatorFactory(final IScalarEvaluatorFactory[] args) {
        return new IScalarEvaluatorFactory() {
            private static final long serialVersionUID = 1L;

            @Override
            public IScalarEvaluator createScalarEvaluator(final IEvaluatorContext ctx) throws HyracksDataException {
                return new IScalarEvaluator() {
                    // For inputs.
                    private final IPointable leftPtr = new VoidPointable();
                    private final IPointable rightPtr = new VoidPointable();
                    private final IScalarEvaluator evalLeft = args[0].createScalarEvaluator(ctx);
                    private final IScalarEvaluator evalRight = args[1].createScalarEvaluator(ctx);
                    private double operand0;
                    private double operand1;

                    // For the output.
                    private final AMutableDouble aDouble = new AMutableDouble(0.0);
                    @SuppressWarnings("rawtypes")
                    private final ISerializerDeserializer outputSerde =
                            SerializerDeserializerProvider.INSTANCE.getSerializerDeserializer(BuiltinType.ADOUBLE);
                    private final ArrayBackedValueStorage resultStorage = new ArrayBackedValueStorage();
                    private final DataOutput out = resultStorage.getDataOutput();

                    @SuppressWarnings("unchecked")
                    @Override
                    public void evaluate(IFrameTupleReference tuple, IPointable result) throws HyracksDataException {
                        resultStorage.reset();
                        evalLeft.evaluate(tuple, leftPtr);
                        evalRight.evaluate(tuple, rightPtr);

                        if (PointableHelper.checkAndSetMissingOrNull(result, leftPtr, rightPtr)) {
                            return;
                        }

                        if (checkTypeAndWarn(leftPtr, 0) || checkTypeAndWarn(rightPtr, 1)) {
                            PointableHelper.setNull(result);
                            return;
                        }
                        String funName = getIdentifier().getName();
                        operand0 = ATypeHierarchy.getDoubleValue(funName, 0, leftPtr.getByteArray(),
                                leftPtr.getStartOffset());
                        operand1 = ATypeHierarchy.getDoubleValue(funName, 1, rightPtr.getByteArray(),
                                rightPtr.getStartOffset());
                        aDouble.setValue(Math.atan2(operand0, operand1));
                        outputSerde.serialize(aDouble, out);
                        result.set(resultStorage);
                    }

                    private boolean checkTypeAndWarn(IPointable argPtr, int argIdx) {
                        byte[] data = argPtr.getByteArray();
                        int offset = argPtr.getStartOffset();
                        byte type = data[offset];
                        if (ATypeHierarchy.getTypeDomain(VALUE_TYPE_MAPPING[type]) != ATypeHierarchy.Domain.NUMERIC) {
                            ExceptionUtil.warnTypeMismatch(ctx, sourceLoc, getIdentifier(), type, argIdx,
                                    NUMERIC_TYPES);
                            return true;
                        }
                        return false;
                    }
                };
            }
        };
    }
}
