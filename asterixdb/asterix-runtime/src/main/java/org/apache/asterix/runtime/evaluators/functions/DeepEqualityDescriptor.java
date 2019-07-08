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

import org.apache.asterix.common.annotations.MissingNullInOutFunction;
import org.apache.asterix.formats.nontagged.SerializerDeserializerProvider;
import org.apache.asterix.om.base.ABoolean;
import org.apache.asterix.om.functions.BuiltinFunctions;
import org.apache.asterix.om.functions.IFunctionDescriptor;
import org.apache.asterix.om.functions.IFunctionDescriptorFactory;
import org.apache.asterix.om.functions.IFunctionTypeInferer;
import org.apache.asterix.om.pointables.PointableAllocator;
import org.apache.asterix.om.pointables.base.IVisitablePointable;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.asterix.om.types.IAType;
import org.apache.asterix.runtime.evaluators.base.AbstractScalarFunctionDynamicDescriptor;
import org.apache.asterix.runtime.evaluators.comparisons.DeepEqualAssessor;
import org.apache.asterix.runtime.functions.FunctionTypeInferers;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import org.apache.hyracks.algebricks.runtime.base.IEvaluatorContext;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluator;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluatorFactory;
import org.apache.hyracks.api.dataflow.value.ISerializerDeserializer;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.api.IPointable;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.apache.hyracks.dataflow.common.data.accessors.IFrameTupleReference;

@MissingNullInOutFunction
public class DeepEqualityDescriptor extends AbstractScalarFunctionDynamicDescriptor {

    public static final IFunctionDescriptorFactory FACTORY = new IFunctionDescriptorFactory() {
        @Override
        public IFunctionDescriptor createFunctionDescriptor() {
            return new DeepEqualityDescriptor();
        }

        @Override
        public IFunctionTypeInferer createFunctionTypeInferer() {
            return new FunctionTypeInferers.DeepEqualityTypeInferer();
        }
    };

    private static final long serialVersionUID = 1L;
    private IAType inputTypeLeft;
    private IAType inputTypeRight;

    @Override
    public void setImmutableStates(Object... states) {
        this.inputTypeLeft = (IAType) states[0];
        this.inputTypeRight = (IAType) states[1];
    }

    @Override
    public IScalarEvaluatorFactory createEvaluatorFactory(final IScalarEvaluatorFactory[] args) {
        final IScalarEvaluatorFactory evalFactoryLeft = args[0];
        final IScalarEvaluatorFactory evalFactoryRight = args[1];

        return new IScalarEvaluatorFactory() {
            private static final long serialVersionUID = 1L;
            @SuppressWarnings("unchecked")
            private final ISerializerDeserializer<ABoolean> boolSerde =
                    SerializerDeserializerProvider.INSTANCE.getSerializerDeserializer(BuiltinType.ABOOLEAN);

            @Override
            public IScalarEvaluator createScalarEvaluator(IEvaluatorContext ctx) throws HyracksDataException {
                final ArrayBackedValueStorage resultStorage = new ArrayBackedValueStorage();
                final DataOutput out = resultStorage.getDataOutput();
                final IScalarEvaluator evalLeft = evalFactoryLeft.createScalarEvaluator(ctx);
                final IScalarEvaluator evalRight = evalFactoryRight.createScalarEvaluator(ctx);

                return new IScalarEvaluator() {
                    private final DeepEqualAssessor deepEqualAssessor = new DeepEqualAssessor();
                    private final PointableAllocator allocator = new PointableAllocator();
                    private final IVisitablePointable pointableLeft = allocator.allocateFieldValue(inputTypeLeft);
                    private final IVisitablePointable pointableRight = allocator.allocateFieldValue(inputTypeRight);

                    @Override
                    public void evaluate(IFrameTupleReference tuple, IPointable result) throws HyracksDataException {
                        try {
                            evalLeft.evaluate(tuple, pointableLeft);
                            evalRight.evaluate(tuple, pointableRight);

                            if (PointableHelper.checkAndSetMissingOrNull(result, pointableLeft, pointableRight)) {
                                return;
                            }

                            // Using deep equality assessment to assess the equality of the two values
                            boolean isEqual = deepEqualAssessor.isEqual(pointableLeft, pointableRight);
                            ABoolean resultBit = isEqual ? ABoolean.TRUE : ABoolean.FALSE;

                            resultStorage.reset();
                            boolSerde.serialize(resultBit, out);
                            result.set(resultStorage);
                        } catch (Exception ioe) {
                            throw HyracksDataException.create(ioe);
                        }
                    }
                };
            }
        };
    }

    @Override
    public FunctionIdentifier getIdentifier() {
        return BuiltinFunctions.DEEP_EQUAL;
    }
}
