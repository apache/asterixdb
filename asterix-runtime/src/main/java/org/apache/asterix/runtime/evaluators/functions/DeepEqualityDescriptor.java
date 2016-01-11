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

import org.apache.asterix.formats.nontagged.AqlSerializerDeserializerProvider;
import org.apache.asterix.om.base.ABoolean;
import org.apache.asterix.om.functions.AsterixBuiltinFunctions;
import org.apache.asterix.om.functions.IFunctionDescriptor;
import org.apache.asterix.om.functions.IFunctionDescriptorFactory;
import org.apache.asterix.om.pointables.PointableAllocator;
import org.apache.asterix.om.pointables.base.IVisitablePointable;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.asterix.om.types.IAType;
import org.apache.asterix.runtime.evaluators.base.AbstractScalarFunctionDynamicDescriptor;
import org.apache.asterix.runtime.evaluators.comparisons.DeepEqualAssessor;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import org.apache.hyracks.algebricks.runtime.base.ICopyEvaluator;
import org.apache.hyracks.algebricks.runtime.base.ICopyEvaluatorFactory;
import org.apache.hyracks.api.dataflow.value.ISerializerDeserializer;
import org.apache.hyracks.data.std.api.IDataOutputProvider;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.apache.hyracks.dataflow.common.data.accessors.IFrameTupleReference;

public class DeepEqualityDescriptor extends AbstractScalarFunctionDynamicDescriptor {
    public static final IFunctionDescriptorFactory FACTORY = new IFunctionDescriptorFactory() {
        public IFunctionDescriptor createFunctionDescriptor() {
            return new DeepEqualityDescriptor();
        }
    };

    private static final long serialVersionUID = 1L;
    private IAType inputTypeLeft;
    private IAType inputTypeRight;

    public void reset(IAType inTypeLeft, IAType inTypeRight) {
        this.inputTypeLeft = inTypeLeft;
        this.inputTypeRight = inTypeRight;
    }

    @Override
    public ICopyEvaluatorFactory createEvaluatorFactory(final ICopyEvaluatorFactory[] args) {
        final ICopyEvaluatorFactory evalFactoryLeft = args[0];
        final ICopyEvaluatorFactory evalFactoryRight = args[1];

        return new ICopyEvaluatorFactory() {
            private static final long serialVersionUID = 1L;
            private final ISerializerDeserializer boolSerde = AqlSerializerDeserializerProvider.INSTANCE
                    .getSerializerDeserializer(BuiltinType.ABOOLEAN);

            @Override
            public ICopyEvaluator createEvaluator(final IDataOutputProvider output) throws AlgebricksException {
                final DataOutput out = output.getDataOutput();
                final ArrayBackedValueStorage abvsLeft = new ArrayBackedValueStorage();
                final ICopyEvaluator evalLeft = evalFactoryLeft.createEvaluator(abvsLeft);

                final ArrayBackedValueStorage abvsRight = new ArrayBackedValueStorage();
                final ICopyEvaluator evalRight = evalFactoryRight.createEvaluator(abvsRight);
                final DeepEqualAssessor deepEqualAssessor = new DeepEqualAssessor();

                return new ICopyEvaluator() {
                    private final PointableAllocator allocator = new PointableAllocator();
                    private final IVisitablePointable pointableLeft = allocator.allocateFieldValue(inputTypeLeft);
                    private final IVisitablePointable pointableRight = allocator.allocateFieldValue(inputTypeRight);

                    @Override
                    public void evaluate(IFrameTupleReference tuple) throws AlgebricksException {
                        try {
                            abvsLeft.reset();
                            abvsRight.reset();
                            evalLeft.evaluate(tuple);
                            evalRight.evaluate(tuple);
                            pointableLeft.set(abvsLeft);
                            pointableRight.set(abvsRight);

                            // Using deep equality assessment to assess the equality of the two values
                            boolean isEqual = deepEqualAssessor.isEqual(pointableLeft, pointableRight);
                            ABoolean result = isEqual ? ABoolean.TRUE : ABoolean.FALSE;

                            boolSerde.serialize(result, out);
                        } catch (Exception ioe) {
                            throw new AlgebricksException(ioe);
                        }
                    }
                };
            }
        };
    }

    @Override
    public FunctionIdentifier getIdentifier() {
        return AsterixBuiltinFunctions.DEEP_EQUAL;
    }
}
