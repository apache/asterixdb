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

import org.apache.asterix.common.annotations.MissingNullInOutFunction;
import org.apache.asterix.dataflow.data.nontagged.Coordinate;
import org.apache.asterix.dataflow.data.nontagged.serde.ADoubleSerializerDeserializer;
import org.apache.asterix.dataflow.data.nontagged.serde.APointSerializerDeserializer;
import org.apache.asterix.formats.nontagged.SerializerDeserializerProvider;
import org.apache.asterix.om.base.ACircle;
import org.apache.asterix.om.base.AMutableCircle;
import org.apache.asterix.om.base.AMutablePoint;
import org.apache.asterix.om.functions.BuiltinFunctions;
import org.apache.asterix.om.functions.IFunctionDescriptor;
import org.apache.asterix.om.functions.IFunctionDescriptorFactory;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.asterix.runtime.evaluators.base.AbstractScalarFunctionDynamicDescriptor;
import org.apache.asterix.runtime.exceptions.TypeMismatchException;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import org.apache.hyracks.algebricks.runtime.base.IEvaluatorContext;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluator;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluatorFactory;
import org.apache.hyracks.api.dataflow.value.ISerializerDeserializer;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.api.IPointable;
import org.apache.hyracks.data.std.primitive.VoidPointable;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.apache.hyracks.dataflow.common.data.accessors.IFrameTupleReference;

@MissingNullInOutFunction
public class CreateCircleDescriptor extends AbstractScalarFunctionDynamicDescriptor {

    private static final long serialVersionUID = 1L;

    public static final IFunctionDescriptorFactory FACTORY = new IFunctionDescriptorFactory() {
        @Override
        public IFunctionDescriptor createFunctionDescriptor() {
            return new CreateCircleDescriptor();
        }
    };

    @Override
    public IScalarEvaluatorFactory createEvaluatorFactory(final IScalarEvaluatorFactory[] args) {
        return new IScalarEvaluatorFactory() {
            private static final long serialVersionUID = 1L;

            @Override
            public IScalarEvaluator createScalarEvaluator(IEvaluatorContext ctx) throws HyracksDataException {
                return new IScalarEvaluator() {

                    private ArrayBackedValueStorage resultStorage = new ArrayBackedValueStorage();
                    private DataOutput out = resultStorage.getDataOutput();
                    private IPointable inputArg0 = new VoidPointable();
                    private IPointable inputArg1 = new VoidPointable();
                    private IScalarEvaluator eval0 = args[0].createScalarEvaluator(ctx);
                    private IScalarEvaluator eval1 = args[1].createScalarEvaluator(ctx);
                    private AMutablePoint aPoint = new AMutablePoint(0, 0);
                    private AMutableCircle aCircle = new AMutableCircle(null, 0);
                    @SuppressWarnings("unchecked")
                    private ISerializerDeserializer<ACircle> circleSerde =
                            SerializerDeserializerProvider.INSTANCE.getSerializerDeserializer(BuiltinType.ACIRCLE);

                    @Override
                    public void evaluate(IFrameTupleReference tuple, IPointable result) throws HyracksDataException {
                        resultStorage.reset();
                        eval0.evaluate(tuple, inputArg0);
                        eval1.evaluate(tuple, inputArg1);

                        if (PointableHelper.checkAndSetMissingOrNull(result, inputArg0, inputArg1)) {
                            return;
                        }

                        byte[] bytes0 = inputArg0.getByteArray();
                        int offset0 = inputArg0.getStartOffset();
                        byte[] bytes1 = inputArg1.getByteArray();
                        int offset1 = inputArg1.getStartOffset();

                        // Type check: (point, double)
                        if (bytes0[offset0] != ATypeTag.SERIALIZED_POINT_TYPE_TAG) {
                            throw new TypeMismatchException(sourceLoc, getIdentifier(), 0, bytes0[offset0],
                                    ATypeTag.SERIALIZED_POINT_TYPE_TAG);
                        }
                        if (bytes1[offset1] != ATypeTag.SERIALIZED_DOUBLE_TYPE_TAG) {
                            throw new TypeMismatchException(sourceLoc, getIdentifier(), 1, bytes1[offset1],
                                    ATypeTag.SERIALIZED_DOUBLE_TYPE_TAG);
                        }

                        try {
                            aPoint.setValue(
                                    ADoubleSerializerDeserializer.getDouble(bytes0,
                                            offset0 + APointSerializerDeserializer.getCoordinateOffset(Coordinate.X)),
                                    ADoubleSerializerDeserializer.getDouble(bytes0,
                                            offset0 + APointSerializerDeserializer.getCoordinateOffset(Coordinate.Y)));
                            aCircle.setValue(aPoint, ADoubleSerializerDeserializer.getDouble(bytes1, offset1 + 1));
                            circleSerde.serialize(aCircle, out);
                        } catch (IOException e1) {
                            throw HyracksDataException.create(e1);
                        }
                        result.set(resultStorage);
                    }
                };
            }
        };
    }

    @Override
    public FunctionIdentifier getIdentifier() {
        return BuiltinFunctions.CREATE_CIRCLE;
    }

}
