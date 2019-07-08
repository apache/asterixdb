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
import org.apache.asterix.om.functions.BuiltinFunctions;
import org.apache.asterix.om.functions.IFunctionDescriptor;
import org.apache.asterix.om.functions.IFunctionDescriptorFactory;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.runtime.evaluators.base.AbstractScalarFunctionDynamicDescriptor;
import org.apache.asterix.runtime.exceptions.TypeMismatchException;
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
public class SpatialDistanceDescriptor extends AbstractScalarFunctionDynamicDescriptor {

    private static final long serialVersionUID = 1L;
    public static final IFunctionDescriptorFactory FACTORY = new IFunctionDescriptorFactory() {
        @Override
        public IFunctionDescriptor createFunctionDescriptor() {
            return new SpatialDistanceDescriptor();
        }
    };

    @Override
    public IScalarEvaluatorFactory createEvaluatorFactory(final IScalarEvaluatorFactory[] args) {
        return new IScalarEvaluatorFactory() {
            private static final long serialVersionUID = 1L;

            @Override
            public IScalarEvaluator createScalarEvaluator(final IEvaluatorContext ctx) throws HyracksDataException {
                return new IScalarEvaluator() {

                    private final ArrayBackedValueStorage resultStorage = new ArrayBackedValueStorage();
                    private final DataOutput out = resultStorage.getDataOutput();
                    private final IPointable inputArg0 = new VoidPointable();
                    private final IPointable inputArg1 = new VoidPointable();
                    private final IScalarEvaluator eval0 = args[0].createScalarEvaluator(ctx);
                    private final IScalarEvaluator eval1 = args[1].createScalarEvaluator(ctx);

                    @Override
                    public void evaluate(IFrameTupleReference tuple, IPointable result) throws HyracksDataException {
                        resultStorage.reset();
                        eval0.evaluate(tuple, inputArg0);
                        eval1.evaluate(tuple, inputArg1);

                        if (PointableHelper.checkAndSetMissingOrNull(result, inputArg0, inputArg1)) {
                            return;
                        }

                        try {
                            byte[] bytes0 = inputArg0.getByteArray();
                            byte[] bytes1 = inputArg1.getByteArray();
                            int offset0 = inputArg0.getStartOffset();
                            int offset1 = inputArg1.getStartOffset();

                            byte tag0 = bytes0[offset0];
                            byte tag1 = bytes1[offset1];
                            double distance;
                            if (tag0 == ATypeTag.SERIALIZED_POINT_TYPE_TAG) {
                                if (tag1 == ATypeTag.SERIALIZED_POINT_TYPE_TAG) {
                                    double x1 = ADoubleSerializerDeserializer.getDouble(bytes0,
                                            offset0 + APointSerializerDeserializer.getCoordinateOffset(Coordinate.X));
                                    double y1 = ADoubleSerializerDeserializer.getDouble(bytes0,
                                            offset0 + APointSerializerDeserializer.getCoordinateOffset(Coordinate.Y));
                                    double x2 = ADoubleSerializerDeserializer.getDouble(bytes1,
                                            offset1 + APointSerializerDeserializer.getCoordinateOffset(Coordinate.X));
                                    double y2 = ADoubleSerializerDeserializer.getDouble(bytes1,
                                            offset1 + APointSerializerDeserializer.getCoordinateOffset(Coordinate.Y));
                                    distance = Math.sqrt(Math.pow(x2 - x1, 2) + Math.pow(y2 - y1, 2));
                                } else {
                                    throw new TypeMismatchException(sourceLoc, getIdentifier(), 1, bytes1[offset1],
                                            ATypeTag.SERIALIZED_POINT_TYPE_TAG);
                                }
                            } else {
                                throw new TypeMismatchException(sourceLoc, getIdentifier(), 0, bytes0[offset0],
                                        ATypeTag.SERIALIZED_POINT_TYPE_TAG);
                            }
                            out.writeByte(ATypeTag.SERIALIZED_DOUBLE_TYPE_TAG);
                            out.writeDouble(distance);
                        } catch (IOException e) {
                            throw HyracksDataException.create(e);
                        }
                        result.set(resultStorage);
                    }
                };
            }
        };
    }

    @Override
    public FunctionIdentifier getIdentifier() {
        return BuiltinFunctions.SPATIAL_DISTANCE;
    }

}
