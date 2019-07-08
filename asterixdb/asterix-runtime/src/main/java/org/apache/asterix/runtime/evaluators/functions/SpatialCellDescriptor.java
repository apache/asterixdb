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
import org.apache.asterix.om.base.AMutablePoint;
import org.apache.asterix.om.base.AMutableRectangle;
import org.apache.asterix.om.base.ARectangle;
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
public class SpatialCellDescriptor extends AbstractScalarFunctionDynamicDescriptor {

    private static final long serialVersionUID = 1L;
    public static final IFunctionDescriptorFactory FACTORY = new IFunctionDescriptorFactory() {
        @Override
        public IFunctionDescriptor createFunctionDescriptor() {
            return new SpatialCellDescriptor();
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
                    private final IPointable inputArg2 = new VoidPointable();
                    private final IPointable inputArg3 = new VoidPointable();
                    private final IScalarEvaluator eval0 = args[0].createScalarEvaluator(ctx);
                    private final IScalarEvaluator eval1 = args[1].createScalarEvaluator(ctx);
                    private final IScalarEvaluator eval2 = args[2].createScalarEvaluator(ctx);
                    private final IScalarEvaluator eval3 = args[3].createScalarEvaluator(ctx);
                    private final AMutableRectangle aRectangle = new AMutableRectangle(null, null);
                    private final AMutablePoint[] aPoint = { new AMutablePoint(0, 0), new AMutablePoint(0, 0) };

                    @SuppressWarnings("unchecked")
                    private final ISerializerDeserializer<ARectangle> rectangleSerde =
                            SerializerDeserializerProvider.INSTANCE.getSerializerDeserializer(BuiltinType.ARECTANGLE);

                    @Override
                    public void evaluate(IFrameTupleReference tuple, IPointable result) throws HyracksDataException {
                        resultStorage.reset();
                        eval0.evaluate(tuple, inputArg0);
                        eval1.evaluate(tuple, inputArg1);
                        eval2.evaluate(tuple, inputArg2);
                        eval3.evaluate(tuple, inputArg3);

                        if (PointableHelper.checkAndSetMissingOrNull(result, inputArg0, inputArg1, inputArg2,
                                inputArg3)) {
                            return;
                        }

                        byte[] bytes0 = inputArg0.getByteArray();
                        byte[] bytes1 = inputArg1.getByteArray();
                        byte[] bytes2 = inputArg2.getByteArray();
                        byte[] bytes3 = inputArg3.getByteArray();
                        int offset0 = inputArg0.getStartOffset();
                        int offset1 = inputArg1.getStartOffset();
                        int offset2 = inputArg2.getStartOffset();
                        int offset3 = inputArg3.getStartOffset();

                        try {
                            byte tag0 = bytes0[offset0];
                            byte tag1 = bytes1[offset1];
                            byte tag2 = bytes2[offset2];
                            byte tag3 = bytes3[offset3];
                            if (tag0 == ATypeTag.SERIALIZED_POINT_TYPE_TAG && tag1 == ATypeTag.SERIALIZED_POINT_TYPE_TAG
                                    && tag2 == ATypeTag.SERIALIZED_DOUBLE_TYPE_TAG
                                    && tag3 == ATypeTag.SERIALIZED_DOUBLE_TYPE_TAG) {
                                double xLoc = ADoubleSerializerDeserializer.getDouble(bytes0,
                                        offset0 + APointSerializerDeserializer.getCoordinateOffset(Coordinate.X));
                                double yLoc = ADoubleSerializerDeserializer.getDouble(bytes0,
                                        offset0 + APointSerializerDeserializer.getCoordinateOffset(Coordinate.Y));

                                double xOrigin = ADoubleSerializerDeserializer.getDouble(bytes1,
                                        offset1 + APointSerializerDeserializer.getCoordinateOffset(Coordinate.X));
                                double yOrigin = ADoubleSerializerDeserializer.getDouble(bytes1,
                                        offset1 + APointSerializerDeserializer.getCoordinateOffset(Coordinate.Y));

                                double xInc = ADoubleSerializerDeserializer.getDouble(bytes2, offset2 + 1);
                                double yInc = ADoubleSerializerDeserializer.getDouble(bytes3, offset3 + 1);

                                double x = xOrigin + (Math.floor((xLoc - xOrigin) / xInc)) * xInc;
                                double y = yOrigin + (Math.floor((yLoc - yOrigin) / yInc)) * yInc;
                                aPoint[0].setValue(x, y);
                                aPoint[1].setValue(x + xInc, y + yInc);
                                aRectangle.setValue(aPoint[0], aPoint[1]);
                                rectangleSerde.serialize(aRectangle, out);
                            } else {
                                if (tag0 != ATypeTag.SERIALIZED_POINT_TYPE_TAG) {
                                    throw new TypeMismatchException(sourceLoc, getIdentifier(), 0, tag0,
                                            ATypeTag.SERIALIZED_POINT_TYPE_TAG);
                                }
                                if (tag1 != ATypeTag.SERIALIZED_POINT_TYPE_TAG) {
                                    throw new TypeMismatchException(sourceLoc, getIdentifier(), 1, tag1,
                                            ATypeTag.SERIALIZED_POINT_TYPE_TAG);
                                }
                                if (tag2 != ATypeTag.SERIALIZED_DOUBLE_TYPE_TAG) {
                                    throw new TypeMismatchException(sourceLoc, getIdentifier(), 2, tag2,
                                            ATypeTag.SERIALIZED_DOUBLE_TYPE_TAG);
                                }
                                if (tag3 != ATypeTag.SERIALIZED_DOUBLE_TYPE_TAG) {
                                    throw new TypeMismatchException(sourceLoc, getIdentifier(), 3, tag3,
                                            ATypeTag.SERIALIZED_DOUBLE_TYPE_TAG);
                                }
                            }
                            result.set(resultStorage);
                        } catch (IOException e1) {
                            throw HyracksDataException.create(e1);
                        }
                    }
                };
            }
        };
    }

    @Override
    public FunctionIdentifier getIdentifier() {
        return BuiltinFunctions.SPATIAL_CELL;
    }

}
