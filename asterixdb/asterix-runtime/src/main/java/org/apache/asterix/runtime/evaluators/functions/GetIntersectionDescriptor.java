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

import org.apache.asterix.dataflow.data.nontagged.Coordinate;
import org.apache.asterix.dataflow.data.nontagged.serde.ADoubleSerializerDeserializer;
import org.apache.asterix.dataflow.data.nontagged.serde.ARectangleSerializerDeserializer;
import org.apache.asterix.formats.nontagged.SerializerDeserializerProvider;
import org.apache.asterix.om.base.AMutablePoint;
import org.apache.asterix.om.base.AMutableRectangle;
import org.apache.asterix.om.base.ARectangle;
import org.apache.asterix.om.functions.BuiltinFunctions;
import org.apache.asterix.om.functions.IFunctionDescriptorFactory;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.asterix.om.types.EnumDeserializer;
import org.apache.asterix.runtime.evaluators.base.AbstractScalarFunctionDynamicDescriptor;
import org.apache.asterix.runtime.exceptions.TypeMismatchException;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import org.apache.hyracks.algebricks.runtime.base.IEvaluatorContext;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluator;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluatorFactory;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.value.ISerializerDeserializer;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.api.IPointable;
import org.apache.hyracks.data.std.primitive.VoidPointable;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.apache.hyracks.dataflow.common.data.accessors.IFrameTupleReference;

public class GetIntersectionDescriptor extends AbstractScalarFunctionDynamicDescriptor {
    private static final long serialVersionUID = 1L;

    public static final IFunctionDescriptorFactory FACTORY = () -> new GetIntersectionDescriptor();

    @Override
    public FunctionIdentifier getIdentifier() {
        return BuiltinFunctions.GET_INTERSECTION;
    }

    @Override
    public IScalarEvaluatorFactory createEvaluatorFactory(final IScalarEvaluatorFactory[] args) {
        return new IScalarEvaluatorFactory() {
            private static final long serialVersionUID = 1L;

            @Override
            public IScalarEvaluator createScalarEvaluator(final IEvaluatorContext ctx) throws HyracksDataException {
                final IHyracksTaskContext hyracksTaskContext = ctx.getTaskContext();

                return new IScalarEvaluator() {

                    private final ArrayBackedValueStorage resultStorage = new ArrayBackedValueStorage();
                    private final IPointable inputArg0 = new VoidPointable();
                    private final IPointable inputArg1 = new VoidPointable();
                    private final IScalarEvaluator eval0 = args[0].createScalarEvaluator(ctx);
                    private final IScalarEvaluator eval1 = args[1].createScalarEvaluator(ctx);

                    private final AMutableRectangle aRectangle =
                            new AMutableRectangle(new AMutablePoint(0.0, 0.0), new AMutablePoint(0.0, 0.0));
                    private final AMutablePoint[] aPoint = { new AMutablePoint(0, 0), new AMutablePoint(0, 0) };

                    @SuppressWarnings("unchecked")
                    private final ISerializerDeserializer<ARectangle> rectangleSerde =
                            SerializerDeserializerProvider.INSTANCE.getSerializerDeserializer(BuiltinType.ARECTANGLE);

                    @Override
                    public void evaluate(IFrameTupleReference tuple, IPointable result) throws HyracksDataException {
                        resultStorage.reset();

                        eval0.evaluate(tuple, inputArg0);
                        eval1.evaluate(tuple, inputArg1);

                        if (PointableHelper.checkAndSetMissingOrNull(result, inputArg0, inputArg1)) {
                            return;
                        }

                        byte[] bytes0 = inputArg0.getByteArray();
                        byte[] bytes1 = inputArg1.getByteArray();

                        int offset0 = inputArg0.getStartOffset();
                        int offset1 = inputArg1.getStartOffset();

                        ATypeTag tag0 = EnumDeserializer.ATYPETAGDESERIALIZER.deserialize(bytes0[offset0]);
                        ATypeTag tag1 = EnumDeserializer.ATYPETAGDESERIALIZER.deserialize(bytes1[offset1]);

                        if (tag0 != ATypeTag.RECTANGLE) {
                            throw new TypeMismatchException(sourceLoc, getIdentifier(), 0, bytes0[offset0],
                                    ATypeTag.SERIALIZED_RECTANGLE_TYPE_TAG);
                        }
                        if (tag1 != ATypeTag.RECTANGLE) {
                            throw new TypeMismatchException(sourceLoc, getIdentifier(), 1, bytes1[offset1],
                                    ATypeTag.SERIALIZED_RECTANGLE_TYPE_TAG);
                        }

                        double ax1 = ADoubleSerializerDeserializer.getDouble(bytes0, offset0 + 1
                                + ARectangleSerializerDeserializer.getBottomLeftCoordinateOffset(Coordinate.X));
                        double ay1 = ADoubleSerializerDeserializer.getDouble(bytes0, offset0 + 1
                                + ARectangleSerializerDeserializer.getBottomLeftCoordinateOffset(Coordinate.Y));

                        double ax2 = ADoubleSerializerDeserializer.getDouble(bytes0, offset0 + 1
                                + ARectangleSerializerDeserializer.getUpperRightCoordinateOffset(Coordinate.X));
                        double ay2 = ADoubleSerializerDeserializer.getDouble(bytes0, offset0 + 1
                                + ARectangleSerializerDeserializer.getUpperRightCoordinateOffset(Coordinate.Y));

                        double bx1 = ADoubleSerializerDeserializer.getDouble(bytes1, offset1 + 1
                                + ARectangleSerializerDeserializer.getBottomLeftCoordinateOffset(Coordinate.X));
                        double by1 = ADoubleSerializerDeserializer.getDouble(bytes1, offset1 + 1
                                + ARectangleSerializerDeserializer.getBottomLeftCoordinateOffset(Coordinate.Y));

                        double bx2 = ADoubleSerializerDeserializer.getDouble(bytes1, offset1 + 1
                                + ARectangleSerializerDeserializer.getUpperRightCoordinateOffset(Coordinate.X));
                        double by2 = ADoubleSerializerDeserializer.getDouble(bytes1, offset1 + 1
                                + ARectangleSerializerDeserializer.getUpperRightCoordinateOffset(Coordinate.Y));

                        // Bottom-left of the intersection rectangle
                        double ix1 = Math.max(ax1, bx1);
                        double iy1 = Math.max(ay1, by1);

                        // Top-right of the intersection rectangle
                        double ix2 = Math.min(ax2, bx2);
                        double iy2 = Math.min(ay2, by2);

                        // Update the intersection rectangle.
                        // If there is no intersection, return default rectangle [(0,0),(0,0)]
                        if ((ix1 < ix2) && (iy1 < iy2)) {
                            aPoint[0].setValue(ix1, iy1);
                            aPoint[1].setValue(ix2, iy2);
                        } else {
                            aPoint[0].setValue(0.0, 0.0);
                            aPoint[1].setValue(0.0, 0.0);
                        }
                        aRectangle.setValue(aPoint[0], aPoint[1]);
                        resultStorage.reset();
                        rectangleSerde.serialize(aRectangle, resultStorage.getDataOutput());
                        result.set(resultStorage);
                    }
                };
            }
        };
    }
}
