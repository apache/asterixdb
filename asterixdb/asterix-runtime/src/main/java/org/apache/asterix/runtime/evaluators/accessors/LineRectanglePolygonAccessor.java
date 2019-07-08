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
package org.apache.asterix.runtime.evaluators.accessors;

import java.io.DataOutput;
import java.io.IOException;

import org.apache.asterix.builders.OrderedListBuilder;
import org.apache.asterix.common.annotations.MissingNullInOutFunction;
import org.apache.asterix.dataflow.data.nontagged.Coordinate;
import org.apache.asterix.dataflow.data.nontagged.serde.ADoubleSerializerDeserializer;
import org.apache.asterix.dataflow.data.nontagged.serde.AInt16SerializerDeserializer;
import org.apache.asterix.dataflow.data.nontagged.serde.ALineSerializerDeserializer;
import org.apache.asterix.dataflow.data.nontagged.serde.APolygonSerializerDeserializer;
import org.apache.asterix.dataflow.data.nontagged.serde.ARectangleSerializerDeserializer;
import org.apache.asterix.formats.nontagged.SerializerDeserializerProvider;
import org.apache.asterix.om.base.AMutablePoint;
import org.apache.asterix.om.base.APoint;
import org.apache.asterix.om.functions.BuiltinFunctions;
import org.apache.asterix.om.functions.IFunctionDescriptor;
import org.apache.asterix.om.functions.IFunctionDescriptorFactory;
import org.apache.asterix.om.types.AOrderedListType;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.asterix.runtime.evaluators.base.AbstractScalarFunctionDynamicDescriptor;
import org.apache.asterix.runtime.evaluators.functions.PointableHelper;
import org.apache.asterix.runtime.exceptions.InvalidDataFormatException;
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
public class LineRectanglePolygonAccessor extends AbstractScalarFunctionDynamicDescriptor {
    private static final long serialVersionUID = 1L;
    private static final FunctionIdentifier FID = BuiltinFunctions.GET_POINTS_LINE_RECTANGLE_POLYGON_ACCESSOR;
    public static final IFunctionDescriptorFactory FACTORY = new IFunctionDescriptorFactory() {

        @Override
        public IFunctionDescriptor createFunctionDescriptor() {
            return new LineRectanglePolygonAccessor();
        }
    };

    @Override
    public IScalarEvaluatorFactory createEvaluatorFactory(final IScalarEvaluatorFactory[] args) {
        return new IScalarEvaluatorFactory() {
            private static final long serialVersionUID = 1L;

            @Override
            public IScalarEvaluator createScalarEvaluator(IEvaluatorContext ctx) throws HyracksDataException {
                return new IScalarEvaluator() {
                    private final ArrayBackedValueStorage resultStorage = new ArrayBackedValueStorage();
                    private final DataOutput out = resultStorage.getDataOutput();
                    private final IPointable argPtr = new VoidPointable();
                    private final IScalarEvaluator eval = args[0].createScalarEvaluator(ctx);

                    private final OrderedListBuilder listBuilder = new OrderedListBuilder();
                    private final ArrayBackedValueStorage inputVal = new ArrayBackedValueStorage();
                    private final AOrderedListType pointListType = new AOrderedListType(BuiltinType.APOINT, null);
                    private final AMutablePoint aPoint = new AMutablePoint(0, 0);
                    @SuppressWarnings("unchecked")
                    private final ISerializerDeserializer<APoint> pointSerde =
                            SerializerDeserializerProvider.INSTANCE.getSerializerDeserializer(BuiltinType.APOINT);

                    @Override
                    public void evaluate(IFrameTupleReference tuple, IPointable result) throws HyracksDataException {
                        eval.evaluate(tuple, argPtr);

                        if (PointableHelper.checkAndSetMissingOrNull(result, argPtr)) {
                            return;
                        }

                        byte[] bytes = argPtr.getByteArray();
                        int startOffset = argPtr.getStartOffset();
                        resultStorage.reset();

                        try {
                            if (bytes[startOffset] == ATypeTag.SERIALIZED_LINE_TYPE_TAG) {
                                listBuilder.reset(pointListType);

                                inputVal.reset();
                                double startX = ADoubleSerializerDeserializer.getDouble(bytes, startOffset
                                        + ALineSerializerDeserializer.getStartPointCoordinateOffset(Coordinate.X));
                                double startY = ADoubleSerializerDeserializer.getDouble(bytes, startOffset
                                        + ALineSerializerDeserializer.getStartPointCoordinateOffset(Coordinate.Y));
                                aPoint.setValue(startX, startY);
                                pointSerde.serialize(aPoint, inputVal.getDataOutput());
                                listBuilder.addItem(inputVal);

                                inputVal.reset();
                                double endX = ADoubleSerializerDeserializer.getDouble(bytes, startOffset
                                        + ALineSerializerDeserializer.getEndPointCoordinateOffset(Coordinate.X));
                                double endY = ADoubleSerializerDeserializer.getDouble(bytes, startOffset
                                        + ALineSerializerDeserializer.getEndPointCoordinateOffset(Coordinate.Y));
                                aPoint.setValue(endX, endY);
                                pointSerde.serialize(aPoint, inputVal.getDataOutput());
                                listBuilder.addItem(inputVal);
                                listBuilder.write(out, true);
                            } else if (bytes[startOffset] == ATypeTag.SERIALIZED_RECTANGLE_TYPE_TAG) {
                                listBuilder.reset(pointListType);
                                inputVal.reset();
                                double x1 = ADoubleSerializerDeserializer.getDouble(bytes, startOffset
                                        + ARectangleSerializerDeserializer.getBottomLeftCoordinateOffset(Coordinate.X));
                                double y1 = ADoubleSerializerDeserializer.getDouble(bytes, startOffset
                                        + ARectangleSerializerDeserializer.getBottomLeftCoordinateOffset(Coordinate.Y));
                                aPoint.setValue(x1, y1);
                                pointSerde.serialize(aPoint, inputVal.getDataOutput());
                                listBuilder.addItem(inputVal);

                                inputVal.reset();
                                double x2 = ADoubleSerializerDeserializer.getDouble(bytes, startOffset
                                        + ARectangleSerializerDeserializer.getUpperRightCoordinateOffset(Coordinate.X));
                                double y2 = ADoubleSerializerDeserializer.getDouble(bytes, startOffset
                                        + ARectangleSerializerDeserializer.getUpperRightCoordinateOffset(Coordinate.Y));
                                aPoint.setValue(x2, y2);
                                pointSerde.serialize(aPoint, inputVal.getDataOutput());
                                listBuilder.addItem(inputVal);
                                listBuilder.write(out, true);
                            } else if (bytes[startOffset] == ATypeTag.SERIALIZED_POLYGON_TYPE_TAG) {
                                int numOfPoints = AInt16SerializerDeserializer.getShort(bytes,
                                        startOffset + APolygonSerializerDeserializer.getNumberOfPointsOffset());

                                if (numOfPoints < 3) {
                                    throw new InvalidDataFormatException(sourceLoc, getIdentifier(),
                                            ATypeTag.SERIALIZED_POLYGON_TYPE_TAG);
                                }
                                listBuilder.reset(pointListType);
                                for (int i = 0; i < numOfPoints; ++i) {
                                    inputVal.reset();
                                    double x = ADoubleSerializerDeserializer.getDouble(bytes, startOffset
                                            + APolygonSerializerDeserializer.getCoordinateOffset(i, Coordinate.X));
                                    double y = ADoubleSerializerDeserializer.getDouble(bytes, startOffset
                                            + APolygonSerializerDeserializer.getCoordinateOffset(i, Coordinate.Y));
                                    aPoint.setValue(x, y);
                                    pointSerde.serialize(aPoint, inputVal.getDataOutput());
                                    listBuilder.addItem(inputVal);
                                }
                                listBuilder.write(out, true);
                            } else {
                                throw new TypeMismatchException(sourceLoc, getIdentifier(), 0, bytes[startOffset],
                                        ATypeTag.SERIALIZED_LINE_TYPE_TAG, ATypeTag.SERIALIZED_RECTANGLE_TYPE_TAG,
                                        ATypeTag.SERIALIZED_POLYGON_TYPE_TAG);
                            }
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
        return FID;
    }

}
