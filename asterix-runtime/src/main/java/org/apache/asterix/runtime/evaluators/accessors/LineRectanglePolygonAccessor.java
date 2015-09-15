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
import org.apache.asterix.dataflow.data.nontagged.Coordinate;
import org.apache.asterix.dataflow.data.nontagged.serde.ADoubleSerializerDeserializer;
import org.apache.asterix.dataflow.data.nontagged.serde.AInt16SerializerDeserializer;
import org.apache.asterix.dataflow.data.nontagged.serde.ALineSerializerDeserializer;
import org.apache.asterix.dataflow.data.nontagged.serde.APolygonSerializerDeserializer;
import org.apache.asterix.dataflow.data.nontagged.serde.ARectangleSerializerDeserializer;
import org.apache.asterix.formats.nontagged.AqlSerializerDeserializerProvider;
import org.apache.asterix.om.base.AMutablePoint;
import org.apache.asterix.om.base.ANull;
import org.apache.asterix.om.base.APoint;
import org.apache.asterix.om.functions.AsterixBuiltinFunctions;
import org.apache.asterix.om.functions.IFunctionDescriptor;
import org.apache.asterix.om.functions.IFunctionDescriptorFactory;
import org.apache.asterix.om.types.AOrderedListType;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.asterix.runtime.evaluators.base.AbstractScalarFunctionDynamicDescriptor;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import org.apache.hyracks.algebricks.runtime.base.ICopyEvaluator;
import org.apache.hyracks.algebricks.runtime.base.ICopyEvaluatorFactory;
import org.apache.hyracks.api.dataflow.value.ISerializerDeserializer;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.api.IDataOutputProvider;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.apache.hyracks.dataflow.common.data.accessors.IFrameTupleReference;

public class LineRectanglePolygonAccessor extends AbstractScalarFunctionDynamicDescriptor {

    private static final long serialVersionUID = 1L;

    private static final FunctionIdentifier FID = AsterixBuiltinFunctions.GET_POINTS_LINE_RECTANGLE_POLYGON_ACCESSOR;
    private static final byte SER_LINE_TAG = ATypeTag.LINE.serialize();
    private static final byte SER_RECTANGLE_TAG = ATypeTag.RECTANGLE.serialize();
    private static final byte SER_POLYGON_TAG = ATypeTag.POLYGON.serialize();
    private static final byte SER_NULL_TYPE_TAG = ATypeTag.NULL.serialize();

    public static final IFunctionDescriptorFactory FACTORY = new IFunctionDescriptorFactory() {

        @Override
        public IFunctionDescriptor createFunctionDescriptor() {
            return new LineRectanglePolygonAccessor();
        }
    };

    @Override
    public ICopyEvaluatorFactory createEvaluatorFactory(final ICopyEvaluatorFactory[] args) throws AlgebricksException {
        return new ICopyEvaluatorFactory() {

            private static final long serialVersionUID = 1L;

            @Override
            public ICopyEvaluator createEvaluator(final IDataOutputProvider output) throws AlgebricksException {
                return new ICopyEvaluator() {
                    private final DataOutput out = output.getDataOutput();
                    private final ArrayBackedValueStorage argOut = new ArrayBackedValueStorage();
                    private final ICopyEvaluator eval = args[0].createEvaluator(argOut);

                    private final OrderedListBuilder listBuilder = new OrderedListBuilder();
                    private final ArrayBackedValueStorage inputVal = new ArrayBackedValueStorage();
                    private final AOrderedListType pointListType = new AOrderedListType(BuiltinType.APOINT, null);

                    @SuppressWarnings("unchecked")
                    private final ISerializerDeserializer<ANull> nullSerde = AqlSerializerDeserializerProvider.INSTANCE
                            .getSerializerDeserializer(BuiltinType.ANULL);
                    private final AMutablePoint aPoint = new AMutablePoint(0, 0);
                    @SuppressWarnings("unchecked")
                    private final ISerializerDeserializer<APoint> pointSerde = AqlSerializerDeserializerProvider.INSTANCE
                            .getSerializerDeserializer(BuiltinType.APOINT);

                    @Override
                    public void evaluate(IFrameTupleReference tuple) throws AlgebricksException {
                        argOut.reset();
                        eval.evaluate(tuple);
                        byte[] bytes = argOut.getByteArray();

                        try {
                            if (bytes[0] == SER_LINE_TAG) {
                                listBuilder.reset(pointListType);

                                inputVal.reset();
                                double startX = ADoubleSerializerDeserializer.getDouble(bytes,
                                        ALineSerializerDeserializer.getStartPointCoordinateOffset(Coordinate.X));
                                double startY = ADoubleSerializerDeserializer.getDouble(bytes,
                                        ALineSerializerDeserializer.getStartPointCoordinateOffset(Coordinate.Y));
                                aPoint.setValue(startX, startY);
                                pointSerde.serialize(aPoint, inputVal.getDataOutput());
                                listBuilder.addItem(inputVal);

                                inputVal.reset();
                                double endX = ADoubleSerializerDeserializer.getDouble(bytes,
                                        ALineSerializerDeserializer.getEndPointCoordinateOffset(Coordinate.X));
                                double endY = ADoubleSerializerDeserializer.getDouble(bytes,
                                        ALineSerializerDeserializer.getEndPointCoordinateOffset(Coordinate.Y));
                                aPoint.setValue(endX, endY);
                                pointSerde.serialize(aPoint, inputVal.getDataOutput());
                                listBuilder.addItem(inputVal);
                                listBuilder.write(out, true);

                            } else if (bytes[0] == SER_RECTANGLE_TAG) {
                                listBuilder.reset(pointListType);

                                inputVal.reset();
                                double x1 = ADoubleSerializerDeserializer.getDouble(bytes,
                                        ARectangleSerializerDeserializer.getBottomLeftCoordinateOffset(Coordinate.X));
                                double y1 = ADoubleSerializerDeserializer.getDouble(bytes,
                                        ARectangleSerializerDeserializer.getBottomLeftCoordinateOffset(Coordinate.Y));
                                aPoint.setValue(x1, y1);
                                pointSerde.serialize(aPoint, inputVal.getDataOutput());
                                listBuilder.addItem(inputVal);

                                inputVal.reset();
                                double x2 = ADoubleSerializerDeserializer.getDouble(bytes,
                                        ARectangleSerializerDeserializer.getUpperRightCoordinateOffset(Coordinate.X));
                                double y2 = ADoubleSerializerDeserializer.getDouble(bytes,
                                        ARectangleSerializerDeserializer.getUpperRightCoordinateOffset(Coordinate.Y));
                                aPoint.setValue(x2, y2);
                                pointSerde.serialize(aPoint, inputVal.getDataOutput());
                                listBuilder.addItem(inputVal);
                                listBuilder.write(out, true);

                            } else if (bytes[0] == SER_POLYGON_TAG) {
                                int numOfPoints = AInt16SerializerDeserializer.getShort(bytes,
                                        APolygonSerializerDeserializer.getNumberOfPointsOffset());

                                if (numOfPoints < 3) {
                                    throw new HyracksDataException("Polygon must have at least 3 points.");
                                }
                                listBuilder.reset(pointListType);
                                for (int i = 0; i < numOfPoints; ++i) {
                                    inputVal.reset();
                                    double x = ADoubleSerializerDeserializer.getDouble(bytes,
                                            APolygonSerializerDeserializer.getCoordinateOffset(i, Coordinate.X));
                                    double y = ADoubleSerializerDeserializer.getDouble(bytes,
                                            APolygonSerializerDeserializer.getCoordinateOffset(i, Coordinate.Y));
                                    aPoint.setValue(x, y);
                                    pointSerde.serialize(aPoint, inputVal.getDataOutput());
                                    listBuilder.addItem(inputVal);
                                }
                                listBuilder.write(out, true);
                            } else if (bytes[0] == SER_NULL_TYPE_TAG) {
                                nullSerde.serialize(ANull.NULL, out);
                            } else {
                                throw new AlgebricksException("get-points does not support the type: " + bytes[0]
                                        + " It is only implemented for LINE, RECTANGLE, or POLYGON.");
                            }
                        } catch (IOException e) {
                            throw new AlgebricksException(e);
                        }
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