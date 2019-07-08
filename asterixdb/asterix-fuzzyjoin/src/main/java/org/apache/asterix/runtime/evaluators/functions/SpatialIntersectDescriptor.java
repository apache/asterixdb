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
import org.apache.asterix.dataflow.data.nontagged.Coordinate;
import org.apache.asterix.dataflow.data.nontagged.serde.ACircleSerializerDeserializer;
import org.apache.asterix.dataflow.data.nontagged.serde.ADoubleSerializerDeserializer;
import org.apache.asterix.dataflow.data.nontagged.serde.AInt16SerializerDeserializer;
import org.apache.asterix.dataflow.data.nontagged.serde.ALineSerializerDeserializer;
import org.apache.asterix.dataflow.data.nontagged.serde.AObjectSerializerDeserializer;
import org.apache.asterix.dataflow.data.nontagged.serde.APointSerializerDeserializer;
import org.apache.asterix.dataflow.data.nontagged.serde.APolygonSerializerDeserializer;
import org.apache.asterix.dataflow.data.nontagged.serde.ARectangleSerializerDeserializer;
import org.apache.asterix.formats.nontagged.BinaryComparatorFactoryProvider;
import org.apache.asterix.fuzzyjoin.IntArray;
import org.apache.asterix.om.base.ABoolean;
import org.apache.asterix.om.functions.BuiltinFunctions;
import org.apache.asterix.om.functions.IFunctionDescriptor;
import org.apache.asterix.om.functions.IFunctionDescriptorFactory;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.EnumDeserializer;
import org.apache.asterix.runtime.evaluators.base.AbstractScalarFunctionDynamicDescriptor;
import org.apache.asterix.runtime.evaluators.common.DoubleArray;
import org.apache.asterix.runtime.evaluators.common.SpatialUtils;
import org.apache.asterix.runtime.exceptions.InvalidDataFormatException;
import org.apache.asterix.runtime.exceptions.TypeMismatchException;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import org.apache.hyracks.algebricks.runtime.base.IEvaluatorContext;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluator;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluatorFactory;
import org.apache.hyracks.api.dataflow.value.IBinaryComparator;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.api.IPointable;
import org.apache.hyracks.data.std.primitive.VoidPointable;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.apache.hyracks.dataflow.common.data.accessors.IFrameTupleReference;

@MissingNullInOutFunction
public class SpatialIntersectDescriptor extends AbstractScalarFunctionDynamicDescriptor {

    private static final long serialVersionUID = 1L;

    public static final IFunctionDescriptorFactory FACTORY = new IFunctionDescriptorFactory() {
        @Override
        public IFunctionDescriptor createFunctionDescriptor() {
            return new SpatialIntersectDescriptor();
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
                    private final IBinaryComparator ascDoubleComp =
                            BinaryComparatorFactoryProvider.DOUBLE_POINTABLE_INSTANCE.createBinaryComparator();
                    private final SpatialUtils spatialUtils = new SpatialUtils();
                    private final IntArray pointsOffsets0 = new IntArray();
                    private final IntArray pointsOffsets1 = new IntArray();
                    private final DoubleArray trianglesX0 = new DoubleArray();
                    private final DoubleArray trianglesY0 = new DoubleArray();
                    private final DoubleArray trianglesX1 = new DoubleArray();
                    private final DoubleArray trianglesY1 = new DoubleArray();
                    private final AObjectSerializerDeserializer aBooleanSerDer = AObjectSerializerDeserializer.INSTANCE;

                    private boolean pointOnLine(double pX, double pY, double startX, double startY, double endX,
                            double endY) throws HyracksDataException {
                        double crossProduct =
                                SpatialUtils.crossProduct(pY - startY, pX - startX, endY - startY, endX - startX);
                        if (Math.abs(crossProduct) > SpatialUtils.doubleEpsilon()) { // crossProduct != 0
                            return false;
                        }

                        double dotProduct =
                                SpatialUtils.dotProduct((pX - startX), (pY - startY), (endX - startX), (endY - startY));
                        if (dotProduct < 0.0) {
                            return false;
                        }

                        double squaredLengthBA = (endX - startX) * (endX - startX) + (endY - startY) * (endY - startY);
                        if (dotProduct > squaredLengthBA) {
                            return false;
                        }
                        return true;
                    }

                    private boolean pointInPolygon(byte[] bytes0, int offset0, byte[] bytes1, int offset1)
                            throws HyracksDataException { // ray casting

                        double pX = ADoubleSerializerDeserializer.getDouble(bytes0,
                                offset0 + APointSerializerDeserializer.getCoordinateOffset(Coordinate.X));
                        double pY = ADoubleSerializerDeserializer.getDouble(bytes0,
                                offset0 + APointSerializerDeserializer.getCoordinateOffset(Coordinate.Y));
                        int numOfPoints1 = AInt16SerializerDeserializer.getShort(bytes1,
                                offset1 + APolygonSerializerDeserializer.getNumberOfPointsOffset());

                        if (numOfPoints1 < 3) {
                            throw new InvalidDataFormatException(sourceLoc, getIdentifier(),
                                    ATypeTag.SERIALIZED_POLYGON_TYPE_TAG);
                        }

                        int counter = 0;
                        double xInters;
                        double x1, x2, y1, y2;
                        x1 = ADoubleSerializerDeserializer.getDouble(bytes1,
                                offset1 + APolygonSerializerDeserializer.getCoordinateOffset(0, Coordinate.X));
                        y1 = ADoubleSerializerDeserializer.getDouble(bytes1,
                                offset1 + APolygonSerializerDeserializer.getCoordinateOffset(0, Coordinate.Y));

                        for (int i = 1; i <= numOfPoints1; i++) {
                            if (i == numOfPoints1) {
                                x2 = ADoubleSerializerDeserializer.getDouble(bytes1,
                                        offset1 + APolygonSerializerDeserializer.getCoordinateOffset(0, Coordinate.X));
                                y2 = ADoubleSerializerDeserializer.getDouble(bytes1,
                                        offset1 + APolygonSerializerDeserializer.getCoordinateOffset(0, Coordinate.Y));
                            } else {
                                x2 = ADoubleSerializerDeserializer.getDouble(bytes1,
                                        offset1 + APolygonSerializerDeserializer.getCoordinateOffset(i, Coordinate.X));
                                y2 = ADoubleSerializerDeserializer.getDouble(bytes1,
                                        offset1 + APolygonSerializerDeserializer.getCoordinateOffset(i, Coordinate.Y));
                            }

                            if (!pointOnLine(pX, pY, x1, y1, x2, y2)) {
                                if (pY > Math.min(y1, y2)) {
                                    if (pY <= Math.max(y1, y2)) {
                                        if (pX <= Math.max(x1, x2)) {
                                            if (y1 != y2) {
                                                xInters = (pY - y1) * (x2 - x1) / (y2 - y1) + x1;
                                                if (x1 == x2 || pX <= xInters) {
                                                    counter++;
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                            x1 = x2;
                            y1 = y2;
                        }
                        if (counter % 2 == 1) {
                            return true;
                        } else {
                            return false;
                        }
                    }

                    private boolean pointInCircle(byte[] bytes0, int offset0, byte[] bytes1, int offset1)
                            throws HyracksDataException {
                        double x = ADoubleSerializerDeserializer.getDouble(bytes0,
                                offset0 + APointSerializerDeserializer.getCoordinateOffset(Coordinate.X));
                        double y = ADoubleSerializerDeserializer.getDouble(bytes0,
                                offset0 + APointSerializerDeserializer.getCoordinateOffset(Coordinate.Y));

                        double cX = ADoubleSerializerDeserializer.getDouble(bytes1,
                                offset1 + ACircleSerializerDeserializer.getCenterPointCoordinateOffset(Coordinate.X));
                        double cY = ADoubleSerializerDeserializer.getDouble(bytes1,
                                offset1 + ACircleSerializerDeserializer.getCenterPointCoordinateOffset(Coordinate.Y));
                        double radius = ADoubleSerializerDeserializer.getDouble(bytes1,
                                offset1 + ACircleSerializerDeserializer.getRadiusOffset());

                        if ((x - cX) * (x - cX) + (y - cY) * (y - cY) <= (radius * radius)) {
                            return true;
                        }
                        return false;
                    }

                    private boolean lineLineIntersection(double startX1, double startY1, double endX1, double endY1,
                            double startX2, double startY2, double endX2, double endY2) {
                        double A1 = endY1 - startY1;
                        double B1 = startX1 - endX1;
                        double C1 = A1 * startX1 + B1 * startY1;

                        double A2 = endY2 - startY2;
                        double B2 = startX2 - endX2;
                        double C2 = A2 * startX2 + B2 * startY2;

                        double det = (A1 * B2) - (A2 * B1);
                        if (Math.abs(det) > SpatialUtils.doubleEpsilon()) { // det != 0
                            double x = (B2 * C1 - B1 * C2) / det;
                            double y = (A1 * C2 - A2 * C1) / det;

                            if ((x >= Math.min(startX1, endX1) && x <= Math.max(startX1, endX1))
                                    && (y >= Math.min(startY1, endY1) && y <= Math.max(startY1, endY1))) {
                                if ((x >= Math.min(startX2, endX2) && x <= Math.max(startX2, endX2))
                                        && (y >= Math.min(startY2, endY2) && y <= Math.max(startY2, endY2))) {
                                    return true;
                                }
                            }
                        }
                        return false;
                    }

                    private boolean linePolygonIntersection(byte[] bytes0, int offset0, byte[] bytes1, int offset1)
                            throws HyracksDataException {
                        double startX1 = ADoubleSerializerDeserializer.getDouble(bytes0,
                                offset0 + ALineSerializerDeserializer.getStartPointCoordinateOffset(Coordinate.X));
                        double startY1 = ADoubleSerializerDeserializer.getDouble(bytes0,
                                offset0 + ALineSerializerDeserializer.getStartPointCoordinateOffset(Coordinate.Y));
                        double endX1 = ADoubleSerializerDeserializer.getDouble(bytes0,
                                offset0 + ALineSerializerDeserializer.getEndPointCoordinateOffset(Coordinate.X));
                        double endY1 = ADoubleSerializerDeserializer.getDouble(bytes0,
                                offset0 + ALineSerializerDeserializer.getEndPointCoordinateOffset(Coordinate.Y));

                        int numOfPoints1 = AInt16SerializerDeserializer.getShort(bytes1,
                                offset1 + APolygonSerializerDeserializer.getNumberOfPointsOffset());

                        if (numOfPoints1 < 3) {
                            throw new InvalidDataFormatException(sourceLoc, getIdentifier(),
                                    ATypeTag.SERIALIZED_POLYGON_TYPE_TAG);
                        }
                        for (int i = 0; i < numOfPoints1; i++) {
                            double startX2 = ADoubleSerializerDeserializer.getDouble(bytes1,
                                    offset1 + APolygonSerializerDeserializer.getCoordinateOffset(i, Coordinate.X));
                            double startY2 = ADoubleSerializerDeserializer.getDouble(bytes1,
                                    offset1 + APolygonSerializerDeserializer.getCoordinateOffset(i, Coordinate.Y));

                            double endX2;
                            double endY2;
                            if (i + 1 == numOfPoints1) {
                                endX2 = ADoubleSerializerDeserializer.getDouble(bytes1,
                                        offset1 + APolygonSerializerDeserializer.getCoordinateOffset(0, Coordinate.X));
                                endY2 = ADoubleSerializerDeserializer.getDouble(bytes1,
                                        offset1 + APolygonSerializerDeserializer.getCoordinateOffset(0, Coordinate.Y));
                            } else {
                                endX2 = ADoubleSerializerDeserializer.getDouble(bytes1, offset1
                                        + APolygonSerializerDeserializer.getCoordinateOffset(i + 1, Coordinate.X));
                                endY2 = ADoubleSerializerDeserializer.getDouble(bytes1, offset1
                                        + APolygonSerializerDeserializer.getCoordinateOffset(i + 1, Coordinate.Y));
                            }

                            boolean intersect = lineLineIntersection(startX1, startY1, endX1, endY1, startX2, startY2,
                                    endX2, endY2);
                            if (intersect) {
                                return true;
                            }
                        }
                        return false;
                    }

                    private boolean lineRectangleIntersection(byte[] bytes0, int offset0, byte[] bytes1, int offset1)
                            throws HyracksDataException {
                        double startX1 = ADoubleSerializerDeserializer.getDouble(bytes0,
                                offset0 + ALineSerializerDeserializer.getStartPointCoordinateOffset(Coordinate.X));
                        double startY1 = ADoubleSerializerDeserializer.getDouble(bytes0,
                                offset0 + ALineSerializerDeserializer.getStartPointCoordinateOffset(Coordinate.Y));
                        double endX1 = ADoubleSerializerDeserializer.getDouble(bytes0,
                                offset0 + ALineSerializerDeserializer.getEndPointCoordinateOffset(Coordinate.X));
                        double endY1 = ADoubleSerializerDeserializer.getDouble(bytes0,
                                offset0 + ALineSerializerDeserializer.getEndPointCoordinateOffset(Coordinate.Y));

                        double x1 = ADoubleSerializerDeserializer.getDouble(bytes1,
                                offset1 + ARectangleSerializerDeserializer.getBottomLeftCoordinateOffset(Coordinate.X));
                        double y1 = ADoubleSerializerDeserializer.getDouble(bytes1,
                                offset1 + ARectangleSerializerDeserializer.getBottomLeftCoordinateOffset(Coordinate.Y));

                        double x2 = ADoubleSerializerDeserializer.getDouble(bytes1,
                                offset1 + ARectangleSerializerDeserializer.getUpperRightCoordinateOffset(Coordinate.X));
                        double y2 = ADoubleSerializerDeserializer.getDouble(bytes1,
                                offset1 + ARectangleSerializerDeserializer.getUpperRightCoordinateOffset(Coordinate.Y));

                        if (lineLineIntersection(startX1, startY1, endX1, endY1, x1, y1, x1, y2)
                                || lineLineIntersection(startX1, startY1, endX1, endY1, x1, y2, x2, y2)
                                || lineLineIntersection(startX1, startY1, endX1, endY1, x2, y2, x2, y1)
                                || lineLineIntersection(startX1, startY1, endX1, endY1, x2, y1, x1, y1)) {
                            return true;
                        }
                        return false;

                    }

                    private boolean lineCircleIntersection(byte[] bytes0, int offset0, byte[] bytes1, int offset1)
                            throws HyracksDataException {
                        double startX = ADoubleSerializerDeserializer.getDouble(bytes0,
                                offset0 + ALineSerializerDeserializer.getStartPointCoordinateOffset(Coordinate.X));
                        double startY = ADoubleSerializerDeserializer.getDouble(bytes0,
                                offset0 + ALineSerializerDeserializer.getStartPointCoordinateOffset(Coordinate.Y));
                        double endX = ADoubleSerializerDeserializer.getDouble(bytes0,
                                offset0 + ALineSerializerDeserializer.getEndPointCoordinateOffset(Coordinate.X));
                        double endY = ADoubleSerializerDeserializer.getDouble(bytes0,
                                offset0 + ALineSerializerDeserializer.getEndPointCoordinateOffset(Coordinate.Y));

                        double cX = ADoubleSerializerDeserializer.getDouble(bytes1,
                                offset1 + ACircleSerializerDeserializer.getCenterPointCoordinateOffset(Coordinate.X));
                        double cY = ADoubleSerializerDeserializer.getDouble(bytes1,
                                offset1 + ACircleSerializerDeserializer.getCenterPointCoordinateOffset(Coordinate.Y));
                        double radius = ADoubleSerializerDeserializer.getDouble(bytes1,
                                offset1 + ACircleSerializerDeserializer.getRadiusOffset());

                        double dx = endX - startX;
                        double dy = endY - startY;
                        double t = -((startX - cX) * dx + (startY - cY) * dy) / ((dx * dx) + (dy * dy));

                        if (t < 0.0) {
                            t = 0.0;
                        } else if (t > 1.0) {
                            t = 1.0;
                        }

                        dx = (startX + t * (endX - startX)) - cX;
                        dy = (startY + t * (endY - startY)) - cY;
                        double rt = (dx * dx) + (dy * dy);
                        if (rt <= (radius * radius)) {
                            return true;
                        }
                        return false;
                    }

                    private boolean findEar(byte[] bytes, int offset, int u, int v, int w, int n,
                            IntArray pointsOffsets) throws HyracksDataException {
                        int p;
                        double Ax, Ay, Bx, By, Cx, Cy, Px, Py;

                        Ax = ADoubleSerializerDeserializer.getDouble(bytes, offset + APolygonSerializerDeserializer
                                .getCoordinateOffset(pointsOffsets.get(u), Coordinate.X));
                        Ay = ADoubleSerializerDeserializer.getDouble(bytes, offset + APolygonSerializerDeserializer
                                .getCoordinateOffset(pointsOffsets.get(u), Coordinate.Y));

                        Bx = ADoubleSerializerDeserializer.getDouble(bytes, offset + APolygonSerializerDeserializer
                                .getCoordinateOffset(pointsOffsets.get(v), Coordinate.X));
                        By = ADoubleSerializerDeserializer.getDouble(bytes, offset + APolygonSerializerDeserializer
                                .getCoordinateOffset(pointsOffsets.get(v), Coordinate.Y));

                        Cx = ADoubleSerializerDeserializer.getDouble(bytes, offset + APolygonSerializerDeserializer
                                .getCoordinateOffset(pointsOffsets.get(w), Coordinate.X));
                        Cy = ADoubleSerializerDeserializer.getDouble(bytes, offset + APolygonSerializerDeserializer
                                .getCoordinateOffset(pointsOffsets.get(w), Coordinate.Y));

                        if (SpatialUtils.doubleEpsilon() > (((Bx - Ax) * (Cy - Ay)) - ((By - Ay) * (Cx - Ax)))) {

                            return false;
                        }

                        for (p = 0; p < n; p++) {
                            if ((p == u) || (p == v) || (p == w)) {
                                continue;
                            }
                            Px = ADoubleSerializerDeserializer.getDouble(bytes, offset + APolygonSerializerDeserializer
                                    .getCoordinateOffset(pointsOffsets.get(p), Coordinate.X));
                            Py = ADoubleSerializerDeserializer.getDouble(bytes, offset + APolygonSerializerDeserializer
                                    .getCoordinateOffset(pointsOffsets.get(p), Coordinate.Y));
                            if (pointInsideTriangle(Ax, Ay, Bx, By, Cx, Cy, Px, Py)) {
                                return false;
                            }
                        }

                        return true;
                    }

                    private int triangulatePolygon(byte[] bytes, int offset, int numOfPoints, IntArray pointsOffsets,
                            DoubleArray trianglesX, DoubleArray trianglesY, int triangleId,
                            int nonSimplePolygonDetection, int middleVertex) throws HyracksDataException { // Ear clipping

                        if (numOfPoints < 3) {
                            return -1;
                        }

                        boolean foundEar = false;
                        int v = middleVertex;
                        while (!foundEar) {
                            if (0 >= (nonSimplePolygonDetection--)) {
                                throw new InvalidDataFormatException(sourceLoc, getIdentifier(),
                                        ATypeTag.SERIALIZED_POLYGON_TYPE_TAG);
                            }
                            int u = v;
                            if (numOfPoints <= u) {
                                u = 0;
                            }
                            v = u + 1;
                            if (numOfPoints <= v) {
                                v = 0;
                            }
                            int w = v + 1;
                            if (numOfPoints <= w) {
                                w = 0;
                            }

                            if (findEar(bytes, offset, u, v, w, numOfPoints, pointsOffsets)) {
                                int s, t;

                                addRectangle(trianglesX, trianglesY);

                                SpatialUtils.setTriangleXCoordinate(trianglesX, triangleId, 0,
                                        ADoubleSerializerDeserializer.getDouble(bytes,
                                                offset + APolygonSerializerDeserializer
                                                        .getCoordinateOffset(pointsOffsets.get(u), Coordinate.X)));

                                SpatialUtils.setTriangleYCoordinate(trianglesY, triangleId, 0,
                                        ADoubleSerializerDeserializer.getDouble(bytes,
                                                offset + APolygonSerializerDeserializer
                                                        .getCoordinateOffset(pointsOffsets.get(u), Coordinate.Y)));

                                SpatialUtils.setTriangleXCoordinate(trianglesX, triangleId, 1,
                                        ADoubleSerializerDeserializer.getDouble(bytes,
                                                offset + APolygonSerializerDeserializer
                                                        .getCoordinateOffset(pointsOffsets.get(v), Coordinate.X)));

                                SpatialUtils.setTriangleYCoordinate(trianglesY, triangleId, 1,
                                        ADoubleSerializerDeserializer.getDouble(bytes,
                                                offset + APolygonSerializerDeserializer
                                                        .getCoordinateOffset(pointsOffsets.get(v), Coordinate.Y)));

                                SpatialUtils.setTriangleXCoordinate(trianglesX, triangleId, 2,
                                        ADoubleSerializerDeserializer.getDouble(bytes,
                                                offset + APolygonSerializerDeserializer
                                                        .getCoordinateOffset(pointsOffsets.get(w), Coordinate.X)));

                                SpatialUtils.setTriangleYCoordinate(trianglesY, triangleId, 2,
                                        ADoubleSerializerDeserializer.getDouble(bytes,
                                                offset + APolygonSerializerDeserializer
                                                        .getCoordinateOffset(pointsOffsets.get(w), Coordinate.Y)));

                                // remove v from polygon
                                for (s = v, t = v + 1; t < numOfPoints; s++, t++) {
                                    pointsOffsets.get()[s] = pointsOffsets.get(t);
                                }
                                foundEar = true;
                            }
                        }

                        return v;
                    }

                    private boolean triangleTriangleIntersection(DoubleArray trianglesX0, DoubleArray trianglesY0,
                            int triangleId0, DoubleArray trianglesX1, DoubleArray trianglesY1, int triangleId1)
                            throws HyracksDataException { // separating axis theorem

                        for (int side = 0; side < 3; side++) {
                            spatialUtils.findNormals(trianglesX0, trianglesY0, triangleId0, side);
                            spatialUtils.projectPolygon(trianglesX0, trianglesY0, triangleId0, spatialUtils.getXAxis(),
                                    spatialUtils.getYAxis());
                            double min1 = spatialUtils.getMinProjection();
                            double max1 = spatialUtils.getMaxProjection();
                            spatialUtils.projectPolygon(trianglesX1, trianglesY1, triangleId1, spatialUtils.getXAxis(),
                                    spatialUtils.getYAxis());
                            double min2 = spatialUtils.getMinProjection();
                            double max2 = spatialUtils.getMaxProjection();

                            if (max1 < min2 || min1 > max2) {
                                return false;
                            }
                        }
                        return true;
                    }

                    private boolean pointInsideTriangle(double x1, double y1, double x2, double y2, double x3,
                            double y3, double pX, double pY) {
                        return pointsOnSameSide(pX, pY, x1, y1, x2, y2, x3, y3)
                                && pointsOnSameSide(pX, pY, x2, y2, x1, y1, x3, y3)
                                && pointsOnSameSide(pX, pY, x3, y3, x1, y1, x2, y2);
                    }

                    private boolean pointsOnSameSide(double pX, double pY, double x1, double y1, double x2, double y2,
                            double x3, double y3) {
                        double cp1 = SpatialUtils.crossProduct(x3 - x2, y3 - y2, pX - x2, pY - y2);
                        double cp2 = SpatialUtils.crossProduct(x3 - x2, y3 - y2, x1 - x2, y1 - y2);
                        return (cp1 * cp2) >= 0.0;
                    }

                    private boolean circleTriangleIntersection(byte[] bytes0, int offset0, DoubleArray trianglesX,
                            DoubleArray trianglesY, int triangleId) throws HyracksDataException { // separating axis theorem

                        double cX = ADoubleSerializerDeserializer.getDouble(bytes0,
                                offset0 + ACircleSerializerDeserializer.getCenterPointCoordinateOffset(Coordinate.X));
                        double cY = ADoubleSerializerDeserializer.getDouble(bytes0,
                                offset0 + ACircleSerializerDeserializer.getCenterPointCoordinateOffset(Coordinate.Y));
                        double radius = ADoubleSerializerDeserializer.getDouble(bytes0,
                                offset0 + ACircleSerializerDeserializer.getRadiusOffset());

                        double distance = Double.MAX_VALUE;
                        double distanceSquared;

                        double temp;
                        double closestPointX = 0.0;
                        double closestPointY = 0.0;
                        for (int i = 0; i < 3; i++) {
                            double pX = SpatialUtils.getTriangleXCoordinate(trianglesX, triangleId, i);
                            double pY = SpatialUtils.getTriangleXCoordinate(trianglesY, triangleId, i);

                            distanceSquared = (cX - pX) * (cX - pX) + (cY - pY) * (cY - pY);
                            if (distanceSquared < distance) {
                                distance = distanceSquared;
                                closestPointX = pX;
                                closestPointY = pY;
                            }
                        }

                        double x = Math.abs(cX - closestPointX);
                        double y = Math.abs(cY - closestPointY);

                        temp = Math.sqrt(SpatialUtils.dotProduct(x, y, x, y));
                        x /= temp;
                        y /= temp;

                        spatialUtils.projectPolygon(trianglesX, trianglesY, triangleId, x, y);

                        double min1 = spatialUtils.getMinProjection();
                        double max1 = spatialUtils.getMaxProjection();

                        double dotProduct = SpatialUtils.dotProduct(x, y, cX, cY);
                        double max2 = dotProduct + radius;
                        double min2 = dotProduct - radius;

                        if (max1 < min2 || min1 > max2) {
                            return false;
                        }

                        for (int side = 0; side < 3; side++) {
                            spatialUtils.findNormals(trianglesX, trianglesY, triangleId, side);
                            spatialUtils.projectPolygon(trianglesX, trianglesY, triangleId, spatialUtils.getXAxis(),
                                    spatialUtils.getYAxis());
                            min1 = spatialUtils.getMinProjection();
                            max1 = spatialUtils.getMaxProjection();

                            dotProduct =
                                    SpatialUtils.dotProduct(spatialUtils.getXAxis(), spatialUtils.getYAxis(), cX, cY);
                            max2 = dotProduct + radius;
                            min2 = dotProduct - radius;

                            if (max1 < min2 || min1 > max2) {
                                return false;
                            }
                        }
                        return true;
                    }

                    private boolean circleCircleIntersection(byte[] bytes0, int offset0, byte[] bytes1, int offset1)
                            throws HyracksDataException {
                        double cX0 = ADoubleSerializerDeserializer.getDouble(bytes0,
                                offset0 + ACircleSerializerDeserializer.getCenterPointCoordinateOffset(Coordinate.X));
                        double cY0 = ADoubleSerializerDeserializer.getDouble(bytes0,
                                offset0 + ACircleSerializerDeserializer.getCenterPointCoordinateOffset(Coordinate.Y));
                        double radius0 = ADoubleSerializerDeserializer.getDouble(bytes0,
                                offset0 + ACircleSerializerDeserializer.getRadiusOffset());

                        double cX1 = ADoubleSerializerDeserializer.getDouble(bytes1,
                                offset1 + ACircleSerializerDeserializer.getCenterPointCoordinateOffset(Coordinate.X));
                        double cY1 = ADoubleSerializerDeserializer.getDouble(bytes1,
                                offset1 + ACircleSerializerDeserializer.getCenterPointCoordinateOffset(Coordinate.Y));
                        double radius1 = ADoubleSerializerDeserializer.getDouble(bytes1,
                                offset1 + ACircleSerializerDeserializer.getRadiusOffset());

                        double distanceSquared = SpatialUtils.dotProduct(cX0 - cX1, cY0 - cY1, cX0 - cX1, cY0 - cY1);
                        double radiusDistanceSquared = (radius0 + radius1) * (radius0 + radius1);
                        if (distanceSquared <= radiusDistanceSquared) {
                            return true;
                        }
                        return false;
                    }

                    private void getCounterClockWisePolygon(byte[] bytes, int offset, IntArray pointsOffsets,
                            int numOfPoints) throws HyracksDataException {
                        pointsOffsets.reset();
                        if (SpatialUtils.polygonArea(bytes, offset, numOfPoints) > 0.0) {
                            for (int i = 0; i < numOfPoints; i++) {
                                pointsOffsets.add(i);
                            }
                        } else {
                            for (int i = 0; i < numOfPoints; i++) {
                                pointsOffsets.add((numOfPoints - 1) - i);
                            }
                        }
                    }

                    private boolean pointInRectangle(byte[] bytes0, int offset0, byte[] bytes1, int offset1)
                            throws HyracksDataException {

                        double pX = ADoubleSerializerDeserializer.getDouble(bytes0,
                                offset0 + APointSerializerDeserializer.getCoordinateOffset(Coordinate.X));
                        double pY = ADoubleSerializerDeserializer.getDouble(bytes0,
                                offset0 + APointSerializerDeserializer.getCoordinateOffset(Coordinate.Y));

                        double x1 = ADoubleSerializerDeserializer.getDouble(bytes1,
                                offset1 + ARectangleSerializerDeserializer.getBottomLeftCoordinateOffset(Coordinate.X));
                        double y1 = ADoubleSerializerDeserializer.getDouble(bytes1,
                                offset1 + ARectangleSerializerDeserializer.getBottomLeftCoordinateOffset(Coordinate.Y));

                        double x2 = ADoubleSerializerDeserializer.getDouble(bytes1,
                                offset1 + ARectangleSerializerDeserializer.getUpperRightCoordinateOffset(Coordinate.X));
                        double y2 = ADoubleSerializerDeserializer.getDouble(bytes1,
                                offset1 + ARectangleSerializerDeserializer.getUpperRightCoordinateOffset(Coordinate.Y));

                        if (pointInsideTriangle(x1, y1, x1, y2, x2, y2, pX, pY)
                                || pointInsideTriangle(x1, y1, x2, y1, x2, y2, pX, pY)) {
                            return true;
                        }
                        return false;

                    }

                    private void addRectangle(DoubleArray trianglesX, DoubleArray trianglesY) {
                        for (int i = 0; i < 3; i++) {
                            double temp = 0;
                            trianglesX.add(temp);
                            trianglesY.add(temp);
                        }
                    }

                    private boolean rectangleCircleIntersection(byte[] bytes0, int offset0, byte[] bytes1, int offset1)
                            throws HyracksDataException {
                        triangulateRectangle(bytes0, offset0, trianglesX0, trianglesY0);
                        boolean res = false;
                        // 2 triangles in a rectangle
                        for (int i = 0; i < 2; i++) {
                            res = circleTriangleIntersection(bytes1, offset1, trianglesX0, trianglesY0, i);
                            if (res) {
                                break;
                            }
                        }
                        return res;
                    }

                    private void triangulateRectangle(byte[] bytes, int offset, DoubleArray trianglesX,
                            DoubleArray trianglesY) throws HyracksDataException {
                        double x1 = ADoubleSerializerDeserializer.getDouble(bytes,
                                offset + ARectangleSerializerDeserializer.getBottomLeftCoordinateOffset(Coordinate.X));
                        double y1 = ADoubleSerializerDeserializer.getDouble(bytes,
                                offset + ARectangleSerializerDeserializer.getBottomLeftCoordinateOffset(Coordinate.Y));

                        double x2 = ADoubleSerializerDeserializer.getDouble(bytes,
                                offset + ARectangleSerializerDeserializer.getUpperRightCoordinateOffset(Coordinate.X));
                        double y2 = ADoubleSerializerDeserializer.getDouble(bytes,
                                offset + ARectangleSerializerDeserializer.getUpperRightCoordinateOffset(Coordinate.Y));
                        trianglesX.reset();
                        trianglesY.reset();

                        addRectangle(trianglesX, trianglesY);
                        addRectangle(trianglesX, trianglesY);

                        SpatialUtils.setTriangleXCoordinate(trianglesX, 0, 0, x1);
                        SpatialUtils.setTriangleYCoordinate(trianglesY, 0, 0, y1);

                        SpatialUtils.setTriangleXCoordinate(trianglesX, 0, 1, x2);
                        SpatialUtils.setTriangleYCoordinate(trianglesY, 0, 1, y1);

                        SpatialUtils.setTriangleXCoordinate(trianglesX, 0, 2, x2);
                        SpatialUtils.setTriangleYCoordinate(trianglesY, 0, 2, y2);

                        SpatialUtils.setTriangleXCoordinate(trianglesX, 1, 0, x2);
                        SpatialUtils.setTriangleYCoordinate(trianglesY, 1, 0, y2);

                        SpatialUtils.setTriangleXCoordinate(trianglesX, 1, 1, x1);
                        SpatialUtils.setTriangleYCoordinate(trianglesY, 1, 1, y2);

                        SpatialUtils.setTriangleXCoordinate(trianglesX, 1, 2, x1);
                        SpatialUtils.setTriangleYCoordinate(trianglesY, 1, 2, y1);
                    }

                    private boolean rectanglePolygonIntersection(byte[] bytes0, int offset0, byte[] bytes1, int offset1)
                            throws HyracksDataException {
                        int numOfPoints1 = AInt16SerializerDeserializer.getShort(bytes1,
                                offset1 + APolygonSerializerDeserializer.getNumberOfPointsOffset());

                        if (numOfPoints1 < 3) {
                            throw new InvalidDataFormatException(sourceLoc, getIdentifier(),
                                    ATypeTag.SERIALIZED_POLYGON_TYPE_TAG);
                        }

                        getCounterClockWisePolygon(bytes1, offset1, pointsOffsets1, numOfPoints1);
                        int nonSimplePolygonDetection1 = 2 * numOfPoints1;
                        int middleVertex1 = numOfPoints1 - 1;
                        int numOfTriangles1 = 0;

                        trianglesX1.reset();
                        trianglesY1.reset();
                        while (true) {
                            middleVertex1 =
                                    triangulatePolygon(bytes1, offset1, numOfPoints1, pointsOffsets1, trianglesX1,
                                            trianglesY1, numOfTriangles1, nonSimplePolygonDetection1, middleVertex1);

                            if (middleVertex1 == -1) {
                                break;
                            }

                            numOfPoints1--;
                            nonSimplePolygonDetection1 = 2 * numOfPoints1;
                            numOfTriangles1++;
                        }

                        triangulateRectangle(bytes0, offset0, trianglesX0, trianglesY0);
                        boolean res = false;
                        // 2 triangles in a rectangle
                        for (int j = 0; j < 2; j++) {
                            for (int i = 0; i < numOfTriangles1; i++) {

                                res = triangleTriangleIntersection(trianglesX1, trianglesY1, i, trianglesX0,
                                        trianglesY0, j);

                                if (res) {
                                    res = triangleTriangleIntersection(trianglesX0, trianglesY0, j, trianglesX1,
                                            trianglesY1, i);

                                    if (res) {
                                        return true;
                                    }
                                }
                            }
                        }
                        return false;
                    }

                    private boolean polygonCircleIntersection(byte[] bytes0, int offset0, byte[] bytes1, int offset1)
                            throws HyracksDataException {
                        int numOfPoints = AInt16SerializerDeserializer.getShort(bytes0,
                                offset0 + APolygonSerializerDeserializer.getNumberOfPointsOffset());

                        if (numOfPoints < 3) {
                            throw new InvalidDataFormatException(sourceLoc, getIdentifier(),
                                    ATypeTag.SERIALIZED_POLYGON_TYPE_TAG);
                        }

                        getCounterClockWisePolygon(bytes0, offset0, pointsOffsets0, numOfPoints);
                        int nonSimplePolygonDetection = 2 * numOfPoints;
                        int middleVertex = numOfPoints - 1;
                        int numOfTriangles = 0;

                        trianglesX0.reset();
                        trianglesY0.reset();
                        boolean res = false;
                        while (true) {
                            middleVertex = triangulatePolygon(bytes0, offset0, numOfPoints, pointsOffsets0, trianglesX0,
                                    trianglesY0, numOfTriangles, nonSimplePolygonDetection, middleVertex);

                            if (middleVertex == -1) {
                                break;
                            }
                            numOfPoints--;
                            nonSimplePolygonDetection = 2 * numOfPoints;
                            numOfTriangles++;
                            int lastTriangle = (trianglesX0.length() / 3) - 1;

                            res = circleTriangleIntersection(bytes1, offset1, trianglesX0, trianglesY0, lastTriangle);
                            if (res) {
                                return true;
                            }
                        }
                        return false;
                    }

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

                        boolean res = false;
                        ATypeTag tag0 = EnumDeserializer.ATYPETAGDESERIALIZER.deserialize(bytes0[offset0]);
                        ATypeTag tag1 = EnumDeserializer.ATYPETAGDESERIALIZER.deserialize(bytes1[offset1]);

                        switch (tag0) {
                            case POINT:
                                switch (tag1) {
                                    case POINT:
                                        if (ascDoubleComp.compare(bytes0,
                                                offset0 + APointSerializerDeserializer
                                                        .getCoordinateOffset(Coordinate.X),
                                                8, bytes1, offset1 + APointSerializerDeserializer
                                                        .getCoordinateOffset(Coordinate.X),
                                                8) == 0) {
                                            if (ascDoubleComp.compare(bytes0,
                                                    offset0 + APointSerializerDeserializer
                                                            .getCoordinateOffset(Coordinate.Y),
                                                    8, bytes1, offset1 + APointSerializerDeserializer
                                                            .getCoordinateOffset(Coordinate.Y),
                                                    8) == 0) {
                                                res = true;
                                            }
                                        }
                                        break;
                                    case LINE:
                                        double pX = ADoubleSerializerDeserializer.getDouble(bytes0, offset0
                                                + APointSerializerDeserializer.getCoordinateOffset(Coordinate.X));
                                        double pY = ADoubleSerializerDeserializer.getDouble(bytes0, offset0
                                                + APointSerializerDeserializer.getCoordinateOffset(Coordinate.Y));

                                        double startX = ADoubleSerializerDeserializer.getDouble(bytes1,
                                                offset1 + ALineSerializerDeserializer
                                                        .getStartPointCoordinateOffset(Coordinate.X));
                                        double startY = ADoubleSerializerDeserializer.getDouble(bytes1,
                                                offset1 + ALineSerializerDeserializer
                                                        .getStartPointCoordinateOffset(Coordinate.Y));
                                        double endX = ADoubleSerializerDeserializer.getDouble(bytes1,
                                                offset1 + ALineSerializerDeserializer
                                                        .getEndPointCoordinateOffset(Coordinate.X));
                                        double endY = ADoubleSerializerDeserializer.getDouble(bytes1,
                                                offset1 + ALineSerializerDeserializer
                                                        .getEndPointCoordinateOffset(Coordinate.Y));

                                        res = pointOnLine(pX, pY, startX, startY, endX, endY);
                                        break;
                                    case POLYGON:
                                        res = pointInPolygon(bytes0, offset0, bytes1, offset1);
                                        break;
                                    case CIRCLE:
                                        res = pointInCircle(bytes0, offset0, bytes1, offset1);
                                        break;
                                    case RECTANGLE:
                                        res = pointInRectangle(bytes0, offset0, bytes1, offset1);
                                        break;
                                    default:
                                        throw new TypeMismatchException(sourceLoc, getIdentifier(), 1, bytes1[offset1],
                                                ATypeTag.SERIALIZED_POINT_TYPE_TAG, ATypeTag.SERIALIZED_LINE_TYPE_TAG,
                                                ATypeTag.SERIALIZED_POLYGON_TYPE_TAG,
                                                ATypeTag.SERIALIZED_CIRCLE_TYPE_TAG,
                                                ATypeTag.SERIALIZED_RECTANGLE_TYPE_TAG);
                                }
                                break;
                            case LINE:
                                switch (tag1) {
                                    case POINT:
                                        double pX = ADoubleSerializerDeserializer.getDouble(bytes1, offset1
                                                + APointSerializerDeserializer.getCoordinateOffset(Coordinate.X));
                                        double pY = ADoubleSerializerDeserializer.getDouble(bytes1, offset1
                                                + APointSerializerDeserializer.getCoordinateOffset(Coordinate.Y));

                                        double startX = ADoubleSerializerDeserializer.getDouble(bytes0,
                                                offset0 + ALineSerializerDeserializer
                                                        .getStartPointCoordinateOffset(Coordinate.X));
                                        double startY = ADoubleSerializerDeserializer.getDouble(bytes0,
                                                offset0 + ALineSerializerDeserializer
                                                        .getStartPointCoordinateOffset(Coordinate.Y));
                                        double endX = ADoubleSerializerDeserializer.getDouble(bytes0,
                                                offset0 + ALineSerializerDeserializer
                                                        .getEndPointCoordinateOffset(Coordinate.X));
                                        double endY = ADoubleSerializerDeserializer.getDouble(bytes0,
                                                offset0 + ALineSerializerDeserializer
                                                        .getEndPointCoordinateOffset(Coordinate.Y));

                                        res = pointOnLine(pX, pY, startX, startY, endX, endY);
                                        break;
                                    case LINE:
                                        double startX1 = ADoubleSerializerDeserializer.getDouble(bytes0,
                                                offset0 + ALineSerializerDeserializer
                                                        .getStartPointCoordinateOffset(Coordinate.X));
                                        double startY1 = ADoubleSerializerDeserializer.getDouble(bytes0,
                                                offset0 + ALineSerializerDeserializer
                                                        .getStartPointCoordinateOffset(Coordinate.Y));
                                        double endX1 = ADoubleSerializerDeserializer.getDouble(bytes0,
                                                offset0 + ALineSerializerDeserializer
                                                        .getEndPointCoordinateOffset(Coordinate.X));
                                        double endY1 = ADoubleSerializerDeserializer.getDouble(bytes0,
                                                offset0 + ALineSerializerDeserializer
                                                        .getEndPointCoordinateOffset(Coordinate.Y));

                                        double startX2 = ADoubleSerializerDeserializer.getDouble(bytes1,
                                                offset1 + ALineSerializerDeserializer
                                                        .getStartPointCoordinateOffset(Coordinate.X));
                                        double startY2 = ADoubleSerializerDeserializer.getDouble(bytes1,
                                                offset1 + ALineSerializerDeserializer
                                                        .getStartPointCoordinateOffset(Coordinate.Y));
                                        double endX2 = ADoubleSerializerDeserializer.getDouble(bytes1,
                                                offset1 + ALineSerializerDeserializer
                                                        .getEndPointCoordinateOffset(Coordinate.X));
                                        double endY2 = ADoubleSerializerDeserializer.getDouble(bytes1,
                                                offset1 + ALineSerializerDeserializer
                                                        .getEndPointCoordinateOffset(Coordinate.Y));
                                        res = lineLineIntersection(startX1, startY1, endX1, endY1, startX2, startY2,
                                                endX2, endY2);
                                        break;
                                    case POLYGON:
                                        res = linePolygonIntersection(bytes0, offset0, bytes1, offset1);
                                        break;
                                    case CIRCLE:
                                        res = lineCircleIntersection(bytes0, offset0, bytes1, offset1);
                                        break;
                                    case RECTANGLE:
                                        res = lineRectangleIntersection(bytes0, offset0, bytes1, offset1);
                                        break;
                                    default:
                                        throw new TypeMismatchException(sourceLoc, getIdentifier(), 1, bytes1[offset1],
                                                ATypeTag.SERIALIZED_POINT_TYPE_TAG, ATypeTag.SERIALIZED_LINE_TYPE_TAG,
                                                ATypeTag.SERIALIZED_POLYGON_TYPE_TAG,
                                                ATypeTag.SERIALIZED_CIRCLE_TYPE_TAG,
                                                ATypeTag.SERIALIZED_RECTANGLE_TYPE_TAG);
                                }
                                break;
                            case POLYGON:
                                switch (tag1) {
                                    case POINT:
                                        res = pointInPolygon(bytes1, offset1, bytes0, offset0);
                                        break;
                                    case LINE:
                                        res = linePolygonIntersection(bytes1, offset1, bytes0, offset0);
                                        break;
                                    case POLYGON:
                                        int numOfPoints0 = AInt16SerializerDeserializer.getShort(bytes0,
                                                offset0 + APolygonSerializerDeserializer.getNumberOfPointsOffset());
                                        int numOfPoints1 = AInt16SerializerDeserializer.getShort(bytes1,
                                                offset1 + APolygonSerializerDeserializer.getNumberOfPointsOffset());

                                        if (numOfPoints0 < 3 || numOfPoints1 < 3) {
                                            throw new InvalidDataFormatException(sourceLoc, getIdentifier(),
                                                    ATypeTag.SERIALIZED_POLYGON_TYPE_TAG);
                                        }

                                        getCounterClockWisePolygon(bytes0, offset0, pointsOffsets0, numOfPoints0);
                                        getCounterClockWisePolygon(bytes1, offset1, pointsOffsets1, numOfPoints1);
                                        int nonSimplePolygonDetection0 = 2 * numOfPoints0;
                                        int nonSimplePolygonDetection1 = 2 * numOfPoints1;
                                        boolean intersect = false;
                                        int middleVertex0 = numOfPoints0 - 1;

                                        int numOfTriangles1 = 0;
                                        int middleVertex1 = numOfPoints1 - 1;
                                        trianglesX1.reset();
                                        trianglesY1.reset();
                                        while (true) {
                                            middleVertex1 = triangulatePolygon(bytes1, offset1, numOfPoints1,
                                                    pointsOffsets1, trianglesX1, trianglesY1, numOfTriangles1,
                                                    nonSimplePolygonDetection1, middleVertex1);

                                            if (middleVertex1 == -1) {
                                                break;
                                            }

                                            numOfPoints1--;
                                            nonSimplePolygonDetection1 = 2 * numOfPoints1;
                                            numOfTriangles1++;
                                        }
                                        int numOfTriangles0 = 0;
                                        trianglesX0.reset();
                                        trianglesY0.reset();
                                        while (true) {
                                            middleVertex0 = triangulatePolygon(bytes0, offset0, numOfPoints0,
                                                    pointsOffsets0, trianglesX0, trianglesY0, numOfTriangles0,
                                                    nonSimplePolygonDetection0, middleVertex0);

                                            if (middleVertex0 == -1) {
                                                break;
                                            }
                                            numOfPoints0--;
                                            nonSimplePolygonDetection0 = 2 * numOfPoints0;
                                            numOfTriangles0++;
                                            int lastTriangle = (trianglesX0.length() / 3) - 1;

                                            for (int i = 0; i < numOfTriangles1; i++) {

                                                res = triangleTriangleIntersection(trianglesX0, trianglesY0,
                                                        lastTriangle, trianglesX1, trianglesY1, i);

                                                if (res) {
                                                    res = triangleTriangleIntersection(trianglesX1, trianglesY1, i,
                                                            trianglesX0, trianglesY0, lastTriangle);

                                                    if (res) {
                                                        intersect = true;
                                                        break;
                                                    }
                                                }
                                            }
                                            if (intersect) {
                                                break;
                                            }
                                        }
                                        break;
                                    case CIRCLE:
                                        res = polygonCircleIntersection(bytes0, offset0, bytes1, offset1);
                                        break;
                                    case RECTANGLE:
                                        res = rectanglePolygonIntersection(bytes1, offset1, bytes0, offset0);
                                        break;
                                    default:
                                        throw new TypeMismatchException(sourceLoc, getIdentifier(), 1, bytes1[offset1],
                                                ATypeTag.SERIALIZED_POINT_TYPE_TAG, ATypeTag.SERIALIZED_LINE_TYPE_TAG,
                                                ATypeTag.SERIALIZED_POLYGON_TYPE_TAG,
                                                ATypeTag.SERIALIZED_CIRCLE_TYPE_TAG,
                                                ATypeTag.SERIALIZED_RECTANGLE_TYPE_TAG);
                                }
                                break;
                            case CIRCLE:
                                switch (tag1) {
                                    case POINT:
                                        res = pointInCircle(bytes1, offset1, bytes0, offset0);
                                        break;
                                    case LINE:
                                        res = lineCircleIntersection(bytes1, offset1, bytes0, offset0);
                                        break;
                                    case POLYGON:
                                        res = polygonCircleIntersection(bytes1, offset1, bytes0, offset0);
                                        break;
                                    case CIRCLE:
                                        res = circleCircleIntersection(bytes0, offset0, bytes1, offset1);
                                        break;
                                    case RECTANGLE:
                                        res = rectangleCircleIntersection(bytes1, offset1, bytes0, offset0);
                                        break;
                                    default:
                                        throw new TypeMismatchException(sourceLoc, getIdentifier(), 1, bytes1[offset1],
                                                ATypeTag.SERIALIZED_POINT_TYPE_TAG, ATypeTag.SERIALIZED_LINE_TYPE_TAG,
                                                ATypeTag.SERIALIZED_POLYGON_TYPE_TAG,
                                                ATypeTag.SERIALIZED_CIRCLE_TYPE_TAG,
                                                ATypeTag.SERIALIZED_RECTANGLE_TYPE_TAG);
                                }
                                break;
                            case RECTANGLE:
                                switch (tag1) {
                                    case POINT:
                                        res = pointInRectangle(bytes1, offset1, bytes0, offset0);
                                        break;
                                    case LINE:
                                        res = lineRectangleIntersection(bytes1, offset1, bytes0, offset0);
                                        break;
                                    case POLYGON:
                                        res = rectanglePolygonIntersection(bytes0, offset0, bytes1, offset1);
                                        break;
                                    case CIRCLE:
                                        res = rectangleCircleIntersection(bytes0, offset0, bytes1, offset1);
                                        break;
                                    case RECTANGLE:
                                        triangulateRectangle(bytes0, offset0, trianglesX0, trianglesY0);
                                        triangulateRectangle(bytes1, offset1, trianglesX1, trianglesY1);

                                        boolean intersect = false;
                                        // 2 triangles in a rectangle
                                        for (int j = 0; j < 2; j++) {
                                            for (int i = 0; i < 2; i++) {

                                                res = triangleTriangleIntersection(trianglesX1, trianglesY1, i,
                                                        trianglesX0, trianglesY0, j);

                                                if (res) {
                                                    res = triangleTriangleIntersection(trianglesX0, trianglesY0, j,
                                                            trianglesX1, trianglesY1, i);

                                                    if (res) {
                                                        intersect = true;
                                                        break;
                                                    }
                                                }
                                            }
                                            if (intersect) {
                                                break;
                                            }
                                        }
                                        break;
                                    default:
                                        throw new TypeMismatchException(sourceLoc, getIdentifier(), 1, bytes1[offset1],
                                                ATypeTag.SERIALIZED_POINT_TYPE_TAG, ATypeTag.SERIALIZED_LINE_TYPE_TAG,
                                                ATypeTag.SERIALIZED_POLYGON_TYPE_TAG,
                                                ATypeTag.SERIALIZED_CIRCLE_TYPE_TAG,
                                                ATypeTag.SERIALIZED_RECTANGLE_TYPE_TAG);
                                }
                                break;
                            default:
                                throw new TypeMismatchException(sourceLoc, getIdentifier(), 0, bytes0[offset0],
                                        ATypeTag.SERIALIZED_POINT_TYPE_TAG, ATypeTag.SERIALIZED_LINE_TYPE_TAG,
                                        ATypeTag.SERIALIZED_POLYGON_TYPE_TAG, ATypeTag.SERIALIZED_CIRCLE_TYPE_TAG,
                                        ATypeTag.SERIALIZED_RECTANGLE_TYPE_TAG);
                        }

                        ABoolean aResult = res ? ABoolean.TRUE : ABoolean.FALSE;
                        aBooleanSerDer.serialize(aResult, out);
                        result.set(resultStorage);
                    }
                };
            }
        };
    }

    @Override
    public FunctionIdentifier getIdentifier() {
        return BuiltinFunctions.SPATIAL_INTERSECT;
    }
}
