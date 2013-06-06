/*
 * Copyright 2009-2013 by The Regents of the University of California
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * you may obtain a copy of the License from
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.uci.ics.asterix.runtime.evaluators.functions;

import java.io.DataOutput;

import edu.uci.ics.asterix.dataflow.data.nontagged.Coordinate;
import edu.uci.ics.asterix.dataflow.data.nontagged.serde.ACircleSerializerDeserializer;
import edu.uci.ics.asterix.dataflow.data.nontagged.serde.ADoubleSerializerDeserializer;
import edu.uci.ics.asterix.dataflow.data.nontagged.serde.AInt16SerializerDeserializer;
import edu.uci.ics.asterix.dataflow.data.nontagged.serde.ALineSerializerDeserializer;
import edu.uci.ics.asterix.dataflow.data.nontagged.serde.AObjectSerializerDeserializer;
import edu.uci.ics.asterix.dataflow.data.nontagged.serde.APointSerializerDeserializer;
import edu.uci.ics.asterix.dataflow.data.nontagged.serde.APolygonSerializerDeserializer;
import edu.uci.ics.asterix.dataflow.data.nontagged.serde.ARectangleSerializerDeserializer;
import edu.uci.ics.asterix.formats.nontagged.AqlBinaryComparatorFactoryProvider;
import edu.uci.ics.asterix.om.base.ABoolean;
import edu.uci.ics.asterix.om.functions.AsterixBuiltinFunctions;
import edu.uci.ics.asterix.om.functions.IFunctionDescriptor;
import edu.uci.ics.asterix.om.functions.IFunctionDescriptorFactory;
import edu.uci.ics.asterix.om.types.ATypeTag;
import edu.uci.ics.asterix.om.types.EnumDeserializer;
import edu.uci.ics.asterix.runtime.evaluators.base.AbstractScalarFunctionDynamicDescriptor;
import edu.uci.ics.asterix.runtime.evaluators.common.DoubleArray;
import edu.uci.ics.asterix.runtime.evaluators.common.SpatialUtils;
import edu.uci.ics.fuzzyjoin.IntArray;
import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.common.exceptions.NotImplementedException;
import edu.uci.ics.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import edu.uci.ics.hyracks.algebricks.runtime.base.ICopyEvaluator;
import edu.uci.ics.hyracks.algebricks.runtime.base.ICopyEvaluatorFactory;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparator;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.data.std.api.IDataOutputProvider;
import edu.uci.ics.hyracks.data.std.util.ArrayBackedValueStorage;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.IFrameTupleReference;

public class SpatialIntersectDescriptor extends AbstractScalarFunctionDynamicDescriptor {

    private static final long serialVersionUID = 1L;

    public static final IFunctionDescriptorFactory FACTORY = new IFunctionDescriptorFactory() {
        public IFunctionDescriptor createFunctionDescriptor() {
            return new SpatialIntersectDescriptor();
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
                    private final ArrayBackedValueStorage outInput0 = new ArrayBackedValueStorage();
                    private final ArrayBackedValueStorage outInput1 = new ArrayBackedValueStorage();
                    private final ICopyEvaluator eval0 = args[0].createEvaluator(outInput0);
                    private final ICopyEvaluator eval1 = args[1].createEvaluator(outInput1);
                    private final IBinaryComparator ascDoubleComp = AqlBinaryComparatorFactoryProvider.DOUBLE_POINTABLE_INSTANCE
                            .createBinaryComparator();
                    private final SpatialUtils spatialUtils = new SpatialUtils();
                    private final IntArray pointsOffsets0 = new IntArray();
                    private final IntArray pointsOffsets1 = new IntArray();
                    private final DoubleArray trianglesX0 = new DoubleArray();
                    private final DoubleArray trianglesY0 = new DoubleArray();
                    private final DoubleArray trianglesX1 = new DoubleArray();
                    private final DoubleArray trianglesY1 = new DoubleArray();

                    private boolean pointOnLine(double pX, double pY, double startX, double startY, double endX,
                            double endY) throws HyracksDataException {
                        double crossProduct = SpatialUtils.crossProduct(pY - startY, pX - startX, endY - startY, endX
                                - startX);
                        if (Math.abs(crossProduct) > SpatialUtils.doubleEpsilon()) { // crossProduct != 0
                            return false;
                        }

                        double dotProduct = SpatialUtils.dotProduct((pX - startX), (pY - startY), (endX - startX),
                                (endY - startY));
                        if (dotProduct < 0.0) {
                            return false;
                        }

                        double squaredLengthBA = (endX - startX) * (endX - startX) + (endY - startY) * (endY - startY);
                        if (dotProduct > squaredLengthBA) {
                            return false;
                        }
                        return true;
                    }

                    private boolean pointInPolygon(byte[] bytes0, byte[] bytes1) throws HyracksDataException { // ray casting

                        double pX = ADoubleSerializerDeserializer.getDouble(bytes0,
                                APointSerializerDeserializer.getCoordinateOffset(Coordinate.X));
                        double pY = ADoubleSerializerDeserializer.getDouble(bytes0,
                                APointSerializerDeserializer.getCoordinateOffset(Coordinate.Y));
                        int numOfPoints1 = AInt16SerializerDeserializer.getShort(bytes1,
                                APolygonSerializerDeserializer.getNumberOfPointsOffset());

                        if (numOfPoints1 < 3) {
                            throw new HyracksDataException(AsterixBuiltinFunctions.SPATIAL_INTERSECT.getName()
                                    + ": polygon must have at least 3 points.");
                        }

                        int counter = 0;
                        double xInters;
                        double x1, x2, y1, y2;
                        x1 = ADoubleSerializerDeserializer.getDouble(bytes1,
                                APolygonSerializerDeserializer.getCoordinateOffset(0, Coordinate.X));
                        y1 = ADoubleSerializerDeserializer.getDouble(bytes1,
                                APolygonSerializerDeserializer.getCoordinateOffset(0, Coordinate.Y));

                        for (int i = 1; i <= numOfPoints1; i++) {
                            if (i == numOfPoints1) {
                                x2 = ADoubleSerializerDeserializer.getDouble(bytes1,
                                        APolygonSerializerDeserializer.getCoordinateOffset(0, Coordinate.X));
                                y2 = ADoubleSerializerDeserializer.getDouble(bytes1,
                                        APolygonSerializerDeserializer.getCoordinateOffset(0, Coordinate.Y));
                            } else {
                                x2 = ADoubleSerializerDeserializer.getDouble(bytes1,
                                        APolygonSerializerDeserializer.getCoordinateOffset(i, Coordinate.X));
                                y2 = ADoubleSerializerDeserializer.getDouble(bytes1,
                                        APolygonSerializerDeserializer.getCoordinateOffset(i, Coordinate.Y));
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

                    private boolean pointInCircle(byte[] bytes0, byte[] bytes1) throws HyracksDataException {
                        double x = ADoubleSerializerDeserializer.getDouble(bytes0,
                                APointSerializerDeserializer.getCoordinateOffset(Coordinate.X));
                        double y = ADoubleSerializerDeserializer.getDouble(bytes0,
                                APointSerializerDeserializer.getCoordinateOffset(Coordinate.Y));

                        double cX = ADoubleSerializerDeserializer.getDouble(bytes1,
                                ACircleSerializerDeserializer.getCenterPointCoordinateOffset(Coordinate.X));
                        double cY = ADoubleSerializerDeserializer.getDouble(bytes1,
                                ACircleSerializerDeserializer.getCenterPointCoordinateOffset(Coordinate.Y));
                        double radius = ADoubleSerializerDeserializer.getDouble(bytes1,
                                ACircleSerializerDeserializer.getRadiusOffset());

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

                    private boolean linePolygonIntersection(byte[] bytes0, byte[] bytes1) throws HyracksDataException {
                        double startX1 = ADoubleSerializerDeserializer.getDouble(bytes0,
                                ALineSerializerDeserializer.getStartPointCoordinateOffset(Coordinate.X));
                        double startY1 = ADoubleSerializerDeserializer.getDouble(bytes0,
                                ALineSerializerDeserializer.getStartPointCoordinateOffset(Coordinate.Y));
                        double endX1 = ADoubleSerializerDeserializer.getDouble(bytes0,
                                ALineSerializerDeserializer.getEndPointCoordinateOffset(Coordinate.X));
                        double endY1 = ADoubleSerializerDeserializer.getDouble(bytes0,
                                ALineSerializerDeserializer.getEndPointCoordinateOffset(Coordinate.Y));

                        int numOfPoints1 = AInt16SerializerDeserializer.getShort(bytes1,
                                APolygonSerializerDeserializer.getNumberOfPointsOffset());

                        if (numOfPoints1 < 3) {
                            throw new HyracksDataException(AsterixBuiltinFunctions.SPATIAL_INTERSECT.getName()
                                    + ": polygon must have at least 3 points.");
                        }
                        for (int i = 0; i < numOfPoints1; i++) {
                            double startX2 = ADoubleSerializerDeserializer.getDouble(bytes1,
                                    APolygonSerializerDeserializer.getCoordinateOffset(i, Coordinate.X));
                            double startY2 = ADoubleSerializerDeserializer.getDouble(bytes1,
                                    APolygonSerializerDeserializer.getCoordinateOffset(i, Coordinate.Y));

                            double endX2;
                            double endY2;
                            if (i + 1 == numOfPoints1) {
                                endX2 = ADoubleSerializerDeserializer.getDouble(bytes1,
                                        APolygonSerializerDeserializer.getCoordinateOffset(0, Coordinate.X));
                                endY2 = ADoubleSerializerDeserializer.getDouble(bytes1,
                                        APolygonSerializerDeserializer.getCoordinateOffset(0, Coordinate.Y));
                            } else {
                                endX2 = ADoubleSerializerDeserializer.getDouble(bytes1,
                                        APolygonSerializerDeserializer.getCoordinateOffset(i + 1, Coordinate.X));
                                endY2 = ADoubleSerializerDeserializer.getDouble(bytes1,
                                        APolygonSerializerDeserializer.getCoordinateOffset(i + 1, Coordinate.Y));
                            }

                            boolean intersect = lineLineIntersection(startX1, startY1, endX1, endY1, startX2, startY2,
                                    endX2, endY2);
                            if (intersect) {
                                return true;
                            }
                        }
                        return false;
                    }

                    private boolean lineRectangleIntersection(byte[] bytes0, byte[] bytes1) throws HyracksDataException {
                        double startX1 = ADoubleSerializerDeserializer.getDouble(bytes0,
                                ALineSerializerDeserializer.getStartPointCoordinateOffset(Coordinate.X));
                        double startY1 = ADoubleSerializerDeserializer.getDouble(bytes0,
                                ALineSerializerDeserializer.getStartPointCoordinateOffset(Coordinate.Y));
                        double endX1 = ADoubleSerializerDeserializer.getDouble(bytes0,
                                ALineSerializerDeserializer.getEndPointCoordinateOffset(Coordinate.X));
                        double endY1 = ADoubleSerializerDeserializer.getDouble(bytes0,
                                ALineSerializerDeserializer.getEndPointCoordinateOffset(Coordinate.Y));

                        double x1 = ADoubleSerializerDeserializer.getDouble(bytes1,
                                ARectangleSerializerDeserializer.getBottomLeftCoordinateOffset(Coordinate.X));
                        double y1 = ADoubleSerializerDeserializer.getDouble(bytes1,
                                ARectangleSerializerDeserializer.getBottomLeftCoordinateOffset(Coordinate.Y));

                        double x2 = ADoubleSerializerDeserializer.getDouble(bytes1,
                                ARectangleSerializerDeserializer.getUpperRightCoordinateOffset(Coordinate.X));
                        double y2 = ADoubleSerializerDeserializer.getDouble(bytes1,
                                ARectangleSerializerDeserializer.getUpperRightCoordinateOffset(Coordinate.Y));

                        if (lineLineIntersection(startX1, startY1, endX1, endY1, x1, y1, x1, y2)
                                || lineLineIntersection(startX1, startY1, endX1, endY1, x1, y2, x2, y2)
                                || lineLineIntersection(startX1, startY1, endX1, endY1, x2, y2, x2, y1)
                                || lineLineIntersection(startX1, startY1, endX1, endY1, x2, y1, x1, y1)) {
                            return true;
                        }
                        return false;

                    }

                    private boolean lineCircleIntersection(byte[] bytes0, byte[] bytes1) throws HyracksDataException {
                        double startX = ADoubleSerializerDeserializer.getDouble(bytes0,
                                ALineSerializerDeserializer.getStartPointCoordinateOffset(Coordinate.X));
                        double startY = ADoubleSerializerDeserializer.getDouble(bytes0,
                                ALineSerializerDeserializer.getStartPointCoordinateOffset(Coordinate.Y));
                        double endX = ADoubleSerializerDeserializer.getDouble(bytes0,
                                ALineSerializerDeserializer.getEndPointCoordinateOffset(Coordinate.X));
                        double endY = ADoubleSerializerDeserializer.getDouble(bytes0,
                                ALineSerializerDeserializer.getEndPointCoordinateOffset(Coordinate.Y));

                        double cX = ADoubleSerializerDeserializer.getDouble(bytes1,
                                ACircleSerializerDeserializer.getCenterPointCoordinateOffset(Coordinate.X));
                        double cY = ADoubleSerializerDeserializer.getDouble(bytes1,
                                ACircleSerializerDeserializer.getCenterPointCoordinateOffset(Coordinate.Y));
                        double radius = ADoubleSerializerDeserializer.getDouble(bytes1,
                                ACircleSerializerDeserializer.getRadiusOffset());

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

                    private boolean findEar(byte[] bytes, int u, int v, int w, int n, IntArray pointsOffsets)
                            throws HyracksDataException {
                        int p;
                        double Ax, Ay, Bx, By, Cx, Cy, Px, Py;

                        Ax = ADoubleSerializerDeserializer.getDouble(bytes,
                                APolygonSerializerDeserializer.getCoordinateOffset(pointsOffsets.get(u), Coordinate.X));
                        Ay = ADoubleSerializerDeserializer.getDouble(bytes,
                                APolygonSerializerDeserializer.getCoordinateOffset(pointsOffsets.get(u), Coordinate.Y));

                        Bx = ADoubleSerializerDeserializer.getDouble(bytes,
                                APolygonSerializerDeserializer.getCoordinateOffset(pointsOffsets.get(v), Coordinate.X));
                        By = ADoubleSerializerDeserializer.getDouble(bytes,
                                APolygonSerializerDeserializer.getCoordinateOffset(pointsOffsets.get(v), Coordinate.Y));

                        Cx = ADoubleSerializerDeserializer.getDouble(bytes,
                                APolygonSerializerDeserializer.getCoordinateOffset(pointsOffsets.get(w), Coordinate.X));
                        Cy = ADoubleSerializerDeserializer.getDouble(bytes,
                                APolygonSerializerDeserializer.getCoordinateOffset(pointsOffsets.get(w), Coordinate.Y));

                        if (SpatialUtils.doubleEpsilon() > (((Bx - Ax) * (Cy - Ay)) - ((By - Ay) * (Cx - Ax)))) {

                            return false;
                        }

                        for (p = 0; p < n; p++) {
                            if ((p == u) || (p == v) || (p == w)) {
                                continue;
                            }
                            Px = ADoubleSerializerDeserializer.getDouble(bytes, APolygonSerializerDeserializer
                                    .getCoordinateOffset(pointsOffsets.get(p), Coordinate.X));
                            Py = ADoubleSerializerDeserializer.getDouble(bytes, APolygonSerializerDeserializer
                                    .getCoordinateOffset(pointsOffsets.get(p), Coordinate.Y));
                            if (pointInsideTriangle(Ax, Ay, Bx, By, Cx, Cy, Px, Py)) {
                                return false;
                            }
                        }

                        return true;
                    }

                    private int triangulatePolygon(byte[] bytes, int numOfPoints, IntArray pointsOffsets,
                            DoubleArray trianglesX, DoubleArray trianglesY, int triangleId,
                            int nonSimplePolygonDetection, int middleVertex) throws HyracksDataException { // Ear clipping

                        if (numOfPoints < 3) {
                            return -1;
                        }

                        boolean foundEar = false;
                        int v = middleVertex;
                        while (!foundEar) {
                            if (0 >= (nonSimplePolygonDetection--)) {
                                throw new HyracksDataException(AsterixBuiltinFunctions.SPATIAL_INTERSECT.getName()
                                        + ": non-simple polygons are not supported.");
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

                            if (findEar(bytes, u, v, w, numOfPoints, pointsOffsets)) {
                                int s, t;

                                addRectangle(trianglesX, trianglesY);

                                SpatialUtils.setTriangleXCoordinate(trianglesX, triangleId, 0,
                                        ADoubleSerializerDeserializer.getDouble(bytes, APolygonSerializerDeserializer
                                                .getCoordinateOffset(pointsOffsets.get(u), Coordinate.X)));

                                SpatialUtils.setTriangleYCoordinate(trianglesY, triangleId, 0,
                                        ADoubleSerializerDeserializer.getDouble(bytes, APolygonSerializerDeserializer
                                                .getCoordinateOffset(pointsOffsets.get(u), Coordinate.Y)));

                                SpatialUtils.setTriangleXCoordinate(trianglesX, triangleId, 1,
                                        ADoubleSerializerDeserializer.getDouble(bytes, APolygonSerializerDeserializer
                                                .getCoordinateOffset(pointsOffsets.get(v), Coordinate.X)));

                                SpatialUtils.setTriangleYCoordinate(trianglesY, triangleId, 1,
                                        ADoubleSerializerDeserializer.getDouble(bytes, APolygonSerializerDeserializer
                                                .getCoordinateOffset(pointsOffsets.get(v), Coordinate.Y)));

                                SpatialUtils.setTriangleXCoordinate(trianglesX, triangleId, 2,
                                        ADoubleSerializerDeserializer.getDouble(bytes, APolygonSerializerDeserializer
                                                .getCoordinateOffset(pointsOffsets.get(w), Coordinate.X)));

                                SpatialUtils.setTriangleYCoordinate(trianglesY, triangleId, 2,
                                        ADoubleSerializerDeserializer.getDouble(bytes, APolygonSerializerDeserializer
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

                    private boolean circleTriangleIntersection(byte[] bytes0, DoubleArray trianglesX,
                            DoubleArray trianglesY, int triangleId) throws HyracksDataException { // separating axis theorem

                        double cX = ADoubleSerializerDeserializer.getDouble(bytes0,
                                ACircleSerializerDeserializer.getCenterPointCoordinateOffset(Coordinate.X));
                        double cY = ADoubleSerializerDeserializer.getDouble(bytes0,
                                ACircleSerializerDeserializer.getCenterPointCoordinateOffset(Coordinate.Y));
                        double radius = ADoubleSerializerDeserializer.getDouble(bytes0,
                                ACircleSerializerDeserializer.getRadiusOffset());

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

                            dotProduct = SpatialUtils.dotProduct(spatialUtils.getXAxis(), spatialUtils.getYAxis(), cX,
                                    cY);
                            max2 = dotProduct + radius;
                            min2 = dotProduct - radius;

                            if (max1 < min2 || min1 > max2) {
                                return false;
                            }
                        }
                        return true;
                    }

                    private boolean circleCircleIntersection(byte[] bytes0, byte[] bytes1) throws HyracksDataException {
                        double cX0 = ADoubleSerializerDeserializer.getDouble(bytes0,
                                ACircleSerializerDeserializer.getCenterPointCoordinateOffset(Coordinate.X));
                        double cY0 = ADoubleSerializerDeserializer.getDouble(bytes0,
                                ACircleSerializerDeserializer.getCenterPointCoordinateOffset(Coordinate.Y));
                        double radius0 = ADoubleSerializerDeserializer.getDouble(bytes0,
                                ACircleSerializerDeserializer.getRadiusOffset());

                        double cX1 = ADoubleSerializerDeserializer.getDouble(bytes1,
                                ACircleSerializerDeserializer.getCenterPointCoordinateOffset(Coordinate.X));
                        double cY1 = ADoubleSerializerDeserializer.getDouble(bytes1,
                                ACircleSerializerDeserializer.getCenterPointCoordinateOffset(Coordinate.Y));
                        double radius1 = ADoubleSerializerDeserializer.getDouble(bytes1,
                                ACircleSerializerDeserializer.getRadiusOffset());

                        double distanceSquared = SpatialUtils.dotProduct(cX0 - cX1, cY0 - cY1, cX0 - cX1, cY0 - cY1);
                        double radiusDistanceSquared = (radius0 + radius1) * (radius0 + radius1);
                        if (distanceSquared <= radiusDistanceSquared) {
                            return true;
                        }
                        return false;
                    }

                    private void getCounterClockWisePolygon(byte[] bytes, IntArray pointsOffsets, int numOfPoints)
                            throws HyracksDataException {
                        pointsOffsets.reset();
                        if (SpatialUtils.polygonArea(bytes, numOfPoints) > 0.0) {
                            for (int i = 0; i < numOfPoints; i++) {
                                pointsOffsets.add(i);
                            }
                        } else {
                            for (int i = 0; i < numOfPoints; i++) {
                                pointsOffsets.add((numOfPoints - 1) - i);
                            }
                        }
                    }

                    private boolean pointInRectangle(byte[] bytes0, byte[] bytes1) throws HyracksDataException {

                        double pX = ADoubleSerializerDeserializer.getDouble(bytes0,
                                APointSerializerDeserializer.getCoordinateOffset(Coordinate.X));
                        double pY = ADoubleSerializerDeserializer.getDouble(bytes0,
                                APointSerializerDeserializer.getCoordinateOffset(Coordinate.Y));

                        double x1 = ADoubleSerializerDeserializer.getDouble(bytes1,
                                ARectangleSerializerDeserializer.getBottomLeftCoordinateOffset(Coordinate.X));
                        double y1 = ADoubleSerializerDeserializer.getDouble(bytes1,
                                ARectangleSerializerDeserializer.getBottomLeftCoordinateOffset(Coordinate.Y));

                        double x2 = ADoubleSerializerDeserializer.getDouble(bytes1,
                                ARectangleSerializerDeserializer.getUpperRightCoordinateOffset(Coordinate.X));
                        double y2 = ADoubleSerializerDeserializer.getDouble(bytes1,
                                ARectangleSerializerDeserializer.getUpperRightCoordinateOffset(Coordinate.Y));

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

                    private boolean rectangleCircleIntersection(byte[] bytes0, byte[] bytes1)
                            throws HyracksDataException {
                        triangulateRectangle(bytes0, trianglesX0, trianglesY0);
                        boolean res = false;
                        // 2 triangles in a rectangle
                        for (int i = 0; i < 2; i++) {
                            res = circleTriangleIntersection(bytes1, trianglesX0, trianglesY0, i);
                            if (res) {
                                break;
                            }
                        }
                        return res;
                    }

                    private void triangulateRectangle(byte[] bytes, DoubleArray trianglesX, DoubleArray trianglesY)
                            throws HyracksDataException {
                        double x1 = ADoubleSerializerDeserializer.getDouble(bytes,
                                ARectangleSerializerDeserializer.getBottomLeftCoordinateOffset(Coordinate.X));
                        double y1 = ADoubleSerializerDeserializer.getDouble(bytes,
                                ARectangleSerializerDeserializer.getBottomLeftCoordinateOffset(Coordinate.Y));

                        double x2 = ADoubleSerializerDeserializer.getDouble(bytes,
                                ARectangleSerializerDeserializer.getUpperRightCoordinateOffset(Coordinate.X));
                        double y2 = ADoubleSerializerDeserializer.getDouble(bytes,
                                ARectangleSerializerDeserializer.getUpperRightCoordinateOffset(Coordinate.Y));
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

                    private boolean rectanglePolygonIntersection(byte[] bytes0, byte[] bytes1)
                            throws HyracksDataException, AlgebricksException {
                        int numOfPoints1 = AInt16SerializerDeserializer.getShort(bytes1,
                                APolygonSerializerDeserializer.getNumberOfPointsOffset());

                        if (numOfPoints1 < 3) {
                            throw new HyracksDataException(AsterixBuiltinFunctions.SPATIAL_INTERSECT.getName()
                                    + ": polygon must have at least 3 points.");
                        }

                        getCounterClockWisePolygon(bytes1, pointsOffsets1, numOfPoints1);
                        int nonSimplePolygonDetection1 = 2 * numOfPoints1;
                        int middleVertex1 = numOfPoints1 - 1;
                        int numOfTriangles1 = 0;

                        trianglesX1.reset();
                        trianglesY1.reset();
                        while (true) {
                            middleVertex1 = triangulatePolygon(bytes1, numOfPoints1, pointsOffsets1, trianglesX1,
                                    trianglesY1, numOfTriangles1, nonSimplePolygonDetection1, middleVertex1);

                            if (middleVertex1 == -1) {
                                break;
                            }

                            numOfPoints1--;
                            nonSimplePolygonDetection1 = 2 * numOfPoints1;
                            numOfTriangles1++;
                        }

                        triangulateRectangle(bytes0, trianglesX0, trianglesY0);
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

                    private boolean polygonCircleIntersection(byte[] bytes0, byte[] bytes1)
                            throws HyracksDataException, AlgebricksException {
                        int numOfPoints = AInt16SerializerDeserializer.getShort(bytes0,
                                APolygonSerializerDeserializer.getNumberOfPointsOffset());

                        if (numOfPoints < 3) {
                            throw new HyracksDataException(AsterixBuiltinFunctions.SPATIAL_INTERSECT.getName()
                                    + ": polygon must have at least 3 points.");
                        }

                        getCounterClockWisePolygon(bytes0, pointsOffsets0, numOfPoints);
                        int nonSimplePolygonDetection = 2 * numOfPoints;
                        int middleVertex = numOfPoints - 1;
                        int numOfTriangles = 0;

                        trianglesX0.reset();
                        trianglesY0.reset();
                        boolean res = false;
                        while (true) {
                            middleVertex = triangulatePolygon(bytes0, numOfPoints, pointsOffsets0, trianglesX0,
                                    trianglesY0, numOfTriangles, nonSimplePolygonDetection, middleVertex);

                            if (middleVertex == -1) {
                                break;
                            }
                            numOfPoints--;
                            nonSimplePolygonDetection = 2 * numOfPoints;
                            numOfTriangles++;
                            int lastTriangle = (trianglesX0.length() / 3) - 1;

                            res = circleTriangleIntersection(bytes1, trianglesX0, trianglesY0, lastTriangle);
                            if (res) {
                                return true;
                            }
                        }
                        return false;
                    }

                    @Override
                    public void evaluate(IFrameTupleReference tuple) throws AlgebricksException {
                        outInput0.reset();
                        eval0.evaluate(tuple);
                        outInput1.reset();
                        eval1.evaluate(tuple);

                        try {
                            boolean res = false;
                            ATypeTag tag0 = EnumDeserializer.ATYPETAGDESERIALIZER
                                    .deserialize(outInput0.getByteArray()[0]);
                            ATypeTag tag1 = EnumDeserializer.ATYPETAGDESERIALIZER
                                    .deserialize(outInput1.getByteArray()[0]);

                            switch (tag0) {
                                case POINT:
                                    switch (tag1) {
                                        case POINT:
                                            if (ascDoubleComp.compare(outInput0.getByteArray(),
                                                    APointSerializerDeserializer.getCoordinateOffset(Coordinate.X), 8,
                                                    outInput1.getByteArray(),
                                                    APointSerializerDeserializer.getCoordinateOffset(Coordinate.X), 8) == 0) {
                                                if (ascDoubleComp.compare(outInput0.getByteArray(),
                                                        APointSerializerDeserializer.getCoordinateOffset(Coordinate.Y),
                                                        8, outInput1.getByteArray(),
                                                        APointSerializerDeserializer.getCoordinateOffset(Coordinate.Y),
                                                        8) == 0) {
                                                    res = true;
                                                }
                                            }
                                            break;
                                        case LINE:
                                            double pX = ADoubleSerializerDeserializer.getDouble(
                                                    outInput0.getByteArray(),
                                                    APointSerializerDeserializer.getCoordinateOffset(Coordinate.X));
                                            double pY = ADoubleSerializerDeserializer.getDouble(
                                                    outInput0.getByteArray(),
                                                    APointSerializerDeserializer.getCoordinateOffset(Coordinate.Y));

                                            double startX = ADoubleSerializerDeserializer.getDouble(outInput1
                                                    .getByteArray(), ALineSerializerDeserializer
                                                    .getStartPointCoordinateOffset(Coordinate.X));
                                            double startY = ADoubleSerializerDeserializer.getDouble(outInput1
                                                    .getByteArray(), ALineSerializerDeserializer
                                                    .getStartPointCoordinateOffset(Coordinate.Y));
                                            double endX = ADoubleSerializerDeserializer.getDouble(outInput1
                                                    .getByteArray(), ALineSerializerDeserializer
                                                    .getEndPointCoordinateOffset(Coordinate.X));
                                            double endY = ADoubleSerializerDeserializer.getDouble(outInput1
                                                    .getByteArray(), ALineSerializerDeserializer
                                                    .getEndPointCoordinateOffset(Coordinate.Y));

                                            res = pointOnLine(pX, pY, startX, startY, endX, endY);
                                            break;
                                        case POLYGON:
                                            res = pointInPolygon(outInput0.getByteArray(), outInput1.getByteArray());
                                            break;
                                        case CIRCLE:
                                            res = pointInCircle(outInput0.getByteArray(), outInput1.getByteArray());
                                            break;
                                        case RECTANGLE:
                                            res = pointInRectangle(outInput0.getByteArray(), outInput1.getByteArray());
                                            break;
                                        case NULL:
                                            res = false;
                                            break;
                                        default:
                                            throw new NotImplementedException(
                                                    AsterixBuiltinFunctions.SPATIAL_INTERSECT.getName()
                                                            + ": does not support the type: "
                                                            + tag1
                                                            + "; it is only implemented for POINT, ALINE, POLYGON, and CIRCLE.");
                                    }
                                    break;
                                case LINE:
                                    switch (tag1) {
                                        case POINT:
                                            double pX = ADoubleSerializerDeserializer.getDouble(
                                                    outInput1.getByteArray(),
                                                    APointSerializerDeserializer.getCoordinateOffset(Coordinate.X));
                                            double pY = ADoubleSerializerDeserializer.getDouble(
                                                    outInput1.getByteArray(),
                                                    APointSerializerDeserializer.getCoordinateOffset(Coordinate.Y));

                                            double startX = ADoubleSerializerDeserializer.getDouble(outInput0
                                                    .getByteArray(), ALineSerializerDeserializer
                                                    .getStartPointCoordinateOffset(Coordinate.X));
                                            double startY = ADoubleSerializerDeserializer.getDouble(outInput0
                                                    .getByteArray(), ALineSerializerDeserializer
                                                    .getStartPointCoordinateOffset(Coordinate.Y));
                                            double endX = ADoubleSerializerDeserializer.getDouble(outInput0
                                                    .getByteArray(), ALineSerializerDeserializer
                                                    .getEndPointCoordinateOffset(Coordinate.X));
                                            double endY = ADoubleSerializerDeserializer.getDouble(outInput0
                                                    .getByteArray(), ALineSerializerDeserializer
                                                    .getEndPointCoordinateOffset(Coordinate.Y));

                                            res = pointOnLine(pX, pY, startX, startY, endX, endY);
                                            break;
                                        case LINE:
                                            double startX1 = ADoubleSerializerDeserializer.getDouble(outInput0
                                                    .getByteArray(), ALineSerializerDeserializer
                                                    .getStartPointCoordinateOffset(Coordinate.X));
                                            double startY1 = ADoubleSerializerDeserializer.getDouble(outInput0
                                                    .getByteArray(), ALineSerializerDeserializer
                                                    .getStartPointCoordinateOffset(Coordinate.Y));
                                            double endX1 = ADoubleSerializerDeserializer.getDouble(outInput0
                                                    .getByteArray(), ALineSerializerDeserializer
                                                    .getEndPointCoordinateOffset(Coordinate.X));
                                            double endY1 = ADoubleSerializerDeserializer.getDouble(outInput0
                                                    .getByteArray(), ALineSerializerDeserializer
                                                    .getEndPointCoordinateOffset(Coordinate.Y));

                                            double startX2 = ADoubleSerializerDeserializer.getDouble(outInput1
                                                    .getByteArray(), ALineSerializerDeserializer
                                                    .getStartPointCoordinateOffset(Coordinate.X));
                                            double startY2 = ADoubleSerializerDeserializer.getDouble(outInput1
                                                    .getByteArray(), ALineSerializerDeserializer
                                                    .getStartPointCoordinateOffset(Coordinate.Y));
                                            double endX2 = ADoubleSerializerDeserializer.getDouble(outInput1
                                                    .getByteArray(), ALineSerializerDeserializer
                                                    .getEndPointCoordinateOffset(Coordinate.X));
                                            double endY2 = ADoubleSerializerDeserializer.getDouble(outInput1
                                                    .getByteArray(), ALineSerializerDeserializer
                                                    .getEndPointCoordinateOffset(Coordinate.Y));
                                            res = lineLineIntersection(startX1, startY1, endX1, endY1, startX2,
                                                    startY2, endX2, endY2);
                                            break;
                                        case POLYGON:
                                            res = linePolygonIntersection(outInput0.getByteArray(),
                                                    outInput1.getByteArray());
                                            break;
                                        case CIRCLE:
                                            res = lineCircleIntersection(outInput0.getByteArray(),
                                                    outInput1.getByteArray());
                                            break;
                                        case RECTANGLE:
                                            res = lineRectangleIntersection(outInput0.getByteArray(),
                                                    outInput1.getByteArray());
                                            break;
                                        case NULL:
                                            res = false;
                                            break;
                                        default:
                                            throw new NotImplementedException(
                                                    AsterixBuiltinFunctions.SPATIAL_INTERSECT.getName()
                                                            + ": does not support the type: "
                                                            + tag1
                                                            + "; it is only implemented for POINT, ALINE, POLYGON, and CIRCLE.");
                                    }
                                    break;
                                case POLYGON:
                                    switch (tag1) {
                                        case POINT:
                                            res = pointInPolygon(outInput1.getByteArray(), outInput0.getByteArray());
                                            break;
                                        case LINE:
                                            res = linePolygonIntersection(outInput1.getByteArray(),
                                                    outInput0.getByteArray());
                                            break;
                                        case POLYGON:
                                            int numOfPoints0 = AInt16SerializerDeserializer.getShort(
                                                    outInput0.getByteArray(),
                                                    APolygonSerializerDeserializer.getNumberOfPointsOffset());
                                            int numOfPoints1 = AInt16SerializerDeserializer.getShort(
                                                    outInput1.getByteArray(),
                                                    APolygonSerializerDeserializer.getNumberOfPointsOffset());

                                            if (numOfPoints0 < 3 || numOfPoints1 < 3) {
                                                throw new AlgebricksException("Polygon must have at least 3 points.");
                                            }

                                            getCounterClockWisePolygon(outInput0.getByteArray(), pointsOffsets0,
                                                    numOfPoints0);
                                            getCounterClockWisePolygon(outInput1.getByteArray(), pointsOffsets1,
                                                    numOfPoints1);
                                            int nonSimplePolygonDetection0 = 2 * numOfPoints0;
                                            int nonSimplePolygonDetection1 = 2 * numOfPoints1;
                                            boolean intersect = false;
                                            int middleVertex0 = numOfPoints0 - 1;

                                            int numOfTriangles1 = 0;
                                            int middleVertex1 = numOfPoints1 - 1;
                                            trianglesX1.reset();
                                            trianglesY1.reset();
                                            while (true) {
                                                middleVertex1 = triangulatePolygon(outInput1.getByteArray(),
                                                        numOfPoints1, pointsOffsets1, trianglesX1, trianglesY1,
                                                        numOfTriangles1, nonSimplePolygonDetection1, middleVertex1);

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
                                                middleVertex0 = triangulatePolygon(outInput0.getByteArray(),
                                                        numOfPoints0, pointsOffsets0, trianglesX0, trianglesY0,
                                                        numOfTriangles0, nonSimplePolygonDetection0, middleVertex0);

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
                                            res = polygonCircleIntersection(outInput0.getByteArray(),
                                                    outInput1.getByteArray());
                                            break;
                                        case RECTANGLE:
                                            res = rectanglePolygonIntersection(outInput1.getByteArray(),
                                                    outInput0.getByteArray());
                                            break;
                                        case NULL:
                                            res = false;
                                            break;
                                        default:
                                            throw new NotImplementedException(
                                                    AsterixBuiltinFunctions.SPATIAL_INTERSECT.getName()
                                                            + ": does not support the type: "
                                                            + tag1
                                                            + "; it is only implemented for POINT, ALINE, POLYGON, and CIRCLE.");
                                    }
                                    break;
                                case CIRCLE:
                                    switch (tag1) {
                                        case POINT:
                                            res = pointInCircle(outInput1.getByteArray(), outInput0.getByteArray());
                                            break;
                                        case LINE:
                                            res = lineCircleIntersection(outInput1.getByteArray(),
                                                    outInput0.getByteArray());
                                            break;
                                        case POLYGON:
                                            res = polygonCircleIntersection(outInput1.getByteArray(),
                                                    outInput0.getByteArray());
                                            break;
                                        case CIRCLE:
                                            res = circleCircleIntersection(outInput0.getByteArray(),
                                                    outInput1.getByteArray());
                                            break;
                                        case RECTANGLE:
                                            res = rectangleCircleIntersection(outInput1.getByteArray(),
                                                    outInput0.getByteArray());
                                            break;
                                        case NULL:
                                            res = false;
                                            break;
                                        default:
                                            throw new NotImplementedException(
                                                    AsterixBuiltinFunctions.SPATIAL_INTERSECT.getName()
                                                            + ": does not support the type: "
                                                            + tag1
                                                            + "; it is only implemented for POINT, ALINE, POLYGON, and CIRCLE.");
                                    }
                                    break;
                                case RECTANGLE:
                                    switch (tag1) {
                                        case POINT:
                                            res = pointInRectangle(outInput1.getByteArray(), outInput0.getByteArray());
                                            break;
                                        case LINE:
                                            res = lineRectangleIntersection(outInput1.getByteArray(),
                                                    outInput0.getByteArray());
                                            break;
                                        case POLYGON:
                                            res = rectanglePolygonIntersection(outInput0.getByteArray(),
                                                    outInput1.getByteArray());
                                            break;
                                        case CIRCLE:
                                            res = rectangleCircleIntersection(outInput0.getByteArray(),
                                                    outInput1.getByteArray());
                                            break;
                                        case RECTANGLE:
                                            triangulateRectangle(outInput0.getByteArray(), trianglesX0, trianglesY0);
                                            triangulateRectangle(outInput1.getByteArray(), trianglesX1, trianglesY1);

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
                                        case NULL:
                                            res = false;
                                            break;
                                        default:
                                            throw new NotImplementedException(
                                                    AsterixBuiltinFunctions.SPATIAL_INTERSECT.getName()
                                                            + ": does not support the type: "
                                                            + tag1
                                                            + "; it is only implemented for POINT, ALINE, POLYGON, and CIRCLE.");
                                    }
                                    break;
                                case NULL:
                                    res = false;
                                    break;
                                default:
                                    throw new NotImplementedException(
                                            AsterixBuiltinFunctions.SPATIAL_INTERSECT.getName()
                                                    + ": does not support the type: " + tag1
                                                    + "; it is only implemented for POINT, ALINE, POLYGON, and CIRCLE.");
                            }

                            ABoolean aResult = res ? (ABoolean.TRUE) : (ABoolean.FALSE);
                            AObjectSerializerDeserializer.INSTANCE.serialize(aResult, out);
                        } catch (HyracksDataException hde) {
                            throw new AlgebricksException(hde);
                        }
                    }
                };
            }
        };
    }

    @Override
    public FunctionIdentifier getIdentifier() {
        return AsterixBuiltinFunctions.SPATIAL_INTERSECT;
    }
}