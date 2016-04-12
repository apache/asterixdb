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
package org.apache.asterix.runtime.evaluators.common;

import org.apache.asterix.dataflow.data.nontagged.Coordinate;
import org.apache.asterix.dataflow.data.nontagged.serde.ADoubleSerializerDeserializer;
import org.apache.asterix.dataflow.data.nontagged.serde.APolygonSerializerDeserializer;
import org.apache.hyracks.api.exceptions.HyracksDataException;

public class SpatialUtils {
    private double xAxis;
    private double yAxis;
    private double minProjection;
    private double maxProjection;
    private static final double doubleEpsilon = computeDoubleEpsilon();
    private static final double pi = 3.14159265;

    public SpatialUtils() {
    }

    private static double computeDoubleEpsilon() {
        double doubleEpsilon = 1.0;

        do {
            doubleEpsilon /= 2.0;
        } while (1.0 + (doubleEpsilon / 2.0) != 1.0);
        return doubleEpsilon;
    }

    public static double doubleEpsilon() {
        return doubleEpsilon;
    }

    public double getMinProjection() {
        return minProjection;
    }

    public void setMinProjection(double minProjection) {
        this.minProjection = minProjection;
    }

    public double getMaxProjection() {
        return maxProjection;
    }

    public void setMaxProjection(double maxProjection) {
        this.maxProjection = maxProjection;
    }

    public double getXAxis() {
        return xAxis;
    }

    public void setXAxis(double xAxis) {
        this.xAxis = xAxis;
    }

    public double getYAxis() {
        return yAxis;
    }

    public void setYAxis(double yAxis) {
        this.yAxis = yAxis;
    }

    public final static double pi() {
        return pi;
    }

    public final static double dotProduct(double x1, double y1, double x2, double y2) {
        return x1 * x2 + y1 * y2;
    }

    public final static double crossProduct(double x1, double y1, double x2, double y2) {
        return x1 * y2 - y1 * x2;
    }

    // Warning: The caller is responsible of taking the absolute value
    public final static double polygonArea(byte[] bytes, int offset, int numOfPoints) throws HyracksDataException {
        double area = 0.0;
        for (int i = 0; i < numOfPoints; i++) {

            double x1 = ADoubleSerializerDeserializer.getDouble(bytes,
                    offset + APolygonSerializerDeserializer.getCoordinateOffset(i, Coordinate.X));
            double y1 = ADoubleSerializerDeserializer.getDouble(bytes,
                    offset + APolygonSerializerDeserializer.getCoordinateOffset(i, Coordinate.Y));
            double x2;
            double y2;
            if (i + 1 == numOfPoints) {
                x2 = ADoubleSerializerDeserializer.getDouble(bytes,
                        offset + APolygonSerializerDeserializer.getCoordinateOffset(0, Coordinate.X));
                y2 = ADoubleSerializerDeserializer.getDouble(bytes,
                        offset + APolygonSerializerDeserializer.getCoordinateOffset(0, Coordinate.Y));
            } else {
                x2 = ADoubleSerializerDeserializer.getDouble(bytes,
                        offset + APolygonSerializerDeserializer.getCoordinateOffset(i + 1, Coordinate.X));
                y2 = ADoubleSerializerDeserializer.getDouble(bytes,
                        offset + APolygonSerializerDeserializer.getCoordinateOffset(i + 1, Coordinate.Y));
            }
            area += (x1 * y2) - (x2 * y1);
        }
        return area * 0.5;
    }

    public void projectPolygon(DoubleArray trianglesX, DoubleArray trianglesY, int triangleId, double xAxis,
            double yAxis) throws HyracksDataException {
        double temp, min, max;
        min = max = getTriangleXCoordinate(trianglesX, triangleId, 0) * xAxis
                + getTriangleYCoordinate(trianglesY, triangleId, 0) * yAxis;

        for (int i = 1; i < 3; i++) {
            temp = getTriangleXCoordinate(trianglesX, triangleId, i) * xAxis
                    + getTriangleYCoordinate(trianglesY, triangleId, i) * yAxis;

            if (temp > max) {
                max = temp;
            } else if (temp < min) {
                min = temp;
            }
        }
        setMinProjection(min);
        setMaxProjection(max);
    }

    public void findNormals(DoubleArray trianglesX, DoubleArray trianglesY, int triangleId, int side)
            throws HyracksDataException {
        double x, y;
        if (side == 0) {
            x = getTriangleYCoordinate(trianglesY, triangleId, 2)
                    - getTriangleYCoordinate(trianglesY, triangleId, side);

            y = getTriangleXCoordinate(trianglesX, triangleId, side)
                    - getTriangleXCoordinate(trianglesX, triangleId, 2);

        } else {
            x = getTriangleYCoordinate(trianglesY, triangleId, side - 1)
                    - getTriangleYCoordinate(trianglesY, triangleId, side);

            y = getTriangleXCoordinate(trianglesX, triangleId, side)
                    - getTriangleXCoordinate(trianglesX, triangleId, side - 1);
        }
        double temp = Math.sqrt(dotProduct(x, y, x, y));
        x /= temp;
        y /= temp;
        setXAxis(x);
        setYAxis(y);
    }

    public static double getTriangleXCoordinate(DoubleArray trianglesX, int triangleId, int point) {
        return trianglesX.get(triangleId * 3 + point);
    }

    public static double getTriangleYCoordinate(DoubleArray trianglesY, int triangleId, int point) {
        return trianglesY.get(triangleId * 3 + point);
    }

    public static void setTriangleXCoordinate(DoubleArray trianglesX, int triangleId, int point, double value) {
        trianglesX.get()[triangleId * 3 + point] = value;
    }

    public static void setTriangleYCoordinate(DoubleArray trianglesY, int triangleId, int point, double value) {
        trianglesY.get()[triangleId * 3 + point] = value;
    }
}
