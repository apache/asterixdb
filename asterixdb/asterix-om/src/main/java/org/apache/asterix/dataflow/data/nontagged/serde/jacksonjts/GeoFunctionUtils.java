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
package org.apache.asterix.dataflow.data.nontagged.serde.jacksonjts;

import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Geometry;

public class GeoFunctionUtils {
    public static final int LITTLE_ENDIAN_BYTEORDER = 2;

    /**
     * Returns the dimension of the coordinate based on whether Z or M is defined.
     * Covers XY (2), XYZ and XYM (3), and XYZM (4) via the polymorphic
     * {@code Coordinate.getZ()} / {@code Coordinate.getM()} accessors — these
     * return {@code NaN} on coordinate subclasses that don't store the ordinate
     * (e.g. plain {@code Coordinate}, {@code CoordinateXY}, {@code CoordinateXYM}
     * for Z; plain {@code Coordinate}, {@code CoordinateXY} for M).
     * @param geometry The geometry to check.
     * @return the dimensionality of the coordinate (2, 3 or 4).
     */
    public static int getCoordinateDimension(Geometry geometry) {
        int dimension = 2;
        if (geometry == null || geometry.isEmpty()) {
            return 2;
        }
        Coordinate sample = geometry.getCoordinates()[0];
        if (!Double.isNaN(sample.getZ())) {
            dimension++;
        }
        if (!Double.isNaN(sample.getM())) {
            dimension++;
        }
        return dimension;
    }
}
