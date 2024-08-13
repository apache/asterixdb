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
/*
 * This file includes code derived from the project "jackson-datatype-jts"
 * under the Apache License 2.0.
 * Original source: https://github.com/bedatadriven/jackson-datatype-jts/blob/master/src/main/java/com/bedatadriven/jackson/datatype/jts/parsers/PointParser.java.
 *
 * Modifications:
 * - Adapted the code to support the org.locationtech.jts package instead of com.vividsolutions.jts
 *
 * The modified version retains the original license and notices. For more information
 * on the original project and licensing, please visit https://github.com/bedatadriven/jackson-datatype-jts.
 */
package org.apache.asterix.dataflow.data.nontagged.serde.jacksonjts.parsers;

import static org.apache.asterix.dataflow.data.nontagged.serde.jacksonjts.GeoJsonConstants.COORDINATES;

import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.CoordinateXYZM;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.Point;

import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.JsonNode;

/**
 * Parses JSON representations of GeoJSON Points into JTS {@link Point} objects.
 * This parser handles the extraction of coordinates from GeoJSON and converts them
 * into {@link Point} geometries using a provided {@link GeometryFactory}.
 *
 * This class supports reading points defined with two-dimensional (x, y),
 * three-dimensional (x, y, z), and four-dimensional (x, y, z, m) coordinates.
 */
public class PointParser extends BaseParser implements GeometryParser<Point> {

    public PointParser(GeometryFactory geometryFactory) {
        super(geometryFactory);
    }

    public static Coordinate coordinateFromJson(JsonNode array) {
        assert array.isArray() && (array.size() == 2 || array.size() == 3
                || array.size() == 4) : "expecting coordinate array with single point [ x, y, |z|, |m| ]";

        if (array.size() == 2) {
            return new Coordinate(array.get(0).asDouble(), array.get(1).asDouble());
        }

        if (array.size() == 3) {
            return new Coordinate(array.get(0).asDouble(), array.get(1).asDouble(), array.get(2).asDouble());
        }

        return new CoordinateXYZM(array.get(0).asDouble(), array.get(1).asDouble(), array.get(2).asDouble(),
                array.get(3).asDouble());
    }

    public static Coordinate[] coordinatesFromJson(JsonNode array) {
        Coordinate[] points = new Coordinate[array.size()];
        for (int i = 0; i != array.size(); ++i) {
            points[i] = PointParser.coordinateFromJson(array.get(i));
        }
        return points;
    }

    public Point pointFromJson(JsonNode node) {
        return geometryFactory.createPoint(coordinateFromJson(node.get(COORDINATES)));
    }

    @Override
    public Point geometryFromJson(JsonNode node) throws JsonMappingException {
        return pointFromJson(node);
    }
}
