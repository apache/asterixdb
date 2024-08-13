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
 * Original source: https://github.com/bedatadriven/jackson-datatype-jts/blob/master/src/main/java/com/bedatadriven/jackson/datatype/jts/parsers/PolygonParser.java.
 *
 * Modifications:
 * - Adapted the code to support the org.locationtech.jts package instead of com.vividsolutions.jts
 *
 * The modified version retains the original license and notices. For more information
 * on the original project and licensing, please visit https://github.com/bedatadriven/jackson-datatype-jts.
 */
package org.apache.asterix.dataflow.data.nontagged.serde.jacksonjts.parsers;

import static org.apache.asterix.dataflow.data.nontagged.serde.jacksonjts.GeoJsonConstants.COORDINATES;

import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.LinearRing;
import org.locationtech.jts.geom.Polygon;

import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.JsonNode;

/**
 * A parser for transforming GeoJSON polygon data into JTS {@link Polygon} objects using a specified {@link GeometryFactory}.
 * This class handles the parsing of polygons, which may include an outer boundary and any number of inner holes.
 * Each polygon is defined by arrays of coordinates that represent linear rings—the first array defines the exterior boundary,
 * and any subsequent arrays define interior holes.
 * This parser extends {@code BaseParser} to leverage shared functionality and ensure consistent application of the
 * {@link GeometryFactory} in creating polygon geometries.
 */
public class PolygonParser extends BaseParser implements GeometryParser<Polygon> {

    public PolygonParser(GeometryFactory geometryFactory) {
        super(geometryFactory);
    }

    public Polygon polygonFromJson(JsonNode node) {
        JsonNode arrayOfRings = node.get(COORDINATES);
        return polygonFromJsonArrayOfRings(arrayOfRings);
    }

    public Polygon polygonFromJsonArrayOfRings(JsonNode arrayOfRings) {
        LinearRing shell = linearRingsFromJson(arrayOfRings.get(0));
        int size = arrayOfRings.size();
        LinearRing[] holes = new LinearRing[size - 1];
        for (int i = 1; i < size; i++) {
            holes[i - 1] = linearRingsFromJson(arrayOfRings.get(i));
        }
        return geometryFactory.createPolygon(shell, holes);
    }

    private LinearRing linearRingsFromJson(JsonNode coordinates) {
        assert coordinates.isArray() : "expected coordinates array";
        return geometryFactory.createLinearRing(PointParser.coordinatesFromJson(coordinates));
    }

    @Override
    public Polygon geometryFromJson(JsonNode node) throws JsonMappingException {
        return polygonFromJson(node);
    }
}
