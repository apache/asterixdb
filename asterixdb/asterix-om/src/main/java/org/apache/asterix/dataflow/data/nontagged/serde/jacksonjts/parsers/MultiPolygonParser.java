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
 * Original source: https://github.com/bedatadriven/jackson-datatype-jts/blob/master/src/main/java/com/bedatadriven/jackson/datatype/jts/parsers/MultiPolygonParser.java.
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
import org.locationtech.jts.geom.MultiPolygon;
import org.locationtech.jts.geom.Polygon;

import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.JsonNode;

/**
 * A parser for converting GeoJSON multi-polygon data into JTS {@link MultiPolygon} objects using a specified {@link GeometryFactory}.
 * This class leverages a helper {@link PolygonParser} to parse individual polygons from a collection of polygons represented
 * in a GeoJSON format. It supports parsing complex multi-polygon geometries, which can include multiple outer boundaries and
 * their respective inner holes.
 * The parser extends {@code BaseParser} to make use of common functionalities and ensure the consistent application of the
 * {@link GeometryFactory} in creating {@link MultiPolygon} instances.
 */
public class MultiPolygonParser extends BaseParser implements GeometryParser<MultiPolygon> {

    private PolygonParser helperParser;

    public MultiPolygonParser(GeometryFactory geometryFactory) {
        super(geometryFactory);
        helperParser = new PolygonParser(geometryFactory);
    }

    public MultiPolygon multiPolygonFromJson(JsonNode root) {
        JsonNode arrayOfPolygons = root.get(COORDINATES);
        return geometryFactory.createMultiPolygon(polygonsFromJson(arrayOfPolygons));
    }

    private Polygon[] polygonsFromJson(JsonNode arrayOfPolygons) {
        Polygon[] polygons = new Polygon[arrayOfPolygons.size()];
        for (int i = 0; i != arrayOfPolygons.size(); ++i) {
            polygons[i] = helperParser.polygonFromJsonArrayOfRings(arrayOfPolygons.get(i));
        }
        return polygons;
    }

    @Override
    public MultiPolygon geometryFromJson(JsonNode node) throws JsonMappingException {
        return multiPolygonFromJson(node);
    }
}
