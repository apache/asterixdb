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
 * Original source: https://github.com/bedatadriven/jackson-datatype-jts/blob/master/src/main/java/com/bedatadriven/jackson/datatype/jts/parsers/GenericGeometryParser.java.
 *
 * Modifications:
 * - Adapted the code to support the org.locationtech.jts package instead of com.vividsolutions.jts
 *
 * The modified version retains the original license and notices. For more information
 * on the original project and licensing, please visit https://github.com/bedatadriven/jackson-datatype-jts.
 */
package org.apache.asterix.dataflow.data.nontagged.serde.jacksonjts.parsers;

import java.util.HashMap;
import java.util.Map;

import org.apache.asterix.dataflow.data.nontagged.serde.jacksonjts.GeoJsonConstants;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;

import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.JsonNode;

/**
 * A parser for converting various types of GeoJSON geometries into JTS {@link Geometry} objects using a specified {@link GeometryFactory}.
 * This class acts as a general-purpose parser that can handle multiple types of GeoJSON geometries, including Point, MultiPoint,
 * LineString, MultiLineString, Polygon, MultiPolygon, and GeometryCollection. It dynamically delegates the parsing to specific
 * geometry parsers based on the GeoJSON type of the geometry.
 * The parser extends {@code BaseParser} to utilize shared functionality and ensure consistent application of the {@link GeometryFactory}
 * for creating JTS geometry instances. It maintains a registry of individual geometry parsers, each capable of handling a specific
 * type of GeoJSON geometry.
 */
public class GenericGeometryParser extends BaseParser implements GeometryParser<Geometry> {

    private Map<String, GeometryParser> parsers;

    public GenericGeometryParser(GeometryFactory geometryFactory) {
        super(geometryFactory);
        parsers = new HashMap<>();
        parsers.put(GeoJsonConstants.POINT, new PointParser(geometryFactory));
        parsers.put(GeoJsonConstants.MULTI_POINT, new MultiPointParser(geometryFactory));
        parsers.put(GeoJsonConstants.LINE_STRING, new LineStringParser(geometryFactory));
        parsers.put(GeoJsonConstants.MULTI_LINE_STRING, new MultiLineStringParser(geometryFactory));
        parsers.put(GeoJsonConstants.POLYGON, new PolygonParser(geometryFactory));
        parsers.put(GeoJsonConstants.MULTI_POLYGON, new MultiPolygonParser(geometryFactory));
        parsers.put(GeoJsonConstants.GEOMETRY_COLLECTION, new GeometryCollectionParser(geometryFactory, this));
    }

    @Override
    public Geometry geometryFromJson(JsonNode node) throws JsonMappingException {
        String typeName = node.get(GeoJsonConstants.TYPE).asText();
        GeometryParser parser = parsers.get(typeName);
        if (parser != null) {
            return parser.geometryFromJson(node);
        } else {
            throw new JsonMappingException("Invalid geometry type: " + typeName);
        }
    }
}
