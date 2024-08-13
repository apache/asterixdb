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
 * Original source: https://github.com/bedatadriven/jackson-datatype-jts/blob/master/src/main/java/com/bedatadriven/jackson/datatype/jts/parsers/LineStringParser.java.
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
import org.locationtech.jts.geom.LineString;

import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.JsonNode;

/**
 * A parser for converting GeoJSON line string data into JTS {@link LineString} objects using a specified {@link GeometryFactory}.
 * This class parses JSON representations of line strings, which are essentially ordered sets of coordinates that form a continuous line.
 * It supports the extraction and transformation of coordinate arrays from GeoJSON into JTS LineString geometries.
 * The parser extends {@code BaseParser} to utilize common functionalities and ensures the consistent application of the {@link GeometryFactory}
 * for creating {@link LineString} instances.
 */
public class LineStringParser extends BaseParser implements GeometryParser<LineString> {

    public LineStringParser(GeometryFactory geometryFactory) {
        super(geometryFactory);
    }

    public LineString lineStringFromJson(JsonNode root) {
        return geometryFactory.createLineString(PointParser.coordinatesFromJson(root.get(COORDINATES)));
    }

    @Override
    public LineString geometryFromJson(JsonNode node) throws JsonMappingException {
        return lineStringFromJson(node);
    }
}
