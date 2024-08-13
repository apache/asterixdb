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
 * Original source: https://github.com/bedatadriven/jackson-datatype-jts/blob/master/src/main/java/com/bedatadriven/jackson/datatype/jts/parsers/MultiPointParser.java.
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
import org.locationtech.jts.geom.MultiPoint;

import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.JsonNode;

/**
 * A parser for converting GeoJSON multi-point data into JTS {@link MultiPoint} objects using a specified {@link GeometryFactory}.
 * This class is capable of parsing JSON representations of multi-point geometries, where each point is represented
 * by an array of coordinates. The parser extends {@code BaseParser} to utilize common functionality and ensure
 * the {@link GeometryFactory} is applied consistently to create {@link MultiPoint} instances.
 */
public class MultiPointParser extends BaseParser implements GeometryParser<MultiPoint> {

    public MultiPointParser(GeometryFactory geometryFactory) {
        super(geometryFactory);
    }

    public MultiPoint multiPointFromJson(JsonNode root) {
        return geometryFactory.createMultiPointFromCoords(PointParser.coordinatesFromJson(root.get(COORDINATES)));
    }

    @Override
    public MultiPoint geometryFromJson(JsonNode node) throws JsonMappingException {
        return multiPointFromJson(node);
    }
}
