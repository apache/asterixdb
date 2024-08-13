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
 * Original source: https://github.com/bedatadriven/jackson-datatype-jts/blob/master/src/main/java/com/bedatadriven/jackson/datatype/jts/parsers/GeometryParser.java.
 *
 * Modifications:
 * - Adapted the code to support the org.locationtech.jts package instead of com.vividsolutions.jts
 *
 * The modified version retains the original license and notices. For more information
 * on the original project and licensing, please visit https://github.com/bedatadriven/jackson-datatype-jts.
 */
package org.apache.asterix.dataflow.data.nontagged.serde.jacksonjts.parsers;

import org.locationtech.jts.geom.Geometry;

import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.JsonNode;

/**
 * An interface for parsing GeoJSON data into JTS {@link Geometry} objects.
 * This interface defines a contract for classes that convert JSON representations of geometries
 * into specific JTS geometry instances, such as Points, LineStrings, Polygons, etc.
 * The {@code GeometryParser} interface ensures a standard method is available for deserializing
 * GeoJSON structures into their corresponding JTS geometrical forms. Implementations of this interface
 * are responsible for handling the parsing logic for different types of geometries.
 *
 * @param <T> the type of JTS Geometry that the parser will produce, such as Point, LineString, Polygon, etc.
 */
public interface GeometryParser<T extends Geometry> {

    T geometryFromJson(JsonNode node) throws JsonMappingException;

}
