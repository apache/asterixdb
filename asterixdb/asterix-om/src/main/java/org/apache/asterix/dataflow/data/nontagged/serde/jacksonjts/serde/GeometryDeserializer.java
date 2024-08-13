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
 * Original source: https://github.com/bedatadriven/jackson-datatype-jts/blob/master/src/main/java/com/bedatadriven/jackson/datatype/jts/serialization/GeometryDeserializer.java.
 *
 * Modifications:
 * - Adapted the code to support the org.locationtech.jts package instead of com.vividsolutions.jts
 *
 * The modified version retains the original license and notices. For more information
 * on the original project and licensing, please visit https://github.com/bedatadriven/jackson-datatype-jts.
 */
package org.apache.asterix.dataflow.data.nontagged.serde.jacksonjts.serde;

import java.io.IOException;

import org.apache.asterix.dataflow.data.nontagged.serde.jacksonjts.parsers.GeometryParser;
import org.locationtech.jts.geom.Geometry;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.ObjectCodec;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonNode;

/**
 * A custom Jackson deserializer for JTS Geometry objects.
 * This deserializer translates JSON structures into JTS Geometry instances using a specified
 * {@link GeometryParser}. It supports generic geometry types, allowing for flexible deserialization
 * of various specific types of geometries such as Point, LineString, Polygon, etc.
 * The deserializer relies on a geometry parser which must be provided during instantiation.
 * The parser is responsible for converting a JSON node into a corresponding JTS Geometry object.
 * Usage:
 * This deserializer is registered in the Jackson JTS
 * module {@link org.apache.asterix.dataflow.data.nontagged.serde.jacksonjts.JtsModule} and is used to configure an
 * ObjectMapper to support JTS geometries.
 */
public class GeometryDeserializer<T extends Geometry> extends JsonDeserializer<T> {

    private GeometryParser<T> geometryParser;

    public GeometryDeserializer(GeometryParser<T> geometryParser) {
        this.geometryParser = geometryParser;
    }

    /**
     * Deserializes a JSON node into a JTS Geometry object.
     * The JSON node is processed by the configured GeometryParser to produce the Geometry instance.
     *
     * @param jsonParser the Jackson parser reading the JSON content
     * @param deserializationContext the Jackson deserialization context
     * @return the deserialized JTS Geometry object
     * @throws IOException if there is an issue in reading or parsing the JSON node
     */
    @Override
    public T deserialize(JsonParser jsonParser, DeserializationContext deserializationContext) throws IOException {
        ObjectCodec oc = jsonParser.getCodec();
        JsonNode root = oc.readTree(jsonParser);
        return geometryParser.geometryFromJson(root);
    }
}
