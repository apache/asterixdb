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
 * Original source: https://github.com/bedatadriven/jackson-datatype-jts/blob/master/src/main/java/com/bedatadriven/jackson/datatype/jts/serialization/GeometrySerializer.java.
 *
 * Modifications:
 * - Adapted the code to support the org.locationtech.jts package instead of com.vividsolutions.jts
 *
 * The modified version retains the original license and notices. For more information
 * on the original project and licensing, please visit https://github.com/bedatadriven/jackson-datatype-jts.
 */
package org.apache.asterix.dataflow.data.nontagged.serde.jacksonjts.serde;

import java.io.IOException;
import java.util.Arrays;

import org.apache.asterix.dataflow.data.nontagged.serde.jacksonjts.GeoJsonConstants;
import org.locationtech.jts.geom.CoordinateXYZM;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryCollection;
import org.locationtech.jts.geom.LineString;
import org.locationtech.jts.geom.MultiLineString;
import org.locationtech.jts.geom.MultiPoint;
import org.locationtech.jts.geom.MultiPolygon;
import org.locationtech.jts.geom.Point;
import org.locationtech.jts.geom.Polygon;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;

/**
 * A custom Jackson serializer for JTS Geometry objects that translates these objects into their GeoJSON representations.
 * This class supports serialization for all primary JTS geometry types including Point, LineString, Polygon, and their
 * respective collections such as MultiPoint, MultiLineString, MultiPolygon, and GeometryCollection.
 * It handles complex geometries by delegating to specific methods based on the instance type of the geometry object,
 * ensuring that each geometry type is correctly represented according to the GeoJSON standard.
 */
public class GeometrySerializer extends JsonSerializer<Geometry> {

    @Override
    public void serialize(Geometry value, JsonGenerator jsonGenerator, SerializerProvider provider) throws IOException {
        writeGeometry(jsonGenerator, value);
    }

    /**
     * Writes the geometry object to the JsonGenerator. This method determines the type of the geometry
     * and calls the appropriate method to handle the serialization.
     *
     * @param jsonGenerator the JsonGenerator to use for writing the GeoJSON
     * @param value the Geometry object to serialize
     * @throws IOException if an input/output error occurs
     */
    public void writeGeometry(JsonGenerator jsonGenerator, Geometry value) throws IOException {
        if (value instanceof Polygon) {
            writePolygon(jsonGenerator, (Polygon) value);

        } else if (value instanceof Point) {
            writePoint(jsonGenerator, (Point) value);

        } else if (value instanceof MultiPoint) {
            writeMultiPoint(jsonGenerator, (MultiPoint) value);

        } else if (value instanceof MultiPolygon) {
            writeMultiPolygon(jsonGenerator, (MultiPolygon) value);

        } else if (value instanceof LineString) {
            writeLineString(jsonGenerator, (LineString) value);

        } else if (value instanceof MultiLineString) {
            writeMultiLineString(jsonGenerator, (MultiLineString) value);

        } else if (value instanceof GeometryCollection) {
            writeGeometryCollection(jsonGenerator, (GeometryCollection) value);

        } else {
            throw new JsonMappingException(jsonGenerator,
                    "Geometry type " + value.getClass().getName() + " cannot be serialized as GeoJSON."
                            + "Supported types are: "
                            + Arrays.asList(Point.class.getName(), LineString.class.getName(), Polygon.class.getName(),
                                    MultiPoint.class.getName(), MultiLineString.class.getName(),
                                    MultiPolygon.class.getName(), GeometryCollection.class.getName()));
        }
    }

    private void writeGeometryCollection(JsonGenerator jsonGenerator, GeometryCollection value) throws IOException {
        jsonGenerator.writeStartObject();
        jsonGenerator.writeStringField(GeoJsonConstants.TYPE, GeoJsonConstants.GEOMETRY_COLLECTION);
        jsonGenerator.writeArrayFieldStart(GeoJsonConstants.GEOMETRIES);

        for (int i = 0; i != value.getNumGeometries(); ++i) {
            writeGeometry(jsonGenerator, value.getGeometryN(i));
        }

        jsonGenerator.writeEndArray();
        jsonGenerator.writeEndObject();
    }

    private void writeMultiPoint(JsonGenerator jsonGenerator, MultiPoint value) throws IOException {
        jsonGenerator.writeStartObject();
        jsonGenerator.writeStringField(GeoJsonConstants.TYPE, GeoJsonConstants.MULTI_POINT);
        jsonGenerator.writeArrayFieldStart(GeoJsonConstants.COORDINATES);

        for (int i = 0; i != value.getNumGeometries(); ++i) {
            writePointCoordinates(jsonGenerator, (Point) value.getGeometryN(i));
        }

        jsonGenerator.writeEndArray();
        jsonGenerator.writeEndObject();
    }

    private void writeMultiLineString(JsonGenerator jsonGenerator, MultiLineString value) throws IOException {
        jsonGenerator.writeStartObject();
        jsonGenerator.writeStringField(GeoJsonConstants.TYPE, GeoJsonConstants.MULTI_LINE_STRING);
        jsonGenerator.writeArrayFieldStart(GeoJsonConstants.COORDINATES);

        for (int i = 0; i != value.getNumGeometries(); ++i) {
            writeLineStringCoordinates(jsonGenerator, (LineString) value.getGeometryN(i));
        }

        jsonGenerator.writeEndArray();
        jsonGenerator.writeEndObject();
    }

    @Override
    public Class<Geometry> handledType() {
        return Geometry.class;
    }

    private void writeMultiPolygon(JsonGenerator jsonGenerator, MultiPolygon value) throws IOException {
        jsonGenerator.writeStartObject();
        jsonGenerator.writeStringField(GeoJsonConstants.TYPE, GeoJsonConstants.MULTI_POLYGON);
        jsonGenerator.writeArrayFieldStart(GeoJsonConstants.COORDINATES);

        for (int i = 0; i != value.getNumGeometries(); ++i) {
            writePolygonCoordinates(jsonGenerator, (Polygon) value.getGeometryN(i));
        }

        jsonGenerator.writeEndArray();
        jsonGenerator.writeEndObject();
    }

    private void writePolygon(JsonGenerator jsonGenerator, Polygon value) throws IOException {
        jsonGenerator.writeStartObject();
        jsonGenerator.writeStringField(GeoJsonConstants.TYPE, GeoJsonConstants.POLYGON);
        jsonGenerator.writeFieldName(GeoJsonConstants.COORDINATES);
        writePolygonCoordinates(jsonGenerator, value);

        jsonGenerator.writeEndObject();
    }

    private void writePolygonCoordinates(JsonGenerator jsonGenerator, Polygon value) throws IOException {
        jsonGenerator.writeStartArray();
        writeLineStringCoordinates(jsonGenerator, value.getExteriorRing());

        for (int i = 0; i < value.getNumInteriorRing(); ++i) {
            writeLineStringCoordinates(jsonGenerator, value.getInteriorRingN(i));
        }
        jsonGenerator.writeEndArray();
    }

    private void writeLineStringCoordinates(JsonGenerator jsonGenerator, LineString ring) throws IOException {
        jsonGenerator.writeStartArray();
        for (int i = 0; i != ring.getNumPoints(); ++i) {
            Point p = ring.getPointN(i);
            writePointCoordinates(jsonGenerator, p);
        }
        jsonGenerator.writeEndArray();
    }

    private void writeLineString(JsonGenerator jsonGenerator, LineString lineString) throws IOException {
        jsonGenerator.writeStartObject();
        jsonGenerator.writeStringField(GeoJsonConstants.TYPE, GeoJsonConstants.LINE_STRING);
        jsonGenerator.writeFieldName(GeoJsonConstants.COORDINATES);
        writeLineStringCoordinates(jsonGenerator, lineString);
        jsonGenerator.writeEndObject();
    }

    private void writePoint(JsonGenerator jsonGenerator, Point p) throws IOException {
        jsonGenerator.writeStartObject();
        jsonGenerator.writeStringField(GeoJsonConstants.TYPE, GeoJsonConstants.POINT);
        jsonGenerator.writeFieldName(GeoJsonConstants.COORDINATES);
        writePointCoordinates(jsonGenerator, p);
        jsonGenerator.writeEndObject();
    }

    private void writePointCoordinates(JsonGenerator jsonGenerator, Point p) throws IOException {
        jsonGenerator.writeStartArray();

        writeFormattedNumber(jsonGenerator, p.getCoordinate().x);
        writeFormattedNumber(jsonGenerator, p.getCoordinate().y);

        if (!Double.isNaN(p.getCoordinate().z)) {
            writeFormattedNumber(jsonGenerator, p.getCoordinate().z);
        }

        if (p.getCoordinate() instanceof CoordinateXYZM) {
            double m = p.getCoordinate().getM();
            writeFormattedNumber(jsonGenerator, m);
        }
        jsonGenerator.writeEndArray();
    }

    private void writeFormattedNumber(JsonGenerator jsonGenerator, double value) throws IOException {
        if ((value == Math.floor(value)) && !Double.isInfinite(value)) {
            jsonGenerator.writeNumber((int) value);
        } else {
            jsonGenerator.writeNumber(value);
        }
    }

}
