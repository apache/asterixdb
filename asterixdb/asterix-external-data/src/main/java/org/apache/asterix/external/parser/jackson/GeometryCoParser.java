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
package org.apache.asterix.external.parser.jackson;

import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.common.exceptions.RuntimeDataException;
import org.apache.asterix.dataflow.data.nontagged.serde.APointSerializerDeserializer;
import org.apache.asterix.om.types.ATypeTag;

import com.fasterxml.jackson.core.JsonParser;

/**
 * Co-parser for JsonDataParser to help parse GeoJSON objects.
 * It only supports POINT, LINE (with only two points) and Polygon.
 * The co-parser can parse GeoJSON geometry object as in {@link https://tools.ietf.org/html/rfc7946}
 * and converts it into a supported AsterixDB geometry type.
 *
 * Example:
 * { ..., "geometry":{"type":"Point", "coordinates":[1.0, 1.0]}, ...}
 * will be:
 * { ..., "geometry":point("1.0, 1.0"), ...}
 */
public class GeometryCoParser {

    //Geometry object fields
    private static final String DEFAULT_GEOMETERY_KEY = "geometry";
    private static final String COORDINATES_KEY = "coordinates";
    private static final String TYPE_KEY = "type";

    //Supported geometry
    private static final List<String> SUPPORTED_GEOMETRY =
            Collections.unmodifiableList(Arrays.asList("Point", "LineString", "Polygon"));
    private static final List<ATypeTag> SUPPORTED_GEOMETRY_TYPETAG =
            Collections.unmodifiableList(Arrays.asList(ATypeTag.POINT, ATypeTag.LINE, ATypeTag.POLYGON));

    //Error messages
    private static final String INVALID_GEOMETRY = "Invalid GeoJSON geometry object";
    private static final String UNSUPPORTED_GEOMETRY = "Unsupported geometry type ";

    private static final int POINT = 0;
    private static final int LINE = 1;
    private static final int POLYGON = 2;

    private final List<Double> coordinates;
    private JsonParser jsonParser;
    private String currentField;
    private int geometryType;
    private String geometryTypeString;

    private int currentCoordinateLevel;
    private int coordinateValueLevel;
    private int coordinatesCounter;
    private String errorMsg;

    /**
     * @param jsonParser
     * @param geometryFieldName
     *            override the default geometry
     *            field name {@value GeometryCoParser#DEFAULT_GEOMETERY_KEY}
     */
    public GeometryCoParser(JsonParser jsonParser) {
        this.jsonParser = jsonParser;
        coordinates = new ArrayList<>();
        currentField = null;
        geometryTypeString = null;
        geometryType = -1;
        currentCoordinateLevel = 0;
        coordinateValueLevel = 0;
        coordinatesCounter = 0;
        errorMsg = null;
    }

    /*
     ****************************************************
     * Public methods
     ****************************************************
     */

    /**
     * Check the field name if it's one of GeoJSON know fields
     *
     * @param fieldName
     * @return
     *         returns true if the field name equals {@value GeometryCoParser#geometryFieldName},
     *         which marks the start of a geometry object.
     */
    public void checkFieldName(String fieldName) {
        if (COORDINATES_KEY.equals(fieldName)) {
            currentField = COORDINATES_KEY;
        } else if (TYPE_KEY.equals(fieldName)) {
            currentField = TYPE_KEY;
        } else {
            reset("Invalid geometry object");
        }
    }

    /**
     * Given the state of the current token in a geometry object, parse the potential values depends
     * on the current field name.
     * In the case of failure, the co-parser will stop parsing without throwing exception.
     *
     * @param token
     * @throws IOException
     * @return
     *         true: if it's an expected value.
     *         false: otherwise.
     */
    public boolean checkValue(ADMToken token) throws IOException {
        if (currentField == null) {
            return false;
        }
        if (currentField == DEFAULT_GEOMETERY_KEY && token != ADMToken.OBJECT_START) {
            reset(INVALID_GEOMETRY);
        } else if (currentField == COORDINATES_KEY) {
            parseCoordinates(token);
        } else if (currentField == TYPE_KEY) {
            if (token != ADMToken.STRING) {
                //unexpected token
                reset(INVALID_GEOMETRY);
            } else {
                geometryTypeString = jsonParser.getValueAsString();
                geometryType = SUPPORTED_GEOMETRY.indexOf(geometryTypeString);
                if (geometryType < 0) {
                    reset(UNSUPPORTED_GEOMETRY + geometryTypeString);
                }
            }
        }
        return currentField != null;
    }

    /**
     * To begin parsing a defined geometry object.
     */
    public void starGeometry() {
        reset("");
        currentField = DEFAULT_GEOMETERY_KEY;
    }

    /**
     * Serialize the parsed geometry
     *
     * @param typeTag
     *            The expected typeTag of the geometry type.
     * @param out
     * @throws IOException
     *             an exception will be thrown in case of failure or type mismatch.
     */
    public void serialize(ATypeTag typeTag, DataOutput out) throws IOException {
        if (!isValidGeometry()) {
            throw new IOException(errorMsg);
        } else if (typeTag != SUPPORTED_GEOMETRY_TYPETAG.get(geometryType)) {
            throw new RuntimeDataException(ErrorCode.PARSER_ADM_DATA_PARSER_TYPE_MISMATCH, typeTag);
        }

        switch (geometryType) {
            case POINT:
                serializePoint(out);
                break;
            case LINE:
                serializeLine(out);
                break;
            case POLYGON:
                serializePolygon(out);
                break;
            default:
                break;
        }
    }

    public String getErrorMessage() {
        return errorMsg;
    }

    /**
     * Reset for a new record to be parsed.
     *
     * @param jsonParser
     */
    public void reset(JsonParser jsonParser) {
        reset("");
        this.jsonParser = jsonParser;
    }

    /*
     ****************************************************
     * Helper methods
     ****************************************************
     */

    /**
     * Parse coordinates values.
     *
     * @param token
     * @throws IOException
     */
    private void parseCoordinates(ADMToken token) throws IOException {
        if (token == ADMToken.DOUBLE) {
            if (++coordinatesCounter > 2) {
                //A point must have 2 coordinates
                reset(INVALID_GEOMETRY);
            }

            coordinates.add(jsonParser.getDoubleValue());

            if (coordinateValueLevel == 0) {
                coordinateValueLevel = currentCoordinateLevel;
            }
        } else if (token == ADMToken.ARRAY_START) {
            currentCoordinateLevel++;
            if (coordinateValueLevel - (currentCoordinateLevel - 1) > 1) {
                reset("Only simple geometries are supported (Point, LineString and Polygon without holes)");
            }
        } else if (token == ADMToken.ARRAY_END) {
            currentCoordinateLevel--;
            coordinatesCounter = 0;
        } else {
            //unexpected token
            reset(INVALID_GEOMETRY);
        }
    }

    private void reset(String errorMsg) {
        coordinates.clear();
        geometryType = -1;
        currentField = null;
        coordinatesCounter = 0;
        coordinateValueLevel = 0;
        currentCoordinateLevel = 0;
        this.errorMsg = errorMsg;
    }

    private boolean isValidGeometry() {
        boolean valid;
        switch (geometryType) {
            case POINT:
                valid = coordinateValueLevel == 1 && coordinates.size() == 2;
                errorMsg = valid ? null : "Point must have 2 coordinates";
                break;
            case LINE:
                valid = coordinateValueLevel == 2 && coordinates.size() == 4;
                errorMsg = valid ? null : "Line must have 4 coordinates";
                break;
            case POLYGON:
                valid = isValidPolygon();
                break;
            default:
                valid = false;
                errorMsg = UNSUPPORTED_GEOMETRY + geometryTypeString;
        }

        return valid;
    }

    private boolean isValidPolygon() {
        /*
         * A valid polygon should have at least 3 points and should start and end at the same point.
         */
        final int size = coordinates.size();
        if (size < 5) {
            errorMsg = "Polygon must consists of at least 3 points (6 coordinates)";
            return false;
        } else if (coordinateValueLevel != 3) {
            errorMsg = "MultiPolygon is not supported";
        } else if (!(coordinates.get(0).equals(coordinates.get(size - 2))
                && coordinates.get(1).equals(coordinates.get(size - 1)))) {
            errorMsg = "Unclosed polygon is not supported";
            return false;
        }
        return true;
    }

    private void serializePoint(DataOutput out) throws IOException {
        out.writeByte(ATypeTag.SERIALIZED_POINT_TYPE_TAG);
        APointSerializerDeserializer.serialize(coordinates.get(0), coordinates.get(1), out);
    }

    private void serializeLine(DataOutput out) throws IOException {
        out.writeByte(ATypeTag.SERIALIZED_LINE_TYPE_TAG);
        APointSerializerDeserializer.serialize(coordinates.get(0), coordinates.get(1), out);
        APointSerializerDeserializer.serialize(coordinates.get(2), coordinates.get(3), out);
    }

    private void serializePolygon(DataOutput out) throws IOException {
        out.writeByte(ATypeTag.SERIALIZED_POLYGON_TYPE_TAG);
        out.writeShort(coordinates.size() / 2);
        for (int i = 0; i < coordinates.size(); i += 2) {
            APointSerializerDeserializer.serialize(coordinates.get(i), coordinates.get(i + 1), out);
        }

    }

}
