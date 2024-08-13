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
package org.apache.asterix.dataflow.data.nontagged.serde.jacksonjts;

/**
 * Constants used for GeoJSON serialization and deserialization of JTS geometries.
 * This class provides string constants that represent various GeoJSON object types
 * and properties, such as types of geometries and common GeoJSON attributes like
 * coordinates and type.
 * <p>
 * These constants are used throughout the Jackson JTS module to ensure consistency
 * in processing and generating GeoJSON.
 */
public class GeoJsonConstants {
    public static final String POINT = "Point";
    public static final String LINE_STRING = "LineString";
    public static final String POLYGON = "Polygon";

    public static final String MULTI_POINT = "MultiPoint";
    public static final String MULTI_LINE_STRING = "MultiLineString";
    public static final String MULTI_POLYGON = "MultiPolygon";

    public static final String GEOMETRY_COLLECTION = "GeometryCollection";

    public static final String TYPE = "type";

    public static final String GEOMETRIES = "geometries";

    public static final String COORDINATES = "coordinates";
}