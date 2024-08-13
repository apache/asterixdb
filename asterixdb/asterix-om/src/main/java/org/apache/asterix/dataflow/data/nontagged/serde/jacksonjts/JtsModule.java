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
 * Original source: https://github.com/bedatadriven/jackson-datatype-jts/blob/master/src/main/java/com/bedatadriven/jackson/datatype/jts/JtsModule.java.
 *
 * Modifications:
 * - Adapted the code to support the org.locationtech.jts package instead of com.vividsolutions.jts
 *
 * The modified version retains the original license and notices. For more information
 * on the original project and licensing, please visit https://github.com/bedatadriven/jackson-datatype-jts.
 */
package org.apache.asterix.dataflow.data.nontagged.serde.jacksonjts;

import org.apache.asterix.dataflow.data.nontagged.serde.jacksonjts.parsers.GenericGeometryParser;
import org.apache.asterix.dataflow.data.nontagged.serde.jacksonjts.parsers.GeometryCollectionParser;
import org.apache.asterix.dataflow.data.nontagged.serde.jacksonjts.parsers.LineStringParser;
import org.apache.asterix.dataflow.data.nontagged.serde.jacksonjts.parsers.MultiLineStringParser;
import org.apache.asterix.dataflow.data.nontagged.serde.jacksonjts.parsers.MultiPointParser;
import org.apache.asterix.dataflow.data.nontagged.serde.jacksonjts.parsers.MultiPolygonParser;
import org.apache.asterix.dataflow.data.nontagged.serde.jacksonjts.parsers.PointParser;
import org.apache.asterix.dataflow.data.nontagged.serde.jacksonjts.parsers.PolygonParser;
import org.apache.asterix.dataflow.data.nontagged.serde.jacksonjts.serde.GeometryDeserializer;
import org.apache.asterix.dataflow.data.nontagged.serde.jacksonjts.serde.GeometrySerializer;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryCollection;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.LineString;
import org.locationtech.jts.geom.MultiLineString;
import org.locationtech.jts.geom.MultiPoint;
import org.locationtech.jts.geom.MultiPolygon;
import org.locationtech.jts.geom.Point;
import org.locationtech.jts.geom.Polygon;

import com.fasterxml.jackson.core.Version;
import com.fasterxml.jackson.databind.module.SimpleModule;

/**
 * A Jackson module for serializing and deserializing JTS (Java Topology Suite) geometry objects.
 * This module provides custom serializers and deserializers capable of handling various types
 * of geometries such as Points, LineStrings, Polygons, and their respective multi-part counterparts,
 * as well as GeometryCollections.
 * <p>
 * It leverages a {@link GeometryFactory} for the creation of geometry objects during deserialization,
 * ensuring that geometry objects are constructed appropriately.
 */
public class JtsModule extends SimpleModule {
    private static final long serialVersionUID = 324082011931609589L;

    public JtsModule() {
        this(new GeometryFactory());
    }

    /**
     * Constructs a JtsModule with a specified {@link GeometryFactory}.
     * This constructor allows for customization of the geometry factory used for creating
     * JTS geometry objects, providing flexibility for various precision and srid settings.
     *
     * @param geometryFactory the geometry factory to use for creating geometry objects
     *                        during deserialization
     */
    public JtsModule(GeometryFactory geometryFactory) {
        super("JtsModule", new Version(1, 0, 0, null, null, null));

        addSerializer(Geometry.class, new GeometrySerializer());
        GenericGeometryParser genericGeometryParser = new GenericGeometryParser(geometryFactory);
        addDeserializer(Geometry.class, new GeometryDeserializer<>(genericGeometryParser));
        addDeserializer(Point.class, new GeometryDeserializer<>(new PointParser(geometryFactory)));
        addDeserializer(MultiPoint.class, new GeometryDeserializer<>(new MultiPointParser(geometryFactory)));
        addDeserializer(LineString.class, new GeometryDeserializer<>(new LineStringParser(geometryFactory)));
        addDeserializer(MultiLineString.class, new GeometryDeserializer<>(new MultiLineStringParser(geometryFactory)));
        addDeserializer(Polygon.class, new GeometryDeserializer<>(new PolygonParser(geometryFactory)));
        addDeserializer(MultiPolygon.class, new GeometryDeserializer<>(new MultiPolygonParser(geometryFactory)));
        addDeserializer(GeometryCollection.class,
                new GeometryDeserializer<>(new GeometryCollectionParser(geometryFactory, genericGeometryParser)));
    }

    @Override
    public void setupModule(SetupContext context) {
        super.setupModule(context);
    }
}
