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
package org.apache.asterix.geo.evaluators.functions;

import java.util.ArrayList;
import java.util.List;

import org.apache.asterix.om.functions.BuiltinFunctions;
import org.apache.asterix.om.functions.IFunctionDescriptorFactory;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.LineString;
import org.locationtech.jts.geom.Point;
import org.locationtech.jts.geom.Polygon;

/**
 * ST_CollectionExtract: extracts components of the requested type from a
 * geometry collection, returning them as a homogeneous geometry. Type codes:
 * 1 = POINT, 2 = LINESTRING, 3 = POLYGON. Returns:
 * <ul>
 *   <li>{@code NULL} when the type code is out of range or the input geometry
 *       contains no components of the requested type.</li>
 *   <li>A single geometry of the matching type when the input is already that
 *       atomic type and matches the code.</li>
 *   <li>A {@code Multi*} wrapping the matching components otherwise.</li>
 * </ul>
 */
public class STCollectionExtractDescriptor extends AbstractSTGeometryNDescriptor {

    private static final long serialVersionUID = 1L;
    public static final IFunctionDescriptorFactory FACTORY = STCollectionExtractDescriptor::new;

    private static final int TYPE_POINT = 1;
    private static final int TYPE_LINESTRING = 2;
    private static final int TYPE_POLYGON = 3;

    @Override
    public FunctionIdentifier getIdentifier() {
        return BuiltinFunctions.ST_COLLECTION_EXTRACT;
    }

    @Override
    protected Geometry evaluateOGCGeometry(Geometry geometry, int type) throws HyracksDataException {
        if (type < TYPE_POINT || type > TYPE_POLYGON) {
            return null;
        }
        GeometryFactory gf = geometry.getFactory();
        List<Geometry> matches = new ArrayList<>();
        collect(geometry, type, matches);
        if (matches.isEmpty()) {
            return null;
        }
        switch (type) {
            case TYPE_POINT:
                return gf.createMultiPoint(matches.toArray(new Point[0]));
            case TYPE_LINESTRING:
                return gf.createMultiLineString(matches.toArray(new LineString[0]));
            case TYPE_POLYGON:
                return gf.createMultiPolygon(matches.toArray(new Polygon[0]));
            default:
                return null;
        }
    }

    private void collect(Geometry geometry, int type, List<Geometry> out) {
        if (geometry == null || geometry.isEmpty()) {
            return;
        }
        if (matches(geometry, type)) {
            out.add(geometry);
            return;
        }
        for (int i = 0; i < geometry.getNumGeometries(); i++) {
            Geometry child = geometry.getGeometryN(i);
            if (child == geometry) {
                // Atomic geometries return themselves from getGeometryN(0) — avoid infinite recursion.
                return;
            }
            collect(child, type, out);
        }
    }

    private boolean matches(Geometry geometry, int type) {
        switch (type) {
            case TYPE_POINT:
                return geometry instanceof Point;
            case TYPE_LINESTRING:
                return geometry instanceof LineString;
            case TYPE_POLYGON:
                return geometry instanceof Polygon;
            default:
                return false;
        }
    }
}
