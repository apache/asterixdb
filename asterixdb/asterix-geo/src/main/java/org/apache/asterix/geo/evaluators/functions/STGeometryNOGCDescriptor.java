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

import org.apache.asterix.om.functions.BuiltinFunctions;
import org.apache.asterix.om.functions.IFunctionDescriptorFactory;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.locationtech.jts.geom.Geometry;

/**
 * OGC SFA ST_GeometryN: 1-indexed. {@code ST_GeometryN(coll, 1)} returns the
 * first geometry. Accepts any geometry type:
 * <ul>
 *   <li>{@code GeometryCollection}, {@code MultiPoint}, {@code MultiLineString},
 *       {@code MultiPolygon} — returns the Nth component (1-indexed).</li>
 *   <li>Singular geometries ({@code Point}, {@code LineString}, {@code Polygon},
 *       {@code LinearRing}) — returns the geometry itself when N=1.</li>
 * </ul>
 * Returns {@code NULL} when N is less than 1 or greater than the number of
 * components. Distinct from the existing 0-indexed {@code st-geometry-n},
 * which only accepts plain {@code GeometryCollection}.
 */
public class STGeometryNOGCDescriptor extends AbstractSTGeometryNDescriptor {

    public static final IFunctionDescriptorFactory FACTORY = STGeometryNOGCDescriptor::new;

    private static final long serialVersionUID = 1L;

    @Override
    public FunctionIdentifier getIdentifier() {
        return BuiltinFunctions.ST_GEOMETRYN;
    }

    @Override
    protected Geometry evaluateOGCGeometry(Geometry geometry, int n) throws HyracksDataException {
        if (n < 1 || n > geometry.getNumGeometries()) {
            return null;
        }
        return geometry.getGeometryN(n - 1);
    }
}
