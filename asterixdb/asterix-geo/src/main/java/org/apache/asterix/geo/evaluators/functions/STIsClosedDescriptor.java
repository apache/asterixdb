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
import org.locationtech.jts.geom.GeometryCollection;
import org.locationtech.jts.geom.LineString;
import org.locationtech.jts.geom.MultiLineString;

public class STIsClosedDescriptor extends AbstractSTSingleGeometryDescriptor {

    private static final long serialVersionUID = 1L;
    public static final IFunctionDescriptorFactory FACTORY = STIsClosedDescriptor::new;

    @Override
    protected Object evaluateOGCGeometry(Geometry geometry) throws HyracksDataException {
        return isClosed(geometry);
    }

    private boolean isClosed(Geometry geometry) {
        switch (geometry.getGeometryType()) {
            case Geometry.TYPENAME_LINESTRING:
                return ((LineString) geometry).isClosed();

            case Geometry.TYPENAME_MULTILINESTRING:
                return ((MultiLineString) geometry).isClosed();

            case Geometry.TYPENAME_MULTIPOINT:
            case Geometry.TYPENAME_MULTIPOLYGON:
            case Geometry.TYPENAME_POLYGON:
            case Geometry.TYPENAME_POINT:
                return true;

            case Geometry.TYPENAME_GEOMETRYCOLLECTION:
                GeometryCollection geometryCollection = (GeometryCollection) geometry;
                int num = geometryCollection.getNumGeometries();
                for (int i = 0; i < num; ++i) {
                    if (!isClosed(geometryCollection.getGeometryN(i))) {
                        return false;
                    }
                }
                return true;

            default:
                throw new IllegalArgumentException("Unsupported geometry type: " + geometry.getGeometryType());
        }
    }

    @Override
    public FunctionIdentifier getIdentifier() {
        return BuiltinFunctions.ST_IS_CLOSED;
    }

}
