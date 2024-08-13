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
import org.apache.commons.lang3.StringUtils;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryCollection;
import org.locationtech.jts.geom.MultiPolygon;
import org.locationtech.jts.geom.Polygon;

public class STNPointsDescriptor extends AbstractSTSingleGeometryDescriptor {

    private static final long serialVersionUID = 1L;
    public static final IFunctionDescriptorFactory FACTORY = STNPointsDescriptor::new;

    @Override
    protected Object evaluateOGCGeometry(Geometry geometry) throws HyracksDataException {
        if (geometry == null) {
            return 0;
        }
        if (geometry.isEmpty()) {
            return 0;
        }

        if (StringUtils.equals(geometry.getGeometryType(), Geometry.TYPENAME_POINT))
            return 1;

        if (StringUtils.equals(geometry.getGeometryType(), Geometry.TYPENAME_POLYGON)) {
            Polygon polygon = (Polygon) geometry;
            int count = polygon.getExteriorRing().getCoordinates().length - 1;
            for (int i = 0; i < polygon.getNumInteriorRing(); i++) {
                count += polygon.getInteriorRingN(i).getCoordinates().length - 1;
            }
            return count;
        }

        if (StringUtils.equals(geometry.getGeometryType(), Geometry.TYPENAME_MULTIPOLYGON)) {
            int count = 0;
            MultiPolygon multiPolygon = (MultiPolygon) geometry;
            for (int i = 0; i < multiPolygon.getNumGeometries(); i++) {
                count += (int) evaluateOGCGeometry(multiPolygon.getGeometryN(i));
            }
            return count;
        }

        if (StringUtils.equals(geometry.getGeometryType(), Geometry.TYPENAME_GEOMETRYCOLLECTION)) {
            int count = 0;
            GeometryCollection collection = (GeometryCollection) geometry;
            for (int i = 0; i < collection.getNumGeometries(); i++) {
                count += (int) evaluateOGCGeometry(collection.getGeometryN(i));
            }
            return count;
        }

        return geometry.getCoordinates().length;
    }

    @Override
    public FunctionIdentifier getIdentifier() {
        return BuiltinFunctions.ST_N_POINTS;
    }

}
