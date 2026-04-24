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

import java.io.Serial;

import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.om.functions.BuiltinFunctions;
import org.apache.asterix.om.functions.IFunctionDescriptorFactory;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import org.apache.hyracks.api.context.IEvaluatorContext;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.exceptions.IWarningCollector;
import org.apache.hyracks.api.exceptions.Warning;
import org.apache.sis.geometry.DirectPosition2D;
import org.apache.sis.referencing.CommonCRS;
import org.apache.sis.referencing.GeodeticCalculator;
import org.apache.sis.referencing.crs.AbstractCRS;
import org.apache.sis.referencing.cs.AxesConvention;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.operation.distance.DistanceOp;
import org.opengis.referencing.crs.GeographicCRS;

/**
 * ST_DISTANCE_SPHEROID(geometry, geometry) - computes the geodetic (ellipsoidal) distance
 * in meters between two geometries using the WGS84 ellipsoid via Apache SIS GeodeticCalculator.
 * Inputs are assumed to be in lon/lat degrees (EPSG:4326). If an input geometry
 * has a known SRID that is not 4326, a warning is emitted and the function still
 * computes distance using WGS84.
 * For non-Point geometries the centroid is used.
 */
public class STDistanceSpheroidDescriptor extends AbstractSTDoubleGeometryDescriptor {

    @Serial
    private static final long serialVersionUID = 1L;
    public static final IFunctionDescriptorFactory FACTORY = STDistanceSpheroidDescriptor::new;

    private static final GeographicCRS WGS84 = (GeographicCRS) AbstractCRS.castOrCopy(CommonCRS.WGS84.geographic())
            .forConvention(AxesConvention.RIGHT_HANDED);

    @Override
    public FunctionIdentifier getIdentifier() {
        return BuiltinFunctions.ST_DISTANCE_SPHEROID;
    }

    @Override
    protected Object evaluateOGCGeometry(Geometry geom0, Geometry geom1) throws HyracksDataException {
        Coordinate[] nearest = DistanceOp.nearestPoints(geom0, geom1);
        Coordinate c0 = nearest[0];
        Coordinate c1 = nearest[1];
        GeodeticCalculator calc = GeodeticCalculator.create(WGS84);
        calc.setStartPoint(new DirectPosition2D(WGS84, c0.x, c0.y));
        calc.setEndPoint(new DirectPosition2D(WGS84, c1.x, c1.y));
        return calc.getGeodesicDistance();
    }

    @Override
    protected Object evaluateOGCGeometry(Geometry geom0, Geometry geom1, IEvaluatorContext ctx)
            throws HyracksDataException {
        int srid0 = geom0.getSRID();
        int srid1 = geom1.getSRID();
        // NOTE: This warning is currently unreachable. AGeometrySerializerDeserializer uses
        // JTS WKBWriter, which does not persist SRID in the binary representation.
        // geom.getSRID() always returns 0 after deserialization, so the condition
        // (srid != 0 && srid != 4326) is always false. This block will become active
        // when WKB SRID persistence is implemented.
        if ((srid0 != 0 && srid0 != 4326) || (srid1 != 0 && srid1 != 4326)) {
            IWarningCollector wc = ctx.getWarningCollector();
            if (wc.shouldWarn()) {
                wc.warn(Warning.of(sourceLoc, ErrorCode.ST_DISTANCE_SPHEROID_REQUIRES_4326, srid0, srid1));
            }
        }
        return evaluateOGCGeometry(geom0, geom1);
    }
}
