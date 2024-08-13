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
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.CoordinateXYZM;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.Point;

public class STMDescriptor extends AbstractSTSingleGeometryDescriptor {

    private static final long serialVersionUID = 1L;
    public static final IFunctionDescriptorFactory FACTORY = STMDescriptor::new;
    private static final Logger LOGGER = LogManager.getLogger();

    @Override
    protected Object evaluateOGCGeometry(Geometry geometry) throws HyracksDataException {
        if (StringUtils.equals(geometry.getGeometryType(), Geometry.TYPENAME_POINT)) {
            Point point = (Point) geometry;
            Coordinate coordinate = point.getCoordinate();
            if (coordinate instanceof CoordinateXYZM) {
                return coordinate.getM();
            } else {
                LOGGER.debug("The provided point does not have an M value.");
                return Double.NaN;
            }
        } else {
            throw new UnsupportedOperationException("The operation " + getIdentifier()
                    + " is not supported for the type " + geometry.getGeometryType());
        }
    }

    @Override
    public FunctionIdentifier getIdentifier() {
        return BuiltinFunctions.ST_M;
    }

}
