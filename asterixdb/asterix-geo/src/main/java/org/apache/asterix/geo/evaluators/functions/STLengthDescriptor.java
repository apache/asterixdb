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
import org.locationtech.jts.geom.LineString;
import org.locationtech.jts.geom.MultiLineString;

public class STLengthDescriptor extends AbstractSTSingleGeometryDescriptor {

    private static final long serialVersionUID = 1L;
    public static final IFunctionDescriptorFactory FACTORY = STLengthDescriptor::new;

    @Override
    protected Object evaluateOGCGeometry(Geometry geometry) throws HyracksDataException {
        if (StringUtils.equals(geometry.getGeometryType(), Geometry.TYPENAME_LINESTRING)) {
            return geometry.getLength();
        } else if (StringUtils.equals(geometry.getGeometryType(), Geometry.TYPENAME_MULTILINESTRING)) {
            double length = 0;
            MultiLineString multiLine = (MultiLineString) geometry;
            for (int i = 0; i < multiLine.getNumGeometries(); i++) {
                LineString lineString = (LineString) multiLine.getGeometryN(i);
                length += lineString.getLength();
            }
            return length;
        } else {
            throw new UnsupportedOperationException("The operation " + getIdentifier()
                    + " is not supported for the type " + geometry.getGeometryType());
        }
    }

    @Override
    public FunctionIdentifier getIdentifier() {
        return BuiltinFunctions.ST_LENGTH;
    }

}
