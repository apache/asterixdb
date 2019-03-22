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
import org.apache.asterix.om.functions.IFunctionDescriptor;
import org.apache.asterix.om.functions.IFunctionDescriptorFactory;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import org.apache.hyracks.api.exceptions.HyracksDataException;

import com.esri.core.geometry.Point;
import com.esri.core.geometry.Polyline;
import com.esri.core.geometry.SpatialReference;
import com.esri.core.geometry.ogc.OGCGeometry;
import com.esri.core.geometry.ogc.OGCLineString;
import com.esri.core.geometry.ogc.OGCMultiPoint;

public class STLineFromMultiPointDescriptor extends AbstractSTSingleGeometryDescriptor {

    private static final long serialVersionUID = 1L;
    public static final IFunctionDescriptorFactory FACTORY = new IFunctionDescriptorFactory() {
        @Override
        public IFunctionDescriptor createFunctionDescriptor() {
            return new STLineFromMultiPointDescriptor();
        }
    };

    @Override
    protected Object evaluateOGCGeometry(OGCGeometry geometry) throws HyracksDataException {
        if (geometry instanceof OGCMultiPoint) {
            Polyline polyline = new Polyline();
            OGCMultiPoint multiPoint = (OGCMultiPoint) geometry;
            int numPoints = multiPoint.numGeometries();
            polyline.startPath((Point) multiPoint.geometryN(0).getEsriGeometry());
            for (int i = 1; i < numPoints; i++) {
                polyline.lineTo((Point) multiPoint.geometryN(i).getEsriGeometry());
            }
            return new OGCLineString(polyline, 0, SpatialReference.create(4326));
        } else {
            throw new UnsupportedOperationException(
                    "The operation " + getIdentifier() + " is not supported for the type " + geometry.geometryType());
        }
    }

    @Override
    public FunctionIdentifier getIdentifier() {
        return BuiltinFunctions.ST_LINE_FROM_MULTIPOINT;
    }

}
