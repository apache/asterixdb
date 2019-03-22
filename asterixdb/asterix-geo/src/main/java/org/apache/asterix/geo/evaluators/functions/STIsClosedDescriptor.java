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

import com.esri.core.geometry.ogc.OGCCurve;
import com.esri.core.geometry.ogc.OGCGeometry;
import com.esri.core.geometry.ogc.OGCGeometryCollection;
import com.esri.core.geometry.ogc.OGCMultiCurve;
import com.esri.core.geometry.ogc.OGCMultiPoint;
import com.esri.core.geometry.ogc.OGCMultiPolygon;
import com.esri.core.geometry.ogc.OGCPoint;
import com.esri.core.geometry.ogc.OGCPolygon;

public class STIsClosedDescriptor extends AbstractSTSingleGeometryDescriptor {

    private static final long serialVersionUID = 1L;
    public static final IFunctionDescriptorFactory FACTORY = new IFunctionDescriptorFactory() {
        @Override
        public IFunctionDescriptor createFunctionDescriptor() {
            return new STIsClosedDescriptor();
        }
    };

    @Override
    protected Object evaluateOGCGeometry(OGCGeometry geometry) throws HyracksDataException {
        return isClosed(geometry);
    }

    private boolean isClosed(OGCGeometry geometry) {
        if (geometry instanceof OGCCurve) {
            return ((OGCCurve) geometry).isClosed();
        } else if (geometry instanceof OGCMultiCurve) {
            return ((OGCMultiCurve) geometry).isClosed();
        } else if (geometry instanceof OGCMultiPoint || geometry instanceof OGCMultiPolygon
                || geometry instanceof OGCPolygon || geometry instanceof OGCPoint) {
            return true;
        } else if (geometry instanceof OGCGeometryCollection) {
            OGCGeometryCollection geometryCollection = (OGCGeometryCollection) geometry;
            int num = geometryCollection.numGeometries();
            for (int i = 0; i < num; ++i) {
                if (!isClosed(geometryCollection.geometryN(i))) {
                    return false;
                }
            }
            return true;
        } else {
            throw new UnsupportedOperationException(
                    "The operation " + getIdentifier() + " is not supported for the type " + geometry.geometryType());
        }
    }

    @Override
    public FunctionIdentifier getIdentifier() {
        return BuiltinFunctions.ST_IS_CLOSED;
    }

}
