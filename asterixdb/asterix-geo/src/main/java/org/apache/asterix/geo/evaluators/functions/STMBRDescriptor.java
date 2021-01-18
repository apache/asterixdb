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

import org.apache.asterix.om.base.AMutablePoint;
import org.apache.asterix.om.base.AMutableRectangle;
import org.apache.asterix.om.functions.BuiltinFunctions;
import org.apache.asterix.om.functions.IFunctionDescriptor;
import org.apache.asterix.om.functions.IFunctionDescriptorFactory;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import org.apache.hyracks.api.exceptions.HyracksDataException;

import com.esri.core.geometry.Envelope;
import com.esri.core.geometry.ogc.OGCGeometry;

public class STMBRDescriptor extends AbstractSTSingleGeometryDescriptor {

    private static final long serialVersionUID = 1L;

    public static final IFunctionDescriptorFactory FACTORY = new IFunctionDescriptorFactory() {
        @Override
        public IFunctionDescriptor createFunctionDescriptor() {
            return new STMBRDescriptor();
        }
    };

    @Override
    protected Object evaluateOGCGeometry(OGCGeometry geometry) throws HyracksDataException {

        AMutableRectangle aRectangle = new AMutableRectangle(null, null);
        AMutablePoint[] aPoint = { new AMutablePoint(0, 0), new AMutablePoint(0, 0) };
        Envelope env = new Envelope();
        geometry.getEsriGeometry().queryEnvelope(env);
        aPoint[0].setValue(env.getXMin(), env.getYMin());
        aPoint[1].setValue(env.getXMax(), env.getYMax());
        aRectangle.setValue(aPoint[0], aPoint[1]);
        return aRectangle;
    }

    @Override
    public FunctionIdentifier getIdentifier() {
        return BuiltinFunctions.ST_MBR;
    }

}
