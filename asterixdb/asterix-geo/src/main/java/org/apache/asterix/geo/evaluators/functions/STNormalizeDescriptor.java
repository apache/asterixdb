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
 * ST_Normalize: returns a normalized form of the geometry — vertex order,
 * ring orientation, and (where applicable) component ordering are
 * canonicalised. Implemented via JTS {@code Geometry.normalize()}.
 *
 * <p>{@code Geometry.normalize()} mutates the receiver in place, but
 * {@link AbstractSTSingleGeometryDescriptor} deserialises a fresh JTS
 * {@code Geometry} from the input pointable's WKB on every {@code evaluate()}
 * call, so the in-memory geometry is a per-call workspace with no other
 * references — mutating it in place is safe and avoids the cost of a full
 * coordinate-deep copy.</p>
 */
public class STNormalizeDescriptor extends AbstractSTSingleGeometryDescriptor {

    private static final long serialVersionUID = 1L;
    public static final IFunctionDescriptorFactory FACTORY = STNormalizeDescriptor::new;

    @Override
    protected Object evaluateOGCGeometry(Geometry geometry) throws HyracksDataException {
        geometry.normalize();
        return geometry;
    }

    @Override
    public FunctionIdentifier getIdentifier() {
        return BuiltinFunctions.ST_NORMALIZE;
    }
}
