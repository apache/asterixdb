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
import org.locationtech.jts.geom.CoordinateSequence;
import org.locationtech.jts.geom.CoordinateSequenceFilter;
import org.locationtech.jts.geom.Geometry;

public class STFlipCoordinatesDescriptor extends AbstractSTSingleGeometryDescriptor {

    private static final long serialVersionUID = 1L;
    public static final IFunctionDescriptorFactory FACTORY = STFlipCoordinatesDescriptor::new;

    @Override
    protected Object evaluateOGCGeometry(Geometry geometry) throws HyracksDataException {
        Geometry flipped = geometry.copy();
        flipped.apply(FlipCoordinatesFilter.INSTANCE);
        return flipped;
    }

    @Override
    public FunctionIdentifier getIdentifier() {
        return BuiltinFunctions.ST_FLIP_COORDINATES;
    }

    private static final class FlipCoordinatesFilter implements CoordinateSequenceFilter {
        private static final FlipCoordinatesFilter INSTANCE = new FlipCoordinatesFilter();

        @Override
        public void filter(CoordinateSequence seq, int i) {

            double x = seq.getX(i);
            double y = seq.getY(i);
            seq.setOrdinate(i, CoordinateSequence.X, y);
            seq.setOrdinate(i, CoordinateSequence.Y, x);
        }

        @Override
        public boolean isDone() {
            return false;
        }

        @Override
        public boolean isGeometryChanged() {
            return true;
        }
    }

}
