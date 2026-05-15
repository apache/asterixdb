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

import java.util.Collection;

import org.apache.asterix.om.functions.BuiltinFunctions;
import org.apache.asterix.om.functions.IFunctionDescriptorFactory;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.LineString;
import org.locationtech.jts.operation.linemerge.LineMerger;

/**
 * ST_LineMerge: stitches connected {@code LINESTRING}s together by shared
 * endpoints, returning the longest continuous lines possible. Returns a
 * single {@code LINESTRING} if all components merge into one, an empty
 * {@code MULTILINESTRING} if the input contains no mergeable lines, or a
 * {@code MULTILINESTRING} of the resulting segments otherwise. Implemented
 * via JTS {@code LineMerger}.
 */
public class STLineMergeDescriptor extends AbstractSTSingleGeometryDescriptor {

    private static final long serialVersionUID = 1L;
    public static final IFunctionDescriptorFactory FACTORY = STLineMergeDescriptor::new;

    @Override
    protected Object evaluateOGCGeometry(Geometry geometry) throws HyracksDataException {
        LineMerger merger = new LineMerger();
        merger.add(geometry);
        @SuppressWarnings("unchecked")
        Collection<LineString> merged = merger.getMergedLineStrings();
        GeometryFactory gf = geometry.getFactory();
        if (merged.isEmpty()) {
            return gf.createMultiLineString();
        }
        if (merged.size() == 1) {
            return merged.iterator().next();
        }
        return gf.createMultiLineString(merged.toArray(new LineString[0]));
    }

    @Override
    public FunctionIdentifier getIdentifier() {
        return BuiltinFunctions.ST_LINE_MERGE;
    }
}
