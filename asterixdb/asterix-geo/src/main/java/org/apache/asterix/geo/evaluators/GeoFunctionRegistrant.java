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
package org.apache.asterix.geo.evaluators;

import org.apache.asterix.geo.aggregates.STUnionAggregateDescriptor;
import org.apache.asterix.geo.aggregates.ScalarSTUnionAggregateDescriptor;
import org.apache.asterix.geo.aggregates.ScalarSTUnionDistinctAggregateDescriptor;
import org.apache.asterix.geo.evaluators.functions.ParseGeoJSONDescriptor;
import org.apache.asterix.geo.evaluators.functions.STAreaDescriptor;
import org.apache.asterix.geo.evaluators.functions.STAsBinaryDescriptor;
import org.apache.asterix.geo.evaluators.functions.STAsGeoJSONDescriptor;
import org.apache.asterix.geo.evaluators.functions.STAsTextDescriptor;
import org.apache.asterix.geo.evaluators.functions.STBoundaryDescriptor;
import org.apache.asterix.geo.evaluators.functions.STContainsDescriptor;
import org.apache.asterix.geo.evaluators.functions.STCoordDimDescriptor;
import org.apache.asterix.geo.evaluators.functions.STCrossesDescriptor;
import org.apache.asterix.geo.evaluators.functions.STDifferenceDescriptor;
import org.apache.asterix.geo.evaluators.functions.STDimensionDescriptor;
import org.apache.asterix.geo.evaluators.functions.STDisjointDescriptor;
import org.apache.asterix.geo.evaluators.functions.STDistanceDescriptor;
import org.apache.asterix.geo.evaluators.functions.STEndPointDescriptor;
import org.apache.asterix.geo.evaluators.functions.STEnvelopeDescriptor;
import org.apache.asterix.geo.evaluators.functions.STEqualsDescriptor;
import org.apache.asterix.geo.evaluators.functions.STExteriorRingDescriptor;
import org.apache.asterix.geo.evaluators.functions.STGeomFromTextDescriptor;
import org.apache.asterix.geo.evaluators.functions.STGeomFromTextSRIDDescriptor;
import org.apache.asterix.geo.evaluators.functions.STGeomFromWKBDescriptor;
import org.apache.asterix.geo.evaluators.functions.STGeomentryTypeDescriptor;
import org.apache.asterix.geo.evaluators.functions.STGeometryNDescriptor;
import org.apache.asterix.geo.evaluators.functions.STInteriorRingNDescriptor;
import org.apache.asterix.geo.evaluators.functions.STIntersectionDescriptor;
import org.apache.asterix.geo.evaluators.functions.STIntersectsDescriptor;
import org.apache.asterix.geo.evaluators.functions.STIsClosedDescriptor;
import org.apache.asterix.geo.evaluators.functions.STIsCollectionDescriptor;
import org.apache.asterix.geo.evaluators.functions.STIsEmptyDescriptor;
import org.apache.asterix.geo.evaluators.functions.STIsRingDescriptor;
import org.apache.asterix.geo.evaluators.functions.STIsSimpleDescriptor;
import org.apache.asterix.geo.evaluators.functions.STLengthDescriptor;
import org.apache.asterix.geo.evaluators.functions.STLineFromMultiPointDescriptor;
import org.apache.asterix.geo.evaluators.functions.STMDescriptor;
import org.apache.asterix.geo.evaluators.functions.STMakeEnvelopeDescriptorSRID;
import org.apache.asterix.geo.evaluators.functions.STMakePoint3DDescriptor;
import org.apache.asterix.geo.evaluators.functions.STMakePoint3DWithMDescriptor;
import org.apache.asterix.geo.evaluators.functions.STMakePointDescriptor;
import org.apache.asterix.geo.evaluators.functions.STNPointsDescriptor;
import org.apache.asterix.geo.evaluators.functions.STNRingsDescriptor;
import org.apache.asterix.geo.evaluators.functions.STNumGeometriesDescriptor;
import org.apache.asterix.geo.evaluators.functions.STNumInteriorRingsDescriptor;
import org.apache.asterix.geo.evaluators.functions.STOverlapsDescriptor;
import org.apache.asterix.geo.evaluators.functions.STPointNDescriptor;
import org.apache.asterix.geo.evaluators.functions.STPolygonizeDescriptor;
import org.apache.asterix.geo.evaluators.functions.STRelateDescriptor;
import org.apache.asterix.geo.evaluators.functions.STSRIDDescriptor;
import org.apache.asterix.geo.evaluators.functions.STStartPointDescriptor;
import org.apache.asterix.geo.evaluators.functions.STSymDifferenceDescriptor;
import org.apache.asterix.geo.evaluators.functions.STTouchesDescriptor;
import org.apache.asterix.geo.evaluators.functions.STUnionDescriptor;
import org.apache.asterix.geo.evaluators.functions.STWithinDescriptor;
import org.apache.asterix.geo.evaluators.functions.STXDescriptor;
import org.apache.asterix.geo.evaluators.functions.STXMaxDescriptor;
import org.apache.asterix.geo.evaluators.functions.STXMinDescriptor;
import org.apache.asterix.geo.evaluators.functions.STYDescriptor;
import org.apache.asterix.geo.evaluators.functions.STYMaxDescriptor;
import org.apache.asterix.geo.evaluators.functions.STYMinDescriptor;
import org.apache.asterix.geo.evaluators.functions.STZDescriptor;
import org.apache.asterix.geo.evaluators.functions.STZMaxDescriptor;
import org.apache.asterix.geo.evaluators.functions.STZMinDescriptor;
import org.apache.asterix.om.functions.IFunctionCollection;
import org.apache.asterix.om.functions.IFunctionRegistrant;

public class GeoFunctionRegistrant implements IFunctionRegistrant {
    @Override
    public void register(IFunctionCollection fc) {
        //Geo functions
        fc.add(ScalarSTUnionAggregateDescriptor.FACTORY);
        fc.add(ScalarSTUnionDistinctAggregateDescriptor.FACTORY);
        fc.add(STUnionAggregateDescriptor.FACTORY);

        //GeoJSON
        fc.add(ParseGeoJSONDescriptor.FACTORY);
        fc.add(STAreaDescriptor.FACTORY);
        fc.add(STMakePointDescriptor.FACTORY);
        fc.add(STMakePoint3DDescriptor.FACTORY);
        fc.add(STMakePoint3DWithMDescriptor.FACTORY);
        fc.add(STIntersectsDescriptor.FACTORY);
        fc.add(STUnionDescriptor.FACTORY);
        fc.add(STIsCollectionDescriptor.FACTORY);
        fc.add(STContainsDescriptor.FACTORY);
        fc.add(STCrossesDescriptor.FACTORY);
        fc.add(STDisjointDescriptor.FACTORY);
        fc.add(STEqualsDescriptor.FACTORY);
        fc.add(STOverlapsDescriptor.FACTORY);
        fc.add(STTouchesDescriptor.FACTORY);
        fc.add(STWithinDescriptor.FACTORY);
        fc.add(STIsEmptyDescriptor.FACTORY);
        fc.add(STIsSimpleDescriptor.FACTORY);
        fc.add(STCoordDimDescriptor.FACTORY);
        fc.add(STDimensionDescriptor.FACTORY);
        fc.add(STGeomentryTypeDescriptor.FACTORY);
        fc.add(STMDescriptor.FACTORY);
        fc.add(STNRingsDescriptor.FACTORY);
        fc.add(STNPointsDescriptor.FACTORY);
        fc.add(STNumGeometriesDescriptor.FACTORY);
        fc.add(STNumInteriorRingsDescriptor.FACTORY);
        fc.add(STSRIDDescriptor.FACTORY);
        fc.add(STXDescriptor.FACTORY);
        fc.add(STYDescriptor.FACTORY);
        fc.add(STXMaxDescriptor.FACTORY);
        fc.add(STXMinDescriptor.FACTORY);
        fc.add(STYMaxDescriptor.FACTORY);
        fc.add(STYMinDescriptor.FACTORY);
        fc.add(STZDescriptor.FACTORY);
        fc.add(STZMaxDescriptor.FACTORY);
        fc.add(STZMinDescriptor.FACTORY);
        fc.add(STAsBinaryDescriptor.FACTORY);
        fc.add(STAsTextDescriptor.FACTORY);
        fc.add(STAsGeoJSONDescriptor.FACTORY);
        fc.add(STDistanceDescriptor.FACTORY);
        fc.add(STLengthDescriptor.FACTORY);
        fc.add(STGeomFromTextDescriptor.FACTORY);
        fc.add(STGeomFromTextSRIDDescriptor.FACTORY);
        fc.add(STGeomFromWKBDescriptor.FACTORY);
        fc.add(STLineFromMultiPointDescriptor.FACTORY);
        fc.add(STMakeEnvelopeDescriptorSRID.FACTORY);
        fc.add(STIsClosedDescriptor.FACTORY);
        fc.add(STIsRingDescriptor.FACTORY);
        fc.add(STRelateDescriptor.FACTORY);
        fc.add(STBoundaryDescriptor.FACTORY);
        fc.add(STEndPointDescriptor.FACTORY);
        fc.add(STEnvelopeDescriptor.FACTORY);
        fc.add(STExteriorRingDescriptor.FACTORY);
        fc.add(STGeometryNDescriptor.FACTORY);
        fc.add(STInteriorRingNDescriptor.FACTORY);
        fc.add(STPointNDescriptor.FACTORY);
        fc.add(STStartPointDescriptor.FACTORY);
        fc.add(STDifferenceDescriptor.FACTORY);
        fc.add(STIntersectionDescriptor.FACTORY);
        fc.add(STSymDifferenceDescriptor.FACTORY);
        fc.add(STPolygonizeDescriptor.FACTORY);
    }
}
