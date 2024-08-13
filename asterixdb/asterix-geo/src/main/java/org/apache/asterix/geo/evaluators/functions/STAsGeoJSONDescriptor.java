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

import org.apache.asterix.dataflow.data.nontagged.serde.jacksonjts.JtsModule;
import org.apache.asterix.om.functions.BuiltinFunctions;
import org.apache.asterix.om.functions.IFunctionDescriptorFactory;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import org.apache.hyracks.api.exceptions.ErrorCode;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.locationtech.jts.geom.Geometry;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

public class STAsGeoJSONDescriptor extends AbstractSTSingleGeometryDescriptor {

    private static final long serialVersionUID = 1L;
    public static final IFunctionDescriptorFactory FACTORY = STAsGeoJSONDescriptor::new;
    private static final Logger LOGGER = LogManager.getLogger();
    private final ObjectMapper mapper;

    public STAsGeoJSONDescriptor() {
        mapper = new ObjectMapper();
    }

    @Override
    protected Object evaluateOGCGeometry(Geometry geometry) throws HyracksDataException {
        String geoJson = null;
        try {
            mapper.registerModule(new JtsModule());
            geoJson = mapper.writeValueAsString(geometry);
        } catch (JsonProcessingException e) {
            LOGGER.debug("JSON Processing exception during STAsGeoJSON function");
            throw HyracksDataException.create(ErrorCode.PARSING_ERROR);
        }
        return geoJson;
    }

    @Override
    public FunctionIdentifier getIdentifier() {
        return BuiltinFunctions.ST_AS_GEOJSON;
    }

}
