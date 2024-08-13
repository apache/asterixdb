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
package org.apache.asterix.om.base;

import java.io.IOException;

import org.apache.asterix.dataflow.data.nontagged.serde.jacksonjts.JtsModule;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.asterix.om.types.IAType;
import org.locationtech.jts.geom.Geometry;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

public class AGeometry implements IAObject {

    protected Geometry geometry;

    public AGeometry(Geometry geometry) {
        this.geometry = geometry;
    }

    public Geometry getGeometry() {
        return geometry;
    }

    @Override
    public IAType getType() {
        return BuiltinType.AGEOMETRY;
    }

    @Override
    public boolean deepEqual(IAObject obj) {
        if (!(obj instanceof AGeometry)) {
            return false;
        } else {
            AGeometry p = (AGeometry) obj;
            return p.geometry.equals(geometry);
        }
    }

    @Override
    public int hash() {
        return geometry.hashCode();
    }

    @Override
    public String toString() {
        return geometry.toString();
    }

    @Override
    public ObjectNode toJSON() {
        ObjectMapper om = new ObjectMapper();
        om.registerModule(new JtsModule());
        ObjectNode json;
        try {
            String geoJson = om.writeValueAsString(geometry);
            json = (ObjectNode) om.readTree(geoJson);
        } catch (IOException e) {
            return om.createObjectNode();
        }
        return json;
    }
}
