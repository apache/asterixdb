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

import org.apache.asterix.om.types.BuiltinType;
import org.apache.asterix.om.types.IAType;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

public class APoint3D implements IAObject {

    protected double x;
    protected double y;
    protected double z;

    public APoint3D(double x, double y, double z) {
        this.x = x;
        this.y = y;
        this.z = z;
    }

    public double getX() {
        return x;
    }

    public double getY() {
        return y;
    }

    public double getZ() {
        return z;
    }

    @Override
    public IAType getType() {
        return BuiltinType.APOINT3D;
    }

    @Override
    public boolean deepEqual(IAObject obj) {
        if (!(obj instanceof APoint3D)) {
            return false;
        } else {
            APoint3D p = (APoint3D) obj;
            return x == p.x && y == p.y && z == p.z;
        }
    }

    @Override
    public int hash() {
        return InMemUtils.hashDouble(x) + 31 * InMemUtils.hashDouble(y) + 31 * 31 * InMemUtils.hashDouble(z);
    }

    @Override
    public String toString() {
        return "point3d: { x: " + x + ", y: " + y + ", z: " + z + " }";
    }

    @Override
    public ObjectNode toJSON() {
        ObjectMapper om = new ObjectMapper();
        ObjectNode json = om.createObjectNode();

        ObjectNode point = om.createObjectNode();
        point.put("x", x);
        point.put("y", y);
        point.put("z", z);
        json.set("APoint3D", point);

        return json;
    }
}
