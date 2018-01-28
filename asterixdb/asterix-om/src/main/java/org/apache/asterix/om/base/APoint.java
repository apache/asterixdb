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

public class APoint implements IAObject {

    protected double x;
    protected double y;

    public APoint(double x, double y) {
        this.x = x;
        this.y = y;
    }

    public double getX() {
        return x;
    }

    public double getY() {
        return y;
    }

    @Override
    public IAType getType() {
        return BuiltinType.APOINT;
    }

    @Override
    public boolean deepEqual(IAObject obj) {
        if (!(obj instanceof APoint)) {
            return false;
        } else {
            APoint p = (APoint) obj;
            return x == p.x && y == p.y;
        }
    }

    @Override
    public int hash() {
        return InMemUtils.hashDouble(x) + 31 * InMemUtils.hashDouble(y);
    }

    @Override
    public String toString() {
        return "point: { x: " + x + ", y: " + y + " }";
    }

    @Override
    public ObjectNode toJSON() {
        ObjectMapper om = new ObjectMapper();
        ObjectNode json = om.createObjectNode();
        ObjectNode point = om.createObjectNode();
        point.put("x", x);
        point.put("y", y);
        json.set("APoint", point);

        return json;
    }
}
