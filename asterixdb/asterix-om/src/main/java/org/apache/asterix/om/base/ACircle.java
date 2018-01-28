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

public class ACircle implements IAObject {

    protected APoint center;
    protected double radius;

    public ACircle(APoint center, double radius) {
        this.center = center;
        this.radius = radius;
    }

    public APoint getP() {
        return center;
    }

    public void setP(APoint p) {
        this.center = p;
    }

    public double getRadius() {
        return radius;
    }

    public void setRadius(double radius) {
        this.radius = radius;
    }

    @Override
    public IAType getType() {
        return BuiltinType.ACIRCLE;
    }

    @Override
    public boolean deepEqual(IAObject obj) {
        if (!(obj instanceof ACircle)) {
            return false;
        }
        ACircle c = (ACircle) obj;
        return radius == c.radius && center.deepEqual(c.center);
    }

    @Override
    public int hash() {
        return (int) (center.hash() + radius);
    }

    @Override
    public String toString() {
        return "circle: { \"center\": " + center + ", \"radius\":" + radius + "}";
    }

    @Override
    public ObjectNode toJSON() {
        ObjectMapper om = new ObjectMapper();
        ObjectNode json = om.createObjectNode();

        ObjectNode circle = om.createObjectNode();
        circle.set("center", center.toJSON());
        circle.put("radius", radius);
        json.set("ACircle", circle);

        return json;
    }
}
