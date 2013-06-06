/*
 * Copyright 2009-2013 by The Regents of the University of California
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * you may obtain a copy of the License from
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.uci.ics.asterix.om.base;

import org.json.JSONException;
import org.json.JSONObject;

import edu.uci.ics.asterix.common.exceptions.AsterixException;
import edu.uci.ics.asterix.om.types.BuiltinType;
import edu.uci.ics.asterix.om.types.IAType;
import edu.uci.ics.asterix.om.visitors.IOMVisitor;

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
    public void accept(IOMVisitor visitor) throws AsterixException {
        visitor.visitACircle(this);
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
        return "ACircle: { center: " + center + ", radius: " + radius + "}";
    }

    @Override
    public JSONObject toJSON() throws JSONException {
        JSONObject json = new JSONObject();

        JSONObject circle = new JSONObject();
        circle.put("center", center);
        circle.put("radius", radius);
        json.put("ACircle", circle);

        return json;
    }
}
