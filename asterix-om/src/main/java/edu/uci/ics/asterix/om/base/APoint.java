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
    public void accept(IOMVisitor visitor) throws AsterixException {
        visitor.visitAPoint(this);
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
        return "APoint: { x: " + x + ", y: " + y + " }";
    }

    @Override
    public JSONObject toJSON() throws JSONException {
        JSONObject json = new JSONObject();

        JSONObject point = new JSONObject();
        point.put("x", x);
        point.put("y", y);
        json.put("APoint", point);

        return json;
    }
}
