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

public class ADouble implements IAObject {

    protected double value;

    public ADouble(double value) {
        super();
        this.value = value;
    }

    public double getDoubleValue() {
        return value;
    }

    @Override
    public IAType getType() {
        return BuiltinType.ADOUBLE;
    }

    @Override
    public String toString() {
        return "ADouble: {" + value + "}";
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof ADouble))
            return false;
        return value == (((ADouble) obj).getDoubleValue());
    }

    @Override
    public int hashCode() {
        long bits = Double.doubleToLongBits(value);
        return (int) (bits ^ (bits >>> 32));
    }

    @Override
    public void accept(IOMVisitor visitor) throws AsterixException {
        visitor.visitADouble(this);
    }

    @Override
    public boolean deepEqual(IAObject obj) {
        return equals(obj);
    }

    @Override
    public int hash() {
        return hashCode();
    }

    @Override
    public JSONObject toJSON() throws JSONException {
        JSONObject json = new JSONObject();

        json.put("ADouble", value);

        return json;
    }
}
