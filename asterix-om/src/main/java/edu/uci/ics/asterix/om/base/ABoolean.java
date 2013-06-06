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

public final class ABoolean implements IAObject {

    public static ABoolean TRUE = new ABoolean(true);
    public static ABoolean FALSE = new ABoolean(false);

    private final Boolean bVal;

    private ABoolean(boolean b) {
        bVal = new Boolean(b);
    }

    public Boolean getBoolean() {
        return bVal;
    }

    @Override
    public IAType getType() {
        return BuiltinType.ABOOLEAN;
    }

    @Override
    public String toString() {
        return "ABoolean: {" + bVal + "}";
    }

    public boolean equals(Object obj) {
        if (!(obj instanceof ABoolean)) {
            return false;
        } else {
            return bVal.equals(((ABoolean) obj).getBoolean());
        }
    }

    @Override
    public int hashCode() {
        return bVal.hashCode();
    }

    @Override
    public void accept(IOMVisitor visitor) throws AsterixException {
        visitor.visitABoolean(this);
    }

    @Override
    public boolean deepEqual(IAObject obj) {
        return obj == this;
    }

    @Override
    public int hash() {
        return bVal.hashCode();
    }

    @Override
    public JSONObject toJSON() throws JSONException {
        JSONObject json = new JSONObject();

        json.put("ABoolean", bVal);

        return json;
    }
}
