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

public class AInt64 implements IAObject {

    protected long value;

    public AInt64(Long value) {
        this.value = value;
    }

    public long getLongValue() {
        return value;
    }

    @Override
    public IAType getType() {
        return BuiltinType.AINT64;
    }

    @Override
    public void accept(IOMVisitor visitor) throws AsterixException {
        visitor.visitAInt64(this);
    }

    @Override
    public boolean deepEqual(IAObject obj) {
        if (!(obj instanceof AInt64)) {
            return false;
        }
        return value == ((AInt64) obj).getLongValue();
    }

    @Override
    public int hash() {
        return (int) (value ^ (value >>> 32));
    }

    @Override
    public String toString() {
        return "AInt64: {" + value + "}";
    }

    @Override
    public JSONObject toJSON() throws JSONException {
        JSONObject json = new JSONObject();

        json.put("AInt64", value);

        return json;
    }
}
