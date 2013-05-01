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

public class ADayTimeDuration implements IAObject {

    protected long chrononInMillisecond;

    public ADayTimeDuration(long millisecond) {
        this.chrononInMillisecond = millisecond;
    }

    public long getMilliseconds() {
        return chrononInMillisecond;
    }

    /* (non-Javadoc)
     * @see edu.uci.ics.hyracks.api.dataflow.value.JSONSerializable#toJSON()
     */
    @Override
    public JSONObject toJSON() throws JSONException {
        JSONObject json = new JSONObject();

        JSONObject duration = new JSONObject();
        duration.put("milliseconds", chrononInMillisecond);
        json.put("ADuration", duration);

        return json;
    }

    /* (non-Javadoc)
     * @see edu.uci.ics.asterix.om.base.IAObject#getType()
     */
    @Override
    public IAType getType() {
        return BuiltinType.ADAYTIMEDURATION;
    }

    /* (non-Javadoc)
     * @see edu.uci.ics.asterix.om.base.IAObject#accept(edu.uci.ics.asterix.om.visitors.IOMVisitor)
     */
    @Override
    public void accept(IOMVisitor visitor) throws AsterixException {
        visitor.visitADayTimeDuration(this);
    }

    /* (non-Javadoc)
     * @see edu.uci.ics.asterix.om.base.IAObject#deepEqual(edu.uci.ics.asterix.om.base.IAObject)
     */
    @Override
    public boolean deepEqual(IAObject obj) {
        return equals(obj);
    }

    /* (non-Javadoc)
     * @see edu.uci.ics.asterix.om.base.IAObject#hash()
     */
    @Override
    public int hash() {
        return hashCode();
    }

    public boolean equals(Object o) {
        if (o instanceof ADayTimeDuration) {
            return ((ADayTimeDuration) o).chrononInMillisecond == chrononInMillisecond;
        }
        return false;
    }

    public int hashCode() {
        return (int) (chrononInMillisecond ^ (chrononInMillisecond >>> 32));
    }

}
