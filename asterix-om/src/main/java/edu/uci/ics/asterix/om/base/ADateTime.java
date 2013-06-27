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

import java.io.IOException;

import org.json.JSONException;
import org.json.JSONObject;

import edu.uci.ics.asterix.common.exceptions.AsterixException;
import edu.uci.ics.asterix.om.base.temporal.GregorianCalendarSystem;
import edu.uci.ics.asterix.om.types.BuiltinType;
import edu.uci.ics.asterix.om.types.IAType;
import edu.uci.ics.asterix.om.visitors.IOMVisitor;

/**
 * ADateTime type represents the timestamp values.
 * <p/>
 * An ADateTime value contains the following time fields:<br/>
 * - year;<br/>
 * - month;<br/>
 * - day;<br/>
 * - hour; <br/>
 * - minute; <br/>
 * - second; <br/>
 * - millisecond. <br/>
 * By default, an ADateTime value is a UTC time value, i.e., there is no timezone information maintained. However user can use the timezone based AQL function to convert a UTC time to a timezone-embedded time.
 * <p/>
 * And the string representation of an ADateTime value follows the ISO8601 standard, in the following format:<br/>
 * [+|-]YYYY-MM-DDThh:mm:ss.xxxZ
 * <p/>
 * Internally, an ADateTime value is stored as the number of milliseconds elapsed since 1970-01-01T00:00:00.000Z (also called chronon time). Functions to convert between a string representation of an ADateTime and its chronon time are implemented in {@link GregorianCalendarSystem}.
 * <p/>
 */
public class ADateTime implements IAObject {

    /**
     * Represent the time interval as milliseconds since 1970-01-01T00:00:00.000Z.
     */
    protected long chrononTime;

    public ADateTime(long chrononTime) {
        this.chrononTime = chrononTime;
    }

    @Override
    public IAType getType() {
        return BuiltinType.ADATETIME;
    }

    public int compare(Object o) {
        if (!(o instanceof ADateTime)) {
            return -1;
        }

        ADateTime d = (ADateTime) o;
        if (this.chrononTime > d.chrononTime) {
            return 1;
        } else if (this.chrononTime < d.chrononTime) {
            return -1;
        } else {
            return 0;
        }
    }

    public boolean equals(Object o) {
        if (!(o instanceof ADateTime)) {
            return false;
        } else {
            ADateTime t = (ADateTime) o;
            return t.chrononTime == this.chrononTime;
        }
    }

    @Override
    public int hashCode() {
        return (int) (chrononTime ^ (chrononTime >>> 32));
    }

    @Override
    public void accept(IOMVisitor visitor) throws AsterixException {
        visitor.visitADateTime(this);
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
    public String toString() {
        StringBuilder sbder = new StringBuilder();
        sbder.append("ADateTime: { ");
        try {
            GregorianCalendarSystem.getInstance().getExtendStringRepUntilField(chrononTime, 0, sbder,
                    GregorianCalendarSystem.Fields.YEAR, GregorianCalendarSystem.Fields.MILLISECOND, true);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        sbder.append(" }");
        return sbder.toString();
    }

    public long getChrononTime() {
        return chrononTime;
    }

    @Override
    public JSONObject toJSON() throws JSONException {
        JSONObject json = new JSONObject();

        json.put("ADateTime", chrononTime);

        return json;
    }
}
