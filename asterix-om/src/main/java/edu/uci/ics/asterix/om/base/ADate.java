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
 * ADate type represents dates in a gregorian calendar system.
 */
public class ADate implements IAObject {

    /**
     * the number of full calendar days since the 1970-01-01 represented by the date value.
     */
    protected int chrononTimeInDay;

    protected static long CHRONON_OF_DAY = 24 * 60 * 60 * 1000;

    public ADate(int chrononTimeInDay) {
        this.chrononTimeInDay = chrononTimeInDay;
    }

    public IAType getType() {
        return BuiltinType.ADATE;
    }

    public boolean equals(Object o) {
        if (!(o instanceof ADate)) {
            return false;
        } else {
            return ((ADate) o).chrononTimeInDay == this.chrononTimeInDay;
        }
    }

    @Override
    public int hashCode() {
        return chrononTimeInDay;
    }

    @Override
    public void accept(IOMVisitor visitor) throws AsterixException {
        visitor.visitADate(this);
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
        sbder.append("ADate: { ");
        try {
            GregorianCalendarSystem.getInstance().getExtendStringRepUntilField(chrononTimeInDay * CHRONON_OF_DAY, 0,
                    sbder, GregorianCalendarSystem.Fields.YEAR, GregorianCalendarSystem.Fields.DAY, false);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        sbder.append(" }");
        return sbder.toString();
    }

    public int getChrononTimeInDays() {
        return chrononTimeInDay;
    }

    @Override
    public JSONObject toJSON() throws JSONException {
        JSONObject json = new JSONObject();

        json.put("ADate", chrononTimeInDay);

        return json;
    }
}
