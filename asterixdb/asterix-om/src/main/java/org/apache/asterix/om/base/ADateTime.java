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

import java.io.IOException;

import org.apache.asterix.om.base.temporal.GregorianCalendarSystem;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.asterix.om.types.IAType;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

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
 * By default, an ADateTime value is a UTC time value, i.e., there is no timezone information maintained. However user
 * can use the timezone based AQL function to convert a UTC time to a timezone-embedded time.
 * <p/>
 * And the string representation of an ADateTime value follows the ISO8601 standard, in the following format:<br/>
 * [+|-]YYYY-MM-DDThh:mm:ss.xxxZ
 * <p/>
 * Internally, an ADateTime value is stored as the number of milliseconds elapsed since 1970-01-01T00:00:00.000Z (also
 * called chronon time). Functions to convert between a string representation of an ADateTime and its chronon time are
 * implemented in {@link GregorianCalendarSystem}.
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
    public boolean deepEqual(IAObject obj) {
        return equals(obj);
    }

    @Override
    public int hash() {
        return hashCode();
    }

    @Override
    public String toString() {
        return appendDatetime(new StringBuilder().append("datetime: { "), chrononTime).append(" }").toString();
    }

    public String toSimpleString() {
        return appendDatetime(new StringBuilder(), chrononTime).toString();
    }

    private static StringBuilder appendDatetime(StringBuilder sbder, long chrononTime) {
        try {
            GregorianCalendarSystem.getInstance().getExtendStringRepUntilField(chrononTime, 0, sbder,
                    GregorianCalendarSystem.Fields.YEAR, GregorianCalendarSystem.Fields.MILLISECOND, true);
            return sbder;
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }

    public long getChrononTime() {
        return chrononTime;
    }

    @Override
    public ObjectNode toJSON() {
        ObjectMapper om = new ObjectMapper();
        ObjectNode json = om.createObjectNode();

        json.put("ADateTime", chrononTime);

        return json;
    }
}
