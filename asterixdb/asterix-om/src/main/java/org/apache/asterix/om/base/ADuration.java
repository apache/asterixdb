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

import org.apache.asterix.om.base.temporal.GregorianCalendarSystem;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.asterix.om.types.IAType;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

/**
 * ADuration type represents time duration (unanchored time length) values.
 * <p/>
 * An ADuration value may contain the same fields as the {@link ADateTime}: <br/>
 * - year;<br/>
 * - month;<br/>
 * - day;<br/>
 * - hour; <br/>
 * - minute; <br/>
 * - second; <br/>
 * - millisecond. <br/>
 * Compared with {@link ADateTime}, a field in a duration value does not have the limited domain requirement. A duration field is valid as far as the value of the field is an integer.
 * </p>
 * We also support negative durations, which is not specified by ISO 8601, but has been
 * supported by XML spec to enable the arithmetic operations between time instances.
 * <p/>
 * Internally, an ADuration value is stored as two fields: an integer field as the number of months for the YEAR and MONTH fields, and a long integer field as the number of milliseconds for the other fields.
 * <p/>
 */
public class ADuration implements IAObject {

    /**
     * number of full months represented by the year-month part
     */
    protected int chrononInMonth;

    /**
     * number of milliseconds represented by the part other than the year and month
     */
    protected long chrononInMillisecond;

    public ADuration(int months, long seconds) {
        this.chrononInMonth = months;
        this.chrononInMillisecond = seconds;
    }

    public int getMonths() {
        return chrononInMonth;
    }

    public long getMilliseconds() {
        return chrononInMillisecond;
    }

    @Override
    public IAType getType() {
        return BuiltinType.ADURATION;
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof ADuration)) {
            return false;
        } else {
            ADuration d = (ADuration) o;
            return d.getMonths() == chrononInMonth && d.getMilliseconds() == chrononInMillisecond;
        }
    }

    @Override
    public int hashCode() {
        return (int) (chrononInMonth ^ (chrononInMillisecond) ^ (chrononInMillisecond >>> 32));
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
        sbder.append("duration: {");
        GregorianCalendarSystem.getInstance().getDurationExtendStringRepWithTimezoneUntilField(chrononInMillisecond,
                chrononInMonth, sbder);
        sbder.append(" }");
        return sbder.toString();
    }

    @Override
    public ObjectNode toJSON() {
        ObjectMapper om = new ObjectMapper();
        ObjectNode json = om.createObjectNode();

        ObjectNode duration = om.createObjectNode();
        duration.put("months", chrononInMonth);
        duration.put("milliseconds", chrononInMillisecond);
        json.set("ADuration", duration);

        return json;
    }
}
