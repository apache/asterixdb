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
 * AYearMonthDuration represents the duration in the unit of months.
 * <p/>
 * An AYearMonthDuration may contain the following two fields:<br/>
 * - year;<br/>
 * - month.
 * <p/>
 */
public class AYearMonthDuration implements IAObject {

    protected int chrononInMonth;

    public AYearMonthDuration(int months) {
        this.chrononInMonth = months;
    }

    public int getMonths() {
        return chrononInMonth;
    }

    @Override
    public String toString() {
        StringBuilder sbder = new StringBuilder();
        sbder.append("year_month_duration: {");
        GregorianCalendarSystem.getInstance().getDurationExtendStringRepWithTimezoneUntilField(0, chrononInMonth,
                sbder);
        sbder.append(" }");
        return sbder.toString();
    }

    @Override
    public ObjectNode toJSON() {
        ObjectMapper om = new ObjectMapper();
        ObjectNode json = om.createObjectNode();

        ObjectNode duration = om.createObjectNode();
        duration.put("months", chrononInMonth);
        json.set("ADuration", duration);

        return json;
    }

    @Override
    public IAType getType() {
        return BuiltinType.AYEARMONTHDURATION;
    }

    @Override
    public boolean deepEqual(IAObject obj) {
        return equals(obj);
    }

    @Override
    public int hash() {
        return hashCode();
    }

    public boolean equals(Object o) {
        if (o instanceof AYearMonthDuration) {
            return ((AYearMonthDuration) o).chrononInMonth == chrononInMonth;
        }
        return false;
    }

    public int hashCode() {
        return chrononInMonth;
    }

}
