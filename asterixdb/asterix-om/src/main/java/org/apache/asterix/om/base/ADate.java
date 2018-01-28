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
        sbder.append("\"date\": { ");
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
    public ObjectNode toJSON() {
        ObjectMapper om = new ObjectMapper();
        ObjectNode json = om.createObjectNode();

        json.put("ADate", chrononTimeInDay);

        return json;
    }
}
