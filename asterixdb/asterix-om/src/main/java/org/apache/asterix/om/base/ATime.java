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

public class ATime implements IAObject {

    /**
     * number of milliseconds since the beginning of a day represented by this time value
     */
    protected int chrononTime;

    public ATime(int chrononTime) {
        this.chrononTime = chrononTime;
    }

    @Override
    public IAType getType() {
        return BuiltinType.ATIME;
    }

    public int compare(Object o) {
        if (!(o instanceof ATime)) {
            return -1;
        }

        ATime d = (ATime) o;
        if (this.chrononTime > d.chrononTime) {
            return 1;
        } else if (this.chrononTime < d.chrononTime) {
            return -1;
        } else {
            return 0;
        }
    }

    @Override
    public boolean equals(Object o) {

        if (!(o instanceof ATime)) {
            return false;
        } else {
            ATime t = (ATime) o;
            return t.chrononTime == this.chrononTime;

        }
    }

    @Override
    public int hashCode() {
        return chrononTime;
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
        sbder.append("time: { ");
        try {
            GregorianCalendarSystem.getInstance().getExtendStringRepUntilField(chrononTime, 0, sbder,
                    GregorianCalendarSystem.Fields.HOUR, GregorianCalendarSystem.Fields.MILLISECOND, true);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        sbder.append(" }");
        return sbder.toString();

    }

    public int getChrononTime() {
        return chrononTime;
    }

    @Override
    public ObjectNode toJSON() {
        ObjectMapper om = new ObjectMapper();
        ObjectNode json = om.createObjectNode();

        json.put("ATime", chrononTime);

        return json;
    }
}
