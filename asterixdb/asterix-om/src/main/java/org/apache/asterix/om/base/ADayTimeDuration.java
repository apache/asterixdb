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

public class ADayTimeDuration implements IAObject {

    protected long chrononInMillisecond;

    public ADayTimeDuration(long millisecond) {
        this.chrononInMillisecond = millisecond;
    }

    public long getMilliseconds() {
        return chrononInMillisecond;
    }

    @Override
    public String toString() {
        StringBuilder sbder = new StringBuilder();
        sbder.append("day_time_duration: {");
        GregorianCalendarSystem.getInstance().getDurationExtendStringRepWithTimezoneUntilField(chrononInMillisecond, 0,
                sbder);
        sbder.append(" }");
        return sbder.toString();
    }

    /* (non-Javadoc)
     * @see org.apache.hyracks.api.dataflow.value.JSONSerializable#toJSON()
     */
    @Override
    public ObjectNode toJSON() {
        ObjectMapper om = new ObjectMapper();
        ObjectNode json = om.createObjectNode();

        ObjectNode duration = om.createObjectNode();
        duration.put("milliseconds", chrononInMillisecond);
        json.set("ADuration", duration);

        return json;
    }

    /* (non-Javadoc)
     * @see org.apache.asterix.om.base.IAObject#getType()
     */
    @Override
    public IAType getType() {
        return BuiltinType.ADAYTIMEDURATION;
    }

    /* (non-Javadoc)
     * @see org.apache.asterix.om.base.IAObject#deepEqual(org.apache.asterix.om.base.IAObject)
     */
    @Override
    public boolean deepEqual(IAObject obj) {
        return equals(obj);
    }

    /* (non-Javadoc)
     * @see org.apache.asterix.om.base.IAObject#hash()
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
