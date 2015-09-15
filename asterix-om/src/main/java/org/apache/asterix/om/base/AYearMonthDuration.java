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

import org.json.JSONException;
import org.json.JSONObject;

import org.apache.asterix.common.exceptions.AsterixException;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.asterix.om.types.IAType;
import org.apache.asterix.om.visitors.IOMVisitor;

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

    /* (non-Javadoc)
     * @see org.apache.hyracks.api.dataflow.value.JSONSerializable#toJSON()
     */
    @Override
    public JSONObject toJSON() throws JSONException {
        JSONObject json = new JSONObject();

        JSONObject duration = new JSONObject();
        duration.put("months", chrononInMonth);
        json.put("ADuration", duration);

        return json;
    }

    /* (non-Javadoc)
     * @see org.apache.asterix.om.base.IAObject#getType()
     */
    @Override
    public IAType getType() {
        return BuiltinType.AYEARMONTHDURATION;
    }

    /* (non-Javadoc)
     * @see org.apache.asterix.om.base.IAObject#accept(org.apache.asterix.om.visitors.IOMVisitor)
     */
    @Override
    public void accept(IOMVisitor visitor) throws AsterixException {
        visitor.visitAYearMonthDuration(this);
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
        if (o instanceof AYearMonthDuration) {
            return ((AYearMonthDuration) o).chrononInMonth == chrononInMonth;
        }
        return false;
    }

    public int hashCode() {
        return chrononInMonth;
    }

}
