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

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import edu.uci.ics.asterix.common.exceptions.AsterixException;
import edu.uci.ics.asterix.om.types.ARecordType;
import edu.uci.ics.asterix.om.visitors.IOMVisitor;

public class ARecord implements IAObject {

    protected ARecordType type;
    protected IAObject[] fields;

    public ARecord(ARecordType type, IAObject[] fields) {
        this.type = type;
        this.fields = fields;
    }

    @Override
    public void accept(IOMVisitor visitor) throws AsterixException {
        visitor.visitARecord(this);
    }

    @Override
    public ARecordType getType() {
        return type;
    }

    public boolean isOpen() {
        return type.isOpen();
    }

    // efficient way of retrieving the value of a field; pos starts from 0
    public IAObject getValueByPos(int pos) {
        return fields[pos];
    }

    public int numberOfFields() {
        return fields.length;
    }

    @Override
    public boolean deepEqual(IAObject obj) {
        if (!(obj instanceof ARecord)) {
            return false;
        }
        ARecord r = (ARecord) obj;
        if (!type.deepEqual(r.type)) {
            return false;
        }
        return InMemUtils.deepEqualArrays(fields, r.fields);
    }

    @Override
    public int hash() {
        int h = 0;
        for (int i = 0; i < fields.length; i++) {
            h += 31 * h + fields[i].hash();
        }
        return h;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("ARecord: { ");
        for (int i = 0; i < fields.length; i++) {
            if (i > 0) {
                sb.append(", ");
            }
            sb.append(type.getFieldNames()[i]);
            sb.append(": ");
            sb.append(fields[i]);
        }
        sb.append(" }");
        return sb.toString();
    }

    @Override
    public JSONObject toJSON() throws JSONException {
        JSONObject json = new JSONObject();

        JSONArray record = new JSONArray();
        for (int i = 0; i < fields.length; i++) {
            JSONObject item = new JSONObject();
            item.put(type.getFieldNames()[i], fields[i]);
            record.put(item);
        }
        json.put("ARecord", record);

        return json;
    }
}
