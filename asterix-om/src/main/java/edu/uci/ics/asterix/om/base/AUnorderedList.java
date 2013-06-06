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

import java.util.ArrayList;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import edu.uci.ics.asterix.common.exceptions.AsterixException;
import edu.uci.ics.asterix.om.types.AUnorderedListType;
import edu.uci.ics.asterix.om.types.IAType;
import edu.uci.ics.asterix.om.visitors.IOMVisitor;

public class AUnorderedList implements IACollection {

    protected ArrayList<IAObject> values;
    protected AUnorderedListType type;

    public AUnorderedList(AUnorderedListType type) {
        values = new ArrayList<IAObject>();
        this.type = type;
    }

    public AUnorderedList(AUnorderedListType type, ArrayList<IAObject> sequence) {
        values = sequence;
        this.type = type;
    }

    @Override
    public IACursor getCursor() {
        ACollectionCursor cursor = new ACollectionCursor();
        cursor.reset(this);
        return cursor;
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof AUnorderedList)) {
            return false;
        } else {
            AUnorderedList y = (AUnorderedList) o;
            return InMemUtils.cursorEquals(this.getCursor(), y.getCursor());
        }
    }

    @Override
    public int hashCode() {
        return InMemUtils.hashCursor(getCursor());
    }

    @Override
    public IAType getType() {
        return type;
    }

    @Override
    public int size() {
        return values.size();
    }

    public IAObject getOneObject() {
        if (values == null || values.isEmpty()) {
            return null;
        } else {
            return values.get(0);
        }
    }

    @Override
    public void accept(IOMVisitor visitor) throws AsterixException {
        visitor.visitAUnorderedList(this);
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
        StringBuilder sb = new StringBuilder();
        sb.append("AUnorderedList: [ ");
        boolean first = true;
        for (IAObject v : values) {
            if (first) {
                first = false;
            } else {
                sb.append(", ");
            }
            sb.append(v.toString());
        }
        sb.append(" ]");
        return sb.toString();
    }

    @Override
    public JSONObject toJSON() throws JSONException {
        JSONObject json = new JSONObject();

        JSONArray list = new JSONArray();
        for (IAObject v : values) {
            list.put(v.toJSON());
        }
        json.put("AUnorderedList", list);

        return json;
    }
}
