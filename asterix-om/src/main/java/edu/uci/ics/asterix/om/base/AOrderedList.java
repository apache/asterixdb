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
import edu.uci.ics.asterix.om.types.AOrderedListType;
import edu.uci.ics.asterix.om.types.IAType;
import edu.uci.ics.asterix.om.visitors.IOMVisitor;

public class AOrderedList implements IACollection {

    protected ArrayList<IAObject> values;
    protected AOrderedListType type;

    public AOrderedList(AOrderedListType type) {
        values = new ArrayList<IAObject>();
        this.type = type;
    }

    public AOrderedList(AOrderedListType type, ArrayList<IAObject> sequence) {
        values = sequence;
        this.type = type;
    }

    public void add(IAObject obj) {
        values.add(obj);
    }

    @Override
    public IACursor getCursor() {
        ACollectionCursor cursor = new ACollectionCursor();
        cursor.reset(this);
        return cursor;
    }

    @Override
    public IAType getType() {
        return type;
    }

    @Override
    public int size() {
        return values.size();
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof AOrderedList)) {
            return false;
        } else {
            AOrderedList y = (AOrderedList) o;
            return InMemUtils.cursorEquals(this.getCursor(), y.getCursor());
        }
    }

    @Override
    public int hashCode() {
        return InMemUtils.hashCursor(getCursor());
    }

    public IAObject getItem(int index) {
        return values.get(index);
    }

    @Override
    public void accept(IOMVisitor visitor) throws AsterixException {
        visitor.visitAOrderedList(this);
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
        sb.append("AOrderedList: [ ");
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
        json.put("AOrderedList", list);

        return json;
    }
}