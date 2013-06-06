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
package edu.uci.ics.hyracks.control.common.job.profiling.om;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

public abstract class AbstractProfile implements Serializable {
    private static final long serialVersionUID = 1L;

    protected final Map<String, Long> counters;

    public AbstractProfile() {
        counters = new HashMap<String, Long>();
    }

    public Map<String, Long> getCounters() {
        return counters;
    }

    public abstract JSONObject toJSON() throws JSONException;

    protected void populateCounters(JSONObject jo) throws JSONException {
        JSONArray countersObj = new JSONArray();
        for (Map.Entry<String, Long> e : counters.entrySet()) {
            JSONObject jpe = new JSONObject();
            jpe.put("name", e.getKey());
            jpe.put("value", e.getValue());
            countersObj.put(jpe);
        }
        jo.put("counters", countersObj);
    }

    protected void merge(AbstractProfile profile) {
        counters.putAll(profile.counters);
    }
}