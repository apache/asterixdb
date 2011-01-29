/*
 * Copyright 2009-2010 by The Regents of the University of California
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
package edu.uci.ics.hyracks.api.job.profiling.om;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import org.json.JSONException;
import org.json.JSONObject;

public class JobletProfile extends AbstractProfile {
    private static final long serialVersionUID = 1L;

    private final String nodeId;

    private final Map<UUID, StageletProfile> stageletProfiles;

    public JobletProfile(String nodeId) {
        this.nodeId = nodeId;
        stageletProfiles = new HashMap<UUID, StageletProfile>();
    }

    public String getNodeId() {
        return nodeId;
    }

    public Map<UUID, StageletProfile> getStageletProfiles() {
        return stageletProfiles;
    }

    @Override
    public JSONObject toJSON() throws JSONException {
        JSONObject json = new JSONObject();

        json.put("type", "joblet-profile");
        json.put("node-id", nodeId.toString());
        populateCounters(json);
        for (StageletProfile p : stageletProfiles.values()) {
            json.accumulate("stagelets", p.toJSON());
        }

        return json;
    }

    public void merge(JobletProfile jp) {
        super.merge(this);
        for (StageletProfile sp : jp.stageletProfiles.values()) {
            if (stageletProfiles.containsKey(sp.getStageId())) {
                stageletProfiles.get(sp.getStageId()).merge(sp);
            } else {
                stageletProfiles.put(sp.getStageId(), sp);
            }
        }
    }
}