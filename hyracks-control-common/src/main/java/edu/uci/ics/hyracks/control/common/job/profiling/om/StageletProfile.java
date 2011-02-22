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
package edu.uci.ics.hyracks.control.common.job.profiling.om;

import java.util.UUID;

import org.json.JSONException;
import org.json.JSONObject;

public class StageletProfile extends AbstractProfile {
    private static final long serialVersionUID = 1L;

    private final UUID stageId;

    public StageletProfile(UUID stageId) {
        this.stageId = stageId;
    }

    public UUID getStageId() {
        return stageId;
    }

    @Override
    public JSONObject toJSON() throws JSONException {
        JSONObject json = new JSONObject();

        json.put("type", "joblet-profile");
        json.put("stage-id", stageId.toString());
        populateCounters(json);

        return json;
    }
}