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
package edu.uci.ics.hyracks.api.job.statistics;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

public class StageStatistics implements Serializable {
    private static final long serialVersionUID = 1L;

    private UUID stageId;

    private Map<String, StageletStatistics> stageletMap;

    public StageStatistics() {
        stageletMap = new HashMap<String, StageletStatistics>();
    }

    public void setStageId(UUID stageId) {
        this.stageId = stageId;
    }

    public UUID getStageId() {
        return stageId;
    }

    public void addStageletStatistics(StageletStatistics ss) {
        stageletMap.put(ss.getNodeId(), ss);
    }

    public Map<String, StageletStatistics> getStageletStatistics() {
        return stageletMap;
    }

    public void toString(StringBuilder buffer, int level) {
        JobStatistics.indent(buffer, level).append("{\n");
        JobStatistics.indent(buffer, level + 1).append("stageId: '").append(stageId).append("',\n");
        JobStatistics.indent(buffer, level + 1).append("stagelets: {\n");
        boolean first = true;
        for (Map.Entry<String, StageletStatistics> e : stageletMap.entrySet()) {
            if (!first) {
                buffer.append(",\n");
            }
            first = false;
            JobStatistics.indent(buffer, level + 2).append(e.getKey()).append(": ");
            e.getValue().toString(buffer, level + 3);
        }
        buffer.append("\n");
        JobStatistics.indent(buffer, level + 1).append("}\n");
        JobStatistics.indent(buffer, level).append("}");
    }
}