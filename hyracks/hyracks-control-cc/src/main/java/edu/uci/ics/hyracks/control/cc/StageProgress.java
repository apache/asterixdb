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
package edu.uci.ics.hyracks.control.cc;

import java.util.HashSet;
import java.util.Set;
import java.util.UUID;

import edu.uci.ics.hyracks.api.job.statistics.StageStatistics;

public class StageProgress {
    private final UUID stageId;

    private final Set<String> pendingNodes;

    private final StageStatistics stageStatistics;

    public StageProgress(UUID stageId) {
        this.stageId = stageId;
        pendingNodes = new HashSet<String>();
        stageStatistics = new StageStatistics();
        stageStatistics.setStageId(stageId);
    }

    public UUID getStageId() {
        return stageId;
    }

    public void addPendingNodes(Set<String> nodes) {
        pendingNodes.addAll(nodes);
    }

    public void markNodeComplete(String nodeId) {
        pendingNodes.remove(nodeId);
    }

    public boolean stageComplete() {
        return pendingNodes.isEmpty();
    }

    public StageStatistics getStageStatistics() {
        return stageStatistics;
    }
}