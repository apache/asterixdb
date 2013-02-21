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

package edu.uci.ics.hyracks.api.rewriter;

import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang3.tuple.Pair;

import edu.uci.ics.hyracks.api.dataflow.ActivityId;
import edu.uci.ics.hyracks.api.job.ActivityCluster;
import edu.uci.ics.hyracks.api.job.ActivityClusterGraph;
import edu.uci.ics.hyracks.api.job.ActivityClusterId;

/**
 * All the connectors in an OneToOneConnectedCluster are OneToOneConnectorDescriptors.
 * 
 * @author yingyib
 */
public class OneToOneConnectedActivityCluster extends ActivityCluster {

    private static final long serialVersionUID = 1L;
    protected final Map<Integer, Pair<ActivityId, Integer>> clusterInputIndexMap = new HashMap<Integer, Pair<ActivityId, Integer>>();
    protected final Map<Integer, Pair<ActivityId, Integer>> clusterOutputIndexMap = new HashMap<Integer, Pair<ActivityId, Integer>>();
    protected final Map<Pair<ActivityId, Integer>, Integer> invertedClusterOutputIndexMap = new HashMap<Pair<ActivityId, Integer>, Integer>();
    protected final Map<Pair<ActivityId, Integer>, Integer> invertedClusterInputIndexMap = new HashMap<Pair<ActivityId, Integer>, Integer>();

    public OneToOneConnectedActivityCluster(ActivityClusterGraph acg, ActivityClusterId id) {
        super(acg, id);
    }

    public void setClusterOutputIndex(int clusterOutputIndex, ActivityId activityId, int activityOutputIndex) {
        clusterOutputIndexMap.put(clusterOutputIndex, Pair.of(activityId, activityOutputIndex));
        invertedClusterOutputIndexMap.put(Pair.of(activityId, activityOutputIndex), clusterOutputIndex);
    }

    public Pair<ActivityId, Integer> getActivityIdOutputIndex(int clusterOutputIndex) {
        return clusterOutputIndexMap.get(clusterOutputIndex);
    }

    public void setClusterInputIndex(int clusterInputIndex, ActivityId activityId, int activityInputIndex) {
        clusterInputIndexMap.put(clusterInputIndex, Pair.of(activityId, activityInputIndex));
        invertedClusterInputIndexMap.put(Pair.of(activityId, activityInputIndex), clusterInputIndex);
    }

    public Pair<ActivityId, Integer> getActivityIdInputIndex(int clusterInputIndex) {
        return clusterInputIndexMap.get(clusterInputIndex);
    }

    public int getClusterInputIndex(Pair<ActivityId, Integer> activityInputChannel) {
        return invertedClusterInputIndexMap.get(activityInputChannel);
    }

    public int getClusterOutputIndex(Pair<ActivityId, Integer> activityOutputChannel) {
        return invertedClusterOutputIndexMap.get(activityOutputChannel);
    }

}
