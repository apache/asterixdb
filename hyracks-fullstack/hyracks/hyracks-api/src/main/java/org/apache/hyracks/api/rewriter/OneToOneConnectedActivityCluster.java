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

package org.apache.hyracks.api.rewriter;

import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.hyracks.api.dataflow.ActivityId;
import org.apache.hyracks.api.job.ActivityCluster;
import org.apache.hyracks.api.job.ActivityClusterGraph;
import org.apache.hyracks.api.job.ActivityClusterId;

/**
 * All the connectors in an OneToOneConnectedCluster are OneToOneConnectorDescriptors.
 *
 * @author yingyib
 */
public class OneToOneConnectedActivityCluster extends ActivityCluster {

    private static final long serialVersionUID = 1L;

    protected final Map<Integer, Pair<ActivityId, Integer>> clusterInputIndexMap =
            new HashMap<Integer, Pair<ActivityId, Integer>>();
    protected final Map<Integer, Pair<ActivityId, Integer>> clusterOutputIndexMap =
            new HashMap<Integer, Pair<ActivityId, Integer>>();
    protected final Map<Pair<ActivityId, Integer>, Integer> invertedClusterOutputIndexMap =
            new HashMap<Pair<ActivityId, Integer>, Integer>();
    protected final Map<Pair<ActivityId, Integer>, Integer> invertedClusterInputIndexMap =
            new HashMap<Pair<ActivityId, Integer>, Integer>();

    public OneToOneConnectedActivityCluster(ActivityClusterGraph acg, ActivityClusterId id) {
        super(acg, id);
    }

    /**
     * Set up the mapping of the cluster's output channel to an internal activity and its output channel
     *
     * @param clusterOutputIndex
     *            the output channel index for the cluster
     * @param activityId
     *            the id of the internal activity which produces the corresponding output
     * @param activityOutputIndex
     *            the output channel index of the internal activity which corresponds to the output channel of the cluster of activities
     */
    public void setClusterOutputIndex(int clusterOutputIndex, ActivityId activityId, int activityOutputIndex) {
        clusterOutputIndexMap.put(clusterOutputIndex, Pair.of(activityId, activityOutputIndex));
        invertedClusterOutputIndexMap.put(Pair.of(activityId, activityOutputIndex), clusterOutputIndex);
    }

    /**
     * get the an internal activity and its output channel of a cluster output channel
     *
     * @param clusterOutputIndex
     *            the output channel index for the cluster
     * @return a pair containing the activity id of the corresponding internal activity and the output channel index
     */
    public Pair<ActivityId, Integer> getActivityIdOutputIndex(int clusterOutputIndex) {
        return clusterOutputIndexMap.get(clusterOutputIndex);
    }

    /**
     * Set up the mapping of the cluster's input channel to an internal activity and input output channel
     *
     * @param clusterInputIndex
     *            the input channel index for the cluster
     * @param activityId
     *            the id of the internal activity which consumes the corresponding input
     * @param activityInputIndex
     *            the output channel index of the internal activity which corresponds to the input channel of the cluster of activities
     */
    public void setClusterInputIndex(int clusterInputIndex, ActivityId activityId, int activityInputIndex) {
        clusterInputIndexMap.put(clusterInputIndex, Pair.of(activityId, activityInputIndex));
        invertedClusterInputIndexMap.put(Pair.of(activityId, activityInputIndex), clusterInputIndex);
    }

    /**
     * get the an internal activity and its input channel of a cluster input channel
     *
     * @param clusterOutputIndex
     *            the output channel index for the cluster
     * @return a pair containing the activity id of the corresponding internal activity and the output channel index
     */
    public Pair<ActivityId, Integer> getActivityIdInputIndex(int clusterInputIndex) {
        return clusterInputIndexMap.get(clusterInputIndex);
    }

    /**
     * Get the cluster input channel of an input-boundary activity and its input channel
     *
     * @param activityInputChannel
     *            the input-boundary activity and its input channel
     * @return the cluster input channel
     */
    public int getClusterInputIndex(Pair<ActivityId, Integer> activityInputChannel) {
        Integer channel = invertedClusterInputIndexMap.get(activityInputChannel);
        return channel == null ? -1 : channel;
    }

    /**
     * Get the cluster output channel of an input-boundary activity and its output channel
     *
     * @param activityOutputChannel
     *            the output-boundary activity and its output channel
     * @return the cluster output channel
     */
    public int getClusterOutputIndex(Pair<ActivityId, Integer> activityOutputChannel) {
        Integer channel = invertedClusterOutputIndexMap.get(activityOutputChannel);
        return channel == null ? -1 : channel;
    }

}
