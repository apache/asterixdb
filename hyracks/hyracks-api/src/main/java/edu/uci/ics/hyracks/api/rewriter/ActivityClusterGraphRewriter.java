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
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Queue;
import java.util.Set;

import org.apache.commons.lang3.tuple.Pair;

import edu.uci.ics.hyracks.api.dataflow.ActivityId;
import edu.uci.ics.hyracks.api.dataflow.ConnectorDescriptorId;
import edu.uci.ics.hyracks.api.dataflow.IActivity;
import edu.uci.ics.hyracks.api.dataflow.IConnectorDescriptor;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.job.ActivityCluster;
import edu.uci.ics.hyracks.api.job.ActivityClusterGraph;
import edu.uci.ics.hyracks.api.job.ActivityClusterId;
import edu.uci.ics.hyracks.api.rewriter.runtime.SuperActivity;

/**
 * This class rewrite the AcivityClusterGraph to eliminate
 * all one-to-one connections and merge one-to-one connected
 * DAGs into super activities.
 * </p>
 * Each super activity internally maintains a DAG and execute it at the runtime.
 * 
 * @author yingyib
 */
public class ActivityClusterGraphRewriter {
    private static String ONE_TO_ONE_CONNECTOR = "edu.uci.ics.hyracks.dataflow.std.connectors.OneToOneConnectorDescriptor";

    /**
     * rewrite an activity cluster graph to eliminate
     * all one-to-one connections and merge one-to-one connected
     * DAGs into super activities.
     * 
     * @param acg
     *            the activity cluster graph
     */
    public void rewrite(ActivityClusterGraph acg) {
        acg.getActivityMap().clear();
        acg.getConnectorMap().clear();
        Map<IActivity, SuperActivity> invertedActivitySuperActivityMap = new HashMap<IActivity, SuperActivity>();
        for (Entry<ActivityClusterId, ActivityCluster> entry : acg.getActivityClusterMap().entrySet()) {
            rewriteIntraActivityCluster(entry.getValue(), invertedActivitySuperActivityMap);
        }
        for (Entry<ActivityClusterId, ActivityCluster> entry : acg.getActivityClusterMap().entrySet()) {
            rewriteInterActivityCluster(entry.getValue(), invertedActivitySuperActivityMap);
        }
        invertedActivitySuperActivityMap.clear();
    }

    /**
     * rewrite the blocking relationship among activity cluster
     * 
     * @param ac
     *            the activity cluster to be rewritten
     */
    private void rewriteInterActivityCluster(ActivityCluster ac,
            Map<IActivity, SuperActivity> invertedActivitySuperActivityMap) {
        Map<ActivityId, Set<ActivityId>> blocked2BlockerMap = ac.getBlocked2BlockerMap();
        Map<ActivityId, ActivityId> invertedAid2SuperAidMap = new HashMap<ActivityId, ActivityId>();
        for (Entry<IActivity, SuperActivity> entry : invertedActivitySuperActivityMap.entrySet()) {
            invertedAid2SuperAidMap.put(entry.getKey().getActivityId(), entry.getValue().getActivityId());
        }
        Map<ActivityId, Set<ActivityId>> replacedBlocked2BlockerMap = new HashMap<ActivityId, Set<ActivityId>>();
        for (Entry<ActivityId, Set<ActivityId>> entry : blocked2BlockerMap.entrySet()) {
            ActivityId blocked = entry.getKey();
            ActivityId replacedBlocked = invertedAid2SuperAidMap.get(blocked);
            Set<ActivityId> blockers = entry.getValue();
            Set<ActivityId> replacedBlockers = null;
            if (blockers != null) {
                replacedBlockers = new HashSet<ActivityId>();
                for (ActivityId blocker : blockers) {
                    replacedBlockers.add(invertedAid2SuperAidMap.get(blocker));
                    ActivityCluster dependingAc = ac.getActivityClusterGraph().getActivityMap()
                            .get(invertedAid2SuperAidMap.get(blocker));
                    if (!ac.getDependencies().contains(dependingAc)) {
                        ac.getDependencies().add(dependingAc);
                    }
                }
            }
            if (replacedBlockers != null) {
                Set<ActivityId> existingBlockers = replacedBlocked2BlockerMap.get(replacedBlocked);
                if (existingBlockers == null) {
                    replacedBlocked2BlockerMap.put(replacedBlocked, replacedBlockers);
                } else {
                    existingBlockers.addAll(replacedBlockers);
                    replacedBlocked2BlockerMap.put(replacedBlocked, existingBlockers);
                }
            }
        }
        blocked2BlockerMap.clear();
        blocked2BlockerMap.putAll(replacedBlocked2BlockerMap);
    }

    /**
     * rewrite an activity cluster internally
     * 
     * @param ac
     *            the activity cluster to be rewritten
     */
    private void rewriteIntraActivityCluster(ActivityCluster ac,
            Map<IActivity, SuperActivity> invertedActivitySuperActivityMap) {
        Map<ActivityId, IActivity> activities = ac.getActivityMap();
        Map<ActivityId, List<IConnectorDescriptor>> activityInputMap = ac.getActivityInputMap();
        Map<ActivityId, List<IConnectorDescriptor>> activityOutputMap = ac.getActivityOutputMap();
        Map<ConnectorDescriptorId, Pair<Pair<IActivity, Integer>, Pair<IActivity, Integer>>> connectorActivityMap = ac
                .getConnectorActivityMap();
        ActivityClusterGraph acg = ac.getActivityClusterGraph();
        Map<ActivityId, IActivity> startActivities = new HashMap<ActivityId, IActivity>();
        Map<ActivityId, SuperActivity> superActivities = new HashMap<ActivityId, SuperActivity>();
        Map<ActivityId, Queue<IActivity>> toBeExpendedMap = new HashMap<ActivityId, Queue<IActivity>>();

        /**
         * Build the initial super activities
         */
        for (Entry<ActivityId, IActivity> entry : activities.entrySet()) {
            ActivityId activityId = entry.getKey();
            IActivity activity = entry.getValue();
            if (activityInputMap.get(activityId) == null) {
                startActivities.put(activityId, activity);
                /**
                 * use the start activity's id as the id of the super activity
                 */
                createNewSuperActivity(ac, superActivities, toBeExpendedMap, invertedActivitySuperActivityMap,
                        activityId, activity);
            }
        }

        /**
         * expand one-to-one connected activity cluster by the BFS order.
         * after the while-loop, the original activities are partitioned
         * into equivalent classes, one-per-super-activity.
         */
        Map<ActivityId, SuperActivity> clonedSuperActivities = new HashMap<ActivityId, SuperActivity>();
        while (toBeExpendedMap.size() > 0) {
            clonedSuperActivities.clear();
            clonedSuperActivities.putAll(superActivities);
            for (Entry<ActivityId, SuperActivity> entry : clonedSuperActivities.entrySet()) {
                ActivityId superActivityId = entry.getKey();
                SuperActivity superActivity = entry.getValue();

                /**
                 * for the case where the super activity has already been swallowed
                 */
                if (superActivities.get(superActivityId) == null) {
                    continue;
                }

                /**
                 * expend the super activity
                 */
                Queue<IActivity> toBeExpended = toBeExpendedMap.get(superActivityId);
                if (toBeExpended == null) {
                    /**
                     * Nothing to expand
                     */
                    continue;
                }
                IActivity expendingActivity = toBeExpended.poll();
                List<IConnectorDescriptor> outputConnectors = activityOutputMap.get(expendingActivity.getActivityId());
                if (outputConnectors != null) {
                    for (IConnectorDescriptor outputConn : outputConnectors) {
                        Pair<Pair<IActivity, Integer>, Pair<IActivity, Integer>> endPoints = connectorActivityMap
                                .get(outputConn.getConnectorId());
                        IActivity newActivity = endPoints.getRight().getLeft();
                        SuperActivity existingSuperActivity = invertedActivitySuperActivityMap.get(newActivity);
                        if (outputConn.getClass().getName().contains(ONE_TO_ONE_CONNECTOR)) {
                            /**
                             * expend the super activity cluster on an one-to-one out-bound connection
                             */
                            if (existingSuperActivity == null) {
                                superActivity.addActivity(newActivity);
                                toBeExpended.add(newActivity);
                                invertedActivitySuperActivityMap.put(newActivity, superActivity);
                            } else {
                                /**
                                 * the two activities already in the same super activity
                                 */
                                if (existingSuperActivity == superActivity) {
                                    continue;
                                }
                                /**
                                 * swallow an existing super activity
                                 */
                                swallowExistingSuperActivity(superActivities, toBeExpendedMap,
                                        invertedActivitySuperActivityMap, superActivity, superActivityId,
                                        existingSuperActivity);
                            }
                        } else {
                            if (existingSuperActivity == null) {
                                /**
                                 * create new activity
                                 */
                                createNewSuperActivity(ac, superActivities, toBeExpendedMap,
                                        invertedActivitySuperActivityMap, newActivity.getActivityId(), newActivity);
                            }
                        }
                    }
                }

                /**
                 * remove the to-be-expended queue if it is empty
                 */
                if (toBeExpended.size() == 0) {
                    toBeExpendedMap.remove(superActivityId);
                }
            }
        }

        Map<ConnectorDescriptorId, IConnectorDescriptor> connMap = ac.getConnectorMap();
        Map<ConnectorDescriptorId, RecordDescriptor> connRecordDesc = ac.getConnectorRecordDescriptorMap();
        Map<SuperActivity, Integer> superActivityProducerPort = new HashMap<SuperActivity, Integer>();
        Map<SuperActivity, Integer> superActivityConsumerPort = new HashMap<SuperActivity, Integer>();
        for (Entry<ActivityId, SuperActivity> entry : superActivities.entrySet()) {
            superActivityProducerPort.put(entry.getValue(), 0);
            superActivityConsumerPort.put(entry.getValue(), 0);
        }

        /**
         * create a new activity cluster to replace the old activity cluster
         */
        ActivityCluster newActivityCluster = new ActivityCluster(acg, ac.getId());
        newActivityCluster.setConnectorPolicyAssignmentPolicy(ac.getConnectorPolicyAssignmentPolicy());
        for (Entry<ActivityId, SuperActivity> entry : superActivities.entrySet()) {
            newActivityCluster.addActivity(entry.getValue());
            acg.getActivityMap().put(entry.getKey(), newActivityCluster);
        }

        /**
         * Setup connectors: either inside a super activity or among super activities
         */
        for (Entry<ConnectorDescriptorId, Pair<Pair<IActivity, Integer>, Pair<IActivity, Integer>>> entry : connectorActivityMap
                .entrySet()) {
            ConnectorDescriptorId connectorId = entry.getKey();
            Pair<Pair<IActivity, Integer>, Pair<IActivity, Integer>> endPoints = entry.getValue();
            IActivity producerActivity = endPoints.getLeft().getLeft();
            IActivity consumerActivity = endPoints.getRight().getLeft();
            int producerPort = endPoints.getLeft().getRight();
            int consumerPort = endPoints.getRight().getRight();
            RecordDescriptor recordDescriptor = connRecordDesc.get(connectorId);
            IConnectorDescriptor conn = connMap.get(connectorId);
            if (conn.getClass().getName().contains(ONE_TO_ONE_CONNECTOR)) {
                /**
                 * connection edge between inner activities
                 */
                SuperActivity residingSuperActivity = invertedActivitySuperActivityMap.get(producerActivity);
                residingSuperActivity.connect(conn, producerActivity, producerPort, consumerActivity, consumerPort,
                        recordDescriptor);
            } else {
                /**
                 * connection edge between super activities
                 */
                SuperActivity producerSuperActivity = invertedActivitySuperActivityMap.get(producerActivity);
                SuperActivity consumerSuperActivity = invertedActivitySuperActivityMap.get(consumerActivity);
                int producerSAPort = superActivityProducerPort.get(producerSuperActivity);
                int consumerSAPort = superActivityConsumerPort.get(consumerSuperActivity);
                newActivityCluster.addConnector(conn);
                newActivityCluster.connect(conn, producerSuperActivity, producerSAPort, consumerSuperActivity,
                        consumerSAPort, recordDescriptor);

                /**
                 * bridge the port
                 */
                producerSuperActivity.setClusterOutputIndex(producerSAPort, producerActivity.getActivityId(),
                        producerPort);
                consumerSuperActivity.setClusterInputIndex(consumerSAPort, consumerActivity.getActivityId(),
                        consumerPort);
                acg.getConnectorMap().put(connectorId, newActivityCluster);

                /**
                 * increasing the port number for the producer and consumer
                 */
                superActivityProducerPort.put(producerSuperActivity, ++producerSAPort);
                superActivityConsumerPort.put(consumerSuperActivity, ++consumerSAPort);
            }
        }

        /**
         * Set up the roots of the new activity cluster
         */
        for (Entry<ActivityId, SuperActivity> entry : superActivities.entrySet()) {
            List<IConnectorDescriptor> connIds = newActivityCluster.getActivityOutputMap().get(entry.getKey());
            if (connIds == null || connIds.size() == 0) {
                newActivityCluster.addRoot(entry.getValue());
            }
        }

        /**
         * set up the blocked2Blocker mapping, which will be updated in the rewriteInterActivityCluster call
         */
        newActivityCluster.getBlocked2BlockerMap().putAll(ac.getBlocked2BlockerMap());

        /**
         * replace the old activity cluster with the new activity cluster
         */
        acg.getActivityClusterMap().put(ac.getId(), newActivityCluster);
    }

    /**
     * Create a new super activity
     * 
     * @param acg
     *            the activity cluster
     * @param superActivities
     *            the map from activity id to current super activities
     * @param toBeExpendedMap
     *            the map from an existing super activity to its BFS expansion queue of the original activities
     * @param invertedActivitySuperActivityMap
     *            the map from the original activities to their hosted super activities
     * @param activityId
     *            the activity id for the new super activity, which is the first added acitivty's id in the super activity
     * @param activity
     *            the first activity added to the new super activity
     */
    private void createNewSuperActivity(ActivityCluster acg, Map<ActivityId, SuperActivity> superActivities,
            Map<ActivityId, Queue<IActivity>> toBeExpendedMap,
            Map<IActivity, SuperActivity> invertedActivitySuperActivityMap, ActivityId activityId, IActivity activity) {
        SuperActivity superActivity = new SuperActivity(acg.getActivityClusterGraph(), acg.getId(), activityId);
        superActivities.put(activityId, superActivity);
        superActivity.addActivity(activity);
        Queue<IActivity> toBeExpended = new LinkedList<IActivity>();
        toBeExpended.add(activity);
        toBeExpendedMap.put(activityId, toBeExpended);
        invertedActivitySuperActivityMap.put(activity, superActivity);
    }

    /**
     * One super activity swallows another existing super activity.
     * 
     * @param superActivities
     *            the map from activity id to current super activities
     * @param toBeExpendedMap
     *            the map from an existing super activity to its BFS expansion queue of the original activities
     * @param invertedActivitySuperActivityMap
     *            the map from the original activities to their hosted super activities
     * @param superActivity
     *            the "swallowing" super activity
     * @param superActivityId
     *            the activity id for the "swallowing" super activity, which is also the first added acitivty's id in the super activity
     * @param existingSuperActivity
     *            an existing super activity which is to be swallowed by the "swallowing" super activity
     */
    private void swallowExistingSuperActivity(Map<ActivityId, SuperActivity> superActivities,
            Map<ActivityId, Queue<IActivity>> toBeExpendedMap,
            Map<IActivity, SuperActivity> invertedActivitySuperActivityMap, SuperActivity superActivity,
            ActivityId superActivityId, SuperActivity existingSuperActivity) {
        ActivityId existingSuperActivityId = existingSuperActivity.getActivityId();
        superActivities.remove(existingSuperActivityId);
        for (Entry<ActivityId, IActivity> existingEntry : existingSuperActivity.getActivityMap().entrySet()) {
            IActivity existingActivity = existingEntry.getValue();
            superActivity.addActivity(existingActivity);
            invertedActivitySuperActivityMap.put(existingActivity, superActivity);
        }
        Queue<IActivity> tbeQueue = toBeExpendedMap.get(superActivityId);
        Queue<IActivity> existingTbeQueque = toBeExpendedMap.remove(existingSuperActivityId);
        if (existingTbeQueque != null) {
            tbeQueue.addAll(existingTbeQueque);
        }
    }
}
