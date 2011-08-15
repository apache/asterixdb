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
package edu.uci.ics.hyracks.control.cc.scheduler;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

import edu.uci.ics.hyracks.api.dataflow.ActivityId;
import edu.uci.ics.hyracks.api.dataflow.IActivity;
import edu.uci.ics.hyracks.api.dataflow.IConnectorDescriptor;
import edu.uci.ics.hyracks.api.dataflow.OperatorDescriptorId;
import edu.uci.ics.hyracks.api.job.JobActivityGraph;
import edu.uci.ics.hyracks.api.job.JobSpecification;
import edu.uci.ics.hyracks.api.util.Pair;
import edu.uci.ics.hyracks.control.cc.job.ActivityCluster;
import edu.uci.ics.hyracks.control.cc.job.ActivityClusterId;
import edu.uci.ics.hyracks.control.cc.job.JobRun;

public class ActivityClusterGraphBuilder {
    private static final Logger LOGGER = Logger.getLogger(ActivityClusterGraphBuilder.class.getName());

    private final JobRun jobRun;

    public ActivityClusterGraphBuilder(JobRun jobRun) {
        this.jobRun = jobRun;
    }

    private static Pair<ActivityId, ActivityId> findMergePair(JobActivityGraph jag, JobSpecification spec,
            Set<ActivityCluster> eqSets) {
        Map<ActivityId, IActivity> activityNodeMap = jag.getActivityNodeMap();
        for (ActivityCluster eqSet : eqSets) {
            for (ActivityId t : eqSet.getActivities()) {
                IActivity activity = activityNodeMap.get(t);
                List<Integer> inputList = jag.getActivityInputMap().get(t);
                if (inputList != null) {
                    for (Integer idx : inputList) {
                        IConnectorDescriptor conn = spec.getInputConnectorDescriptor(activity.getActivityId()
                                .getOperatorDescriptorId(), idx);
                        OperatorDescriptorId producerId = spec.getProducer(conn).getOperatorId();
                        int producerOutputIndex = spec.getProducerOutputIndex(conn);
                        ActivityId inTask = jag.getOperatorOutputMap().get(producerId).get(producerOutputIndex);
                        if (!eqSet.getActivities().contains(inTask)) {
                            return new Pair<ActivityId, ActivityId>(t, inTask);
                        }
                    }
                }
                List<Integer> outputList = jag.getActivityOutputMap().get(t);
                if (outputList != null) {
                    for (Integer idx : outputList) {
                        IConnectorDescriptor conn = spec.getOutputConnectorDescriptor(activity.getActivityId()
                                .getOperatorDescriptorId(), idx);
                        OperatorDescriptorId consumerId = spec.getConsumer(conn).getOperatorId();
                        int consumerInputIndex = spec.getConsumerInputIndex(conn);
                        ActivityId outTask = jag.getOperatorInputMap().get(consumerId).get(consumerInputIndex);
                        if (!eqSet.getActivities().contains(outTask)) {
                            return new Pair<ActivityId, ActivityId>(t, outTask);
                        }
                    }
                }
            }
        }
        return null;
    }

    public Set<ActivityCluster> inferActivityClusters(JobActivityGraph jag) {
        JobSpecification spec = jag.getJobSpecification();

        /*
         * Build initial equivalence sets map. We create a map such that for each IOperatorTask, t -> { t }
         */
        Map<ActivityId, ActivityCluster> stageMap = new HashMap<ActivityId, ActivityCluster>();
        Set<ActivityCluster> stages = new HashSet<ActivityCluster>();
        for (Set<ActivityId> taskIds : jag.getOperatorActivityMap().values()) {
            for (ActivityId taskId : taskIds) {
                Set<ActivityId> eqSet = new HashSet<ActivityId>();
                eqSet.add(taskId);
                ActivityCluster stage = new ActivityCluster(jobRun, eqSet);
                stageMap.put(taskId, stage);
                stages.add(stage);
            }
        }

        boolean changed = true;
        while (changed) {
            changed = false;
            Pair<ActivityId, ActivityId> pair = findMergePair(jag, spec, stages);
            if (pair != null) {
                merge(stageMap, stages, pair.first, pair.second);
                changed = true;
            }
        }

        Map<ActivityId, Set<ActivityId>> blocker2BlockedMap = jag.getBlocker2BlockedMap();
        for (ActivityCluster s : stages) {
            Set<ActivityCluster> blockedStages = new HashSet<ActivityCluster>();
            for (ActivityId t : s.getActivities()) {
                Set<ActivityId> blockedTasks = blocker2BlockedMap.get(t);
                if (blockedTasks != null) {
                    for (ActivityId bt : blockedTasks) {
                        blockedStages.add(stageMap.get(bt));
                    }
                }
            }
            for (ActivityCluster bs : blockedStages) {
                bs.addDependency(s);
                s.addDependent(bs);
            }
        }
        Set<ActivityCluster> roots = new HashSet<ActivityCluster>();
        int idCounter = 0;
        for (ActivityCluster s : stages) {
            s.setActivityClusterId(new ActivityClusterId(idCounter++));
            if (s.getDependents().isEmpty()) {
                roots.add(s);
            }
        }
        jobRun.setActivityClusters(stages);
        jobRun.getActivityClusterMap().putAll(stageMap);
        if (LOGGER.isLoggable(Level.INFO)) {
            LOGGER.info("Inferred " + stages.size() + " stages");
            for (ActivityCluster s : stages) {
                LOGGER.info(s.toString());
            }
        }
        return roots;
    }

    private void merge(Map<ActivityId, ActivityCluster> eqSetMap, Set<ActivityCluster> eqSets, ActivityId t1,
            ActivityId t2) {
        ActivityCluster stage1 = eqSetMap.get(t1);
        Set<ActivityId> s1 = stage1.getActivities();
        ActivityCluster stage2 = eqSetMap.get(t2);
        Set<ActivityId> s2 = stage2.getActivities();

        Set<ActivityId> mergedSet = new HashSet<ActivityId>();
        mergedSet.addAll(s1);
        mergedSet.addAll(s2);

        eqSets.remove(stage1);
        eqSets.remove(stage2);
        ActivityCluster mergedStage = new ActivityCluster(jobRun, mergedSet);
        eqSets.add(mergedStage);

        for (ActivityId t : mergedSet) {
            eqSetMap.put(t, mergedStage);
        }
    }
}