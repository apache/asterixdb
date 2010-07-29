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
package edu.uci.ics.hyracks.controller.clustercontroller;

import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

import edu.uci.ics.hyracks.api.dataflow.ActivityNodeId;
import edu.uci.ics.hyracks.api.dataflow.IActivityNode;
import edu.uci.ics.hyracks.api.dataflow.IConnectorDescriptor;
import edu.uci.ics.hyracks.api.dataflow.IOperatorDescriptor;
import edu.uci.ics.hyracks.api.dataflow.OperatorDescriptorId;
import edu.uci.ics.hyracks.api.job.JobFlag;
import edu.uci.ics.hyracks.api.job.JobSpecification;
import edu.uci.ics.hyracks.api.util.Pair;
import edu.uci.ics.hyracks.dataflow.base.IOperatorDescriptorVisitor;
import edu.uci.ics.hyracks.dataflow.util.PlanUtils;
import edu.uci.ics.hyracks.job.JobPlan;
import edu.uci.ics.hyracks.job.JobStage;

public class JobPlanner {
    private static final Logger LOGGER = Logger.getLogger(JobPlanner.class.getName());

    private Pair<ActivityNodeId, ActivityNodeId> findMergePair(JobPlan plan, JobSpecification spec, Set<JobStage> eqSets) {
        Map<ActivityNodeId, IActivityNode> activityNodeMap = plan.getActivityNodeMap();
        for (JobStage eqSet : eqSets) {
            for (ActivityNodeId t : eqSet.getTasks()) {
                IOperatorDescriptor owner = activityNodeMap.get(t).getOwner();
                List<Integer> inputList = plan.getTaskInputMap().get(t);
                if (inputList != null) {
                    for (Integer idx : inputList) {
                        IConnectorDescriptor conn = spec.getInputConnectorDescriptor(owner, idx);
                        OperatorDescriptorId producerId = spec.getProducer(conn).getOperatorId();
                        int producerOutputIndex = spec.getProducerOutputIndex(conn);
                        ActivityNodeId inTask = plan.getOperatorOutputMap().get(producerId).get(producerOutputIndex);
                        if (!eqSet.getTasks().contains(inTask)) {
                            return new Pair<ActivityNodeId, ActivityNodeId>(t, inTask);
                        }
                    }
                }
                List<Integer> outputList = plan.getTaskOutputMap().get(t);
                if (outputList != null) {
                    for (Integer idx : outputList) {
                        IConnectorDescriptor conn = spec.getOutputConnectorDescriptor(owner, idx);
                        OperatorDescriptorId consumerId = spec.getConsumer(conn).getOperatorId();
                        int consumerInputIndex = spec.getConsumerInputIndex(conn);
                        ActivityNodeId outTask = plan.getOperatorInputMap().get(consumerId).get(consumerInputIndex);
                        if (!eqSet.getTasks().contains(outTask)) {
                            return new Pair<ActivityNodeId, ActivityNodeId>(t, outTask);
                        }
                    }
                }
            }
        }
        return null;
    }

    private JobStage inferStages(JobPlan plan) throws Exception {
        JobSpecification spec = plan.getJobSpecification();

        /*
         * Build initial equivalence sets map. We create a map such that for each IOperatorTask, t -> { t }
         */
        Map<ActivityNodeId, JobStage> stageMap = new HashMap<ActivityNodeId, JobStage>();
        Set<JobStage> stages = new HashSet<JobStage>();
        for (Set<ActivityNodeId> taskIds : plan.getOperatorTaskMap().values()) {
            for (ActivityNodeId taskId : taskIds) {
                Set<ActivityNodeId> eqSet = new HashSet<ActivityNodeId>();
                eqSet.add(taskId);
                JobStage stage = new JobStage(eqSet);
                stageMap.put(taskId, stage);
                stages.add(stage);
            }
        }

        boolean changed = true;
        while (changed) {
            changed = false;
            Pair<ActivityNodeId, ActivityNodeId> pair = findMergePair(plan, spec, stages);
            if (pair != null) {
                merge(stageMap, stages, pair.first, pair.second);
                changed = true;
            }
        }

        JobStage endStage = new JobStage(new HashSet<ActivityNodeId>());
        Map<ActivityNodeId, Set<ActivityNodeId>> blocker2BlockedMap = plan.getBlocker2BlockedMap();
        for (JobStage s : stages) {
            endStage.addDependency(s);
            s.addDependent(endStage);
            Set<JobStage> blockedStages = new HashSet<JobStage>();
            for (ActivityNodeId t : s.getTasks()) {
                Set<ActivityNodeId> blockedTasks = blocker2BlockedMap.get(t);
                if (blockedTasks != null) {
                    for (ActivityNodeId bt : blockedTasks) {
                        blockedStages.add(stageMap.get(bt));
                    }
                }
            }
            for (JobStage bs : blockedStages) {
                bs.addDependency(s);
                s.addDependent(bs);
            }
        }
        if (LOGGER.isLoggable(Level.INFO)) {
            LOGGER.info("Inferred " + (stages.size() + 1) + " stages");
            for (JobStage s : stages) {
                LOGGER.info(s.toString());
            }
            LOGGER.info("SID: ENDSTAGE");
        }
        return endStage;
    }

    private void merge(Map<ActivityNodeId, JobStage> eqSetMap, Set<JobStage> eqSets, ActivityNodeId t1,
        ActivityNodeId t2) {
        JobStage stage1 = eqSetMap.get(t1);
        Set<ActivityNodeId> s1 = stage1.getTasks();
        JobStage stage2 = eqSetMap.get(t2);
        Set<ActivityNodeId> s2 = stage2.getTasks();

        Set<ActivityNodeId> mergedSet = new HashSet<ActivityNodeId>();
        mergedSet.addAll(s1);
        mergedSet.addAll(s2);

        eqSets.remove(stage1);
        eqSets.remove(stage2);
        JobStage mergedStage = new JobStage(mergedSet);
        eqSets.add(mergedStage);

        for (ActivityNodeId t : mergedSet) {
            eqSetMap.put(t, mergedStage);
        }
    }

    public JobPlan plan(JobSpecification jobSpec, EnumSet<JobFlag> jobFlags) throws Exception {
        final JobPlanBuilder builder = new JobPlanBuilder();
        builder.init(jobSpec, jobFlags);
        PlanUtils.visit(jobSpec, new IOperatorDescriptorVisitor() {
            @Override
            public void visit(IOperatorDescriptor op) throws Exception {
                op.contributeTaskGraph(builder);
            }
        });
        JobPlan plan = builder.getPlan();
        JobStage endStage = inferStages(plan);
        plan.setEndStage(endStage);

        return plan;
    }
}