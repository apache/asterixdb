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
package edu.uci.ics.hyracks.job;

import java.util.ArrayList;
import java.util.Date;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.Vector;
import java.util.concurrent.Semaphore;
import java.util.logging.Level;
import java.util.logging.Logger;

import edu.uci.ics.hyracks.api.comm.Endpoint;
import edu.uci.ics.hyracks.api.constraints.AbsoluteLocationConstraint;
import edu.uci.ics.hyracks.api.constraints.LocationConstraint;
import edu.uci.ics.hyracks.api.constraints.PartitionConstraint;
import edu.uci.ics.hyracks.api.controller.INodeController;
import edu.uci.ics.hyracks.api.dataflow.ActivityNodeId;
import edu.uci.ics.hyracks.api.dataflow.IOperatorDescriptor;
import edu.uci.ics.hyracks.api.dataflow.OperatorDescriptorId;
import edu.uci.ics.hyracks.api.dataflow.PortInstanceId;
import edu.uci.ics.hyracks.api.job.JobFlag;
import edu.uci.ics.hyracks.api.job.JobPlan;
import edu.uci.ics.hyracks.api.job.JobSpecification;
import edu.uci.ics.hyracks.api.job.JobStage;
import edu.uci.ics.hyracks.api.job.JobStatus;
import edu.uci.ics.hyracks.api.job.statistics.JobStatistics;
import edu.uci.ics.hyracks.api.job.statistics.StageletStatistics;
import edu.uci.ics.hyracks.controller.ClusterControllerService;
import edu.uci.ics.hyracks.dataflow.base.IOperatorDescriptorVisitor;
import edu.uci.ics.hyracks.dataflow.util.PlanUtils;

public class JobManager {
    private static final Logger LOGGER = Logger.getLogger(JobManager.class.getName());
    private ClusterControllerService ccs;

    private final Map<UUID, JobControl> jobMap;

    public JobManager(ClusterControllerService ccs) {
        this.ccs = ccs;
        jobMap = new HashMap<UUID, JobControl>();
    }

    public synchronized UUID createJob(JobSpecification jobSpec, EnumSet<JobFlag> jobFlags) throws Exception {
        JobPlanBuilder builder = new JobPlanBuilder();
        builder.init(jobSpec, jobFlags);
        JobControl jc = new JobControl(this, builder.plan());
        if (LOGGER.isLoggable(Level.INFO)) {
            LOGGER.info(jc.getJobPlan().toString());
        }
        jobMap.put(jc.getJobId(), jc);

        return jc.getJobId();
    }

    public synchronized void start(UUID jobId) throws Exception {
        JobControl jobControlImpl = jobMap.get(jobId);
        LOGGER
                .info("Starting job: " + jobControlImpl.getJobId() + ", Current status: "
                        + jobControlImpl.getJobStatus());
        if (jobControlImpl.getJobStatus() != JobStatus.INITIALIZED) {
            return;
        }
        jobControlImpl.getJobStatistics().setStartTime(new Date());
        jobControlImpl.setStatus(JobStatus.RUNNING);
        schedule(jobControlImpl);
    }

    public synchronized void advanceJob(JobControl jobControlImpl) throws Exception {
        schedule(jobControlImpl);
    }

    private void schedule(JobControl jobControlImpl) throws Exception {
        JobPlan plan = jobControlImpl.getJobPlan();
        JobStage endStage = plan.getEndStage();

        Set<UUID> completedStages = jobControlImpl.getCompletedStages();
        List<JobStage> runnableStages = new ArrayList<JobStage>();
        findRunnableStages(endStage, runnableStages, completedStages, new HashSet<UUID>());
        if (runnableStages.size() == 1 && runnableStages.get(0).getTasks().isEmpty()) {
            LOGGER.info("Job " + jobControlImpl.getJobId() + " complete");
            jobControlImpl.getJobStatistics().setEndTime(new Date());
            cleanUp(jobControlImpl);
            jobControlImpl.notifyJobComplete();
        } else {
            for (JobStage s : runnableStages) {
                if (s.isStarted()) {
                    continue;
                }
                startStage(jobControlImpl, s);
            }
        }
    }

    private void cleanUp(JobControl jc) {
        jobMap.remove(jc.getJobId());
        ccs.notifyJobComplete(jc.getJobId());
    }

    private void startStage(JobControl jc, JobStage stage) throws Exception {
        stage.setStarted();
        Set<String> candidateNodes = deploy(jc, stage);
        for (String nodeId : candidateNodes) {
            ccs.lookupNode(nodeId).startStage(jc.getJobId(), stage.getId());
        }
    }

    private void findRunnableStages(JobStage s, List<JobStage> runnableStages, Set<UUID> completedStages, Set<UUID> seen) {
        boolean runnable = true;
        if (seen.contains(s.getId())) {
            return;
        }
        seen.add(s.getId());
        for (JobStage dep : s.getDependencies()) {
            boolean depComplete = completedStages.contains(dep.getId());
            runnable = runnable && depComplete;
            if (!depComplete) {
                findRunnableStages(dep, runnableStages, completedStages, seen);
            }
        }
        if (runnable) {
            if (LOGGER.isLoggable(Level.INFO)) {
                LOGGER.info("Runnable stage: " + s);
            }
            runnableStages.add(s);
        }
    }

    private Set<String> deploy(JobControl jc, JobStage stage) throws Exception {
        if (LOGGER.isLoggable(Level.INFO)) {
            LOGGER.info("Deploying: " + stage);
        }
        Set<String> candidateNodes = plan(jc, stage);
        StageProgress stageProgress = new StageProgress(stage.getId());
        stageProgress.addPendingNodes(candidateNodes);
        Map<PortInstanceId, Endpoint> globalPortMap = runRemote(new Phase1Installer(jc, stage),
                new PortMapMergingAccumulator(), candidateNodes);
        runRemote(new Phase2Installer(jc, stage, globalPortMap), null, candidateNodes);
        runRemote(new Phase3Installer(jc, stage), null, candidateNodes);
        jc.setStageProgress(stage.getId(), stageProgress);
        return candidateNodes;
    }

    private interface RemoteOp<T> {
        public T execute(INodeController node) throws Exception;
    }

    private interface Accumulator<T, R> {
        public void accumulate(T o);

        public R getResult();
    }

    private static class Phase1Installer implements RemoteOp<Map<PortInstanceId, Endpoint>> {
        private JobControl jc;
        private JobStage stage;

        public Phase1Installer(JobControl jc, JobStage stage) {
            this.jc = jc;
            this.stage = stage;
        }

        @Override
        public Map<PortInstanceId, Endpoint> execute(INodeController node) throws Exception {
            return node.initializeJobletPhase1(jc.getJobId(), jc.getJobPlan(), stage);
        }

        @Override
        public String toString() {
            return jc.getJobId() + " Distribution Phase 1";
        }
    }

    private static class Phase2Installer implements RemoteOp<Void> {
        private JobControl jc;
        private JobStage stage;
        private Map<PortInstanceId, Endpoint> globalPortMap;

        public Phase2Installer(JobControl jc, JobStage stage, Map<PortInstanceId, Endpoint> globalPortMap) {
            this.jc = jc;
            this.stage = stage;
            this.globalPortMap = globalPortMap;
        }

        @Override
        public Void execute(INodeController node) throws Exception {
            node.initializeJobletPhase2(jc.getJobId(), jc.getJobPlan(), stage, globalPortMap);
            return null;
        }

        @Override
        public String toString() {
            return jc.getJobId() + " Distribution Phase 2";
        }
    }

    private static class Phase3Installer implements RemoteOp<Void> {
        private JobControl jc;
        private JobStage stage;

        public Phase3Installer(JobControl jc, JobStage stage) {
            this.jc = jc;
            this.stage = stage;
        }

        @Override
        public Void execute(INodeController node) throws Exception {
            node.commitJobletInitialization(jc.getJobId(), jc.getJobPlan(), stage);
            return null;
        }

        @Override
        public String toString() {
            return jc.getJobId() + " Distribution Phase 3";
        }
    }

    private static class PortMapMergingAccumulator implements
            Accumulator<Map<PortInstanceId, Endpoint>, Map<PortInstanceId, Endpoint>> {
        Map<PortInstanceId, Endpoint> portMap = new HashMap<PortInstanceId, Endpoint>();

        @Override
        public void accumulate(Map<PortInstanceId, Endpoint> o) {
            portMap.putAll(o);
        }

        @Override
        public Map<PortInstanceId, Endpoint> getResult() {
            return portMap;
        }
    }

    private <T, R> R runRemote(final RemoteOp<T> remoteOp, final Accumulator<T, R> accumulator,
            Set<String> candidateNodes) throws Exception {
        LOGGER.log(Level.INFO, remoteOp + " : " + candidateNodes);

        final Semaphore installComplete = new Semaphore(candidateNodes.size());
        final List<Exception> errors = new Vector<Exception>();
        for (final String nodeId : candidateNodes) {
            final INodeController node = ccs.lookupNode(nodeId);

            installComplete.acquire();
            Runnable remoteRunner = new Runnable() {
                @Override
                public void run() {
                    try {
                        T t = remoteOp.execute(node);
                        if (accumulator != null) {
                            synchronized (accumulator) {
                                accumulator.accumulate(t);
                            }
                        }
                    } catch (Exception e) {
                        errors.add(e);
                    } finally {
                        installComplete.release();
                    }
                }
            };

            ccs.getExecutor().execute(remoteRunner);
        }
        installComplete.acquire(candidateNodes.size());
        if (!errors.isEmpty()) {
            throw errors.get(0);
        }
        return accumulator == null ? null : accumulator.getResult();
    }

    private Set<String> plan(JobControl jc, JobStage stage) throws Exception {
        LOGGER.log(Level.INFO, String.valueOf(jc.getJobId()) + ": Planning");

        final Set<OperatorDescriptorId> opSet = new HashSet<OperatorDescriptorId>();
        for (ActivityNodeId t : stage.getTasks()) {
            opSet.add(jc.getJobPlan().getActivityNodeMap().get(t).getOwner().getOperatorId());
        }

        final Set<String> candidateNodes = new HashSet<String>();

        IOperatorDescriptorVisitor visitor = new IOperatorDescriptorVisitor() {
            @Override
            public void visit(IOperatorDescriptor op) throws Exception {
                if (!opSet.contains(op.getOperatorId())) {
                    return;
                }
                String[] partitions = op.getPartitions();
                if (partitions == null) {
                    PartitionConstraint pc = op.getPartitionConstraint();
                    LocationConstraint[] lcs = pc.getLocationConstraints();
                    String[] assignment = new String[lcs.length];
                    for (int i = 0; i < lcs.length; ++i) {
                        String nodeId = ((AbsoluteLocationConstraint) lcs[i]).getLocationId();
                        assignment[i] = nodeId;
                    }
                    op.setPartitions(assignment);
                    partitions = assignment;
                }
                for (String p : partitions) {
                    candidateNodes.add(p);
                }
            }
        };

        PlanUtils.visit(jc.getJobPlan().getJobSpecification(), visitor);
        if (LOGGER.isLoggable(Level.INFO)) {
            LOGGER.info(stage + " Candidate nodes: " + candidateNodes);
        }
        return candidateNodes;
    }

    public synchronized void notifyStageletComplete(UUID jobId, UUID stageId, String nodeId,
            StageletStatistics statistics) throws Exception {
        JobControl jc = jobMap.get(jobId);
        if (jc != null) {
            jc.notifyStageletComplete(stageId, nodeId, statistics);
        }
    }

    public synchronized JobStatus getJobStatus(UUID jobId) {
        JobControl jc = jobMap.get(jobId);
        return jc.getJobStatus();
    }

    public JobStatistics waitForCompletion(UUID jobId) throws Exception {
        JobControl jc;
        synchronized (this) {
            jc = jobMap.get(jobId);
        }
        if (jc != null) {
            return jc.waitForCompletion();
        }
        return null;
    }
}