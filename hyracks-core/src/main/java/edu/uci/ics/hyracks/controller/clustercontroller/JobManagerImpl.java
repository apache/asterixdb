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

import java.util.ArrayList;
import java.util.Date;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.logging.Level;
import java.util.logging.Logger;

import edu.uci.ics.hyracks.api.comm.Endpoint;
import edu.uci.ics.hyracks.api.dataflow.PortInstanceId;
import edu.uci.ics.hyracks.api.job.JobFlag;
import edu.uci.ics.hyracks.api.job.JobPlan;
import edu.uci.ics.hyracks.api.job.JobSpecification;
import edu.uci.ics.hyracks.api.job.JobStage;
import edu.uci.ics.hyracks.api.job.JobStatus;
import edu.uci.ics.hyracks.api.job.statistics.JobStatistics;
import edu.uci.ics.hyracks.api.job.statistics.StageletStatistics;

public class JobManagerImpl implements IJobManager {
    private static final Logger LOGGER = Logger.getLogger(JobManagerImpl.class.getName());
    private ClusterControllerService ccs;

    private final Map<UUID, JobControl> jobMap;

    private final IJobPlanner planner;

    public JobManagerImpl(ClusterControllerService ccs) {
        this.ccs = ccs;
        jobMap = new HashMap<UUID, JobControl>();
        planner = new NaiveJobPlannerImpl();
    }

    public synchronized UUID createJob(JobSpecification jobSpec, EnumSet<JobFlag> jobFlags) throws Exception {
        JobPlanner planner = new JobPlanner();
        JobControl jc = new JobControl(this, planner.plan(jobSpec, jobFlags));
        if (LOGGER.isLoggable(Level.INFO)) {
            LOGGER.info(jc.getJobPlan().toString());
        }
        jobMap.put(jc.getJobId(), jc);

        return jc.getJobId();
    }

    public synchronized void start(UUID jobId) throws Exception {
        JobControl jobControlImpl = jobMap.get(jobId);
        LOGGER
            .info("Starting job: " + jobControlImpl.getJobId() + ", Current status: " + jobControlImpl.getJobStatus());
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
            ccs.lookupNode(nodeId).getNodeController().startStage(jc.getJobId(), stage.getId());
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
        UUID jobId = jc.getJobId();
        JobPlan plan = jc.getJobPlan();
        UUID stageId = stage.getId();
        Set<String> participatingNodes = plan(jc, stage);
        StageProgress stageProgress = new StageProgress(stage.getId());
        stageProgress.addPendingNodes(participatingNodes);
        ClusterControllerService.Phase1Installer[] p1is = new ClusterControllerService.Phase1Installer[participatingNodes
            .size()];
        ClusterControllerService.Phase2Installer[] p2is = new ClusterControllerService.Phase2Installer[participatingNodes
            .size()];
        ClusterControllerService.Phase3Installer[] p3is = new ClusterControllerService.Phase3Installer[participatingNodes
            .size()];
        int i = 0;
        for (String nodeId : participatingNodes) {
            p1is[i++] = new ClusterControllerService.Phase1Installer(nodeId, jobId, plan, stageId, stage.getTasks());
        }
        Map<PortInstanceId, Endpoint> globalPortMap = ccs.runRemote(p1is,
            new ClusterControllerService.PortMapMergingAccumulator());
        i = 0;
        for (String nodeId : participatingNodes) {
            p2is[i++] = new ClusterControllerService.Phase2Installer(nodeId, jobId, plan, stageId, stage.getTasks(),
                globalPortMap);
        }
        ccs.runRemote(p2is, null);
        i = 0;
        for (String nodeId : participatingNodes) {
            p3is[i++] = new ClusterControllerService.Phase3Installer(nodeId, jobId, stageId);
        }
        ccs.runRemote(p3is, null);
        jc.setStageProgress(stage.getId(), stageProgress);
        return participatingNodes;
    }

    private Set<String> plan(JobControl jc, JobStage stage) throws Exception {
        LOGGER.log(Level.INFO, String.valueOf(jc.getJobId()) + ": Planning");
        Set<String> participatingNodes = planner.plan(jc, stage);
        if (LOGGER.isLoggable(Level.INFO)) {
            LOGGER.info(stage + " Participating nodes: " + participatingNodes);
        }
        return participatingNodes;
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

    @Override
    public synchronized void notifyNodeFailure(String nodeId) {
    }
}