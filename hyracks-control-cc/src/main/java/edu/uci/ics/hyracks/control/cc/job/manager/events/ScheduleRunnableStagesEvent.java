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
package edu.uci.ics.hyracks.control.cc.job.manager.events;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.logging.Logger;

import edu.uci.ics.hyracks.api.comm.Endpoint;
import edu.uci.ics.hyracks.api.dataflow.ActivityNodeId;
import edu.uci.ics.hyracks.api.dataflow.OperatorDescriptorId;
import edu.uci.ics.hyracks.api.dataflow.PortInstanceId;
import edu.uci.ics.hyracks.api.exceptions.HyracksException;
import edu.uci.ics.hyracks.api.job.JobPlan;
import edu.uci.ics.hyracks.api.job.JobStatus;
import edu.uci.ics.hyracks.control.cc.ClusterControllerService;
import edu.uci.ics.hyracks.control.cc.job.JobAttempt;
import edu.uci.ics.hyracks.control.cc.job.JobRun;
import edu.uci.ics.hyracks.control.cc.job.JobStage;
import edu.uci.ics.hyracks.control.cc.job.JobStageAttempt;
import edu.uci.ics.hyracks.control.cc.remote.RemoteRunner;
import edu.uci.ics.hyracks.control.cc.remote.ops.Phase1Installer;
import edu.uci.ics.hyracks.control.cc.remote.ops.Phase2Installer;
import edu.uci.ics.hyracks.control.cc.remote.ops.Phase3Installer;
import edu.uci.ics.hyracks.control.cc.remote.ops.PortMapMergingAccumulator;
import edu.uci.ics.hyracks.control.cc.remote.ops.StageStarter;
import edu.uci.ics.hyracks.control.cc.scheduler.ISchedule;

public class ScheduleRunnableStagesEvent implements Runnable {
    private static final Logger LOGGER = Logger.getLogger(ScheduleRunnableStagesEvent.class.getName());

    private ClusterControllerService ccs;
    private UUID jobId;
    private int attempt;

    public ScheduleRunnableStagesEvent(ClusterControllerService ccs, UUID jobId, int attempt) {
        this.ccs = ccs;
        this.jobId = jobId;
        this.attempt = attempt;
    }

    @Override
    public void run() {
        JobRun run = ccs.getRunMap().get(jobId);
        JobAttempt ja = run.getAttempts().get(attempt);
        Set<UUID> pendingStages = ja.getPendingStageIds();
        Set<UUID> scheduledStages = ja.getInProgressStageIds();

        LOGGER.info(jobId + ":" + attempt + ":Pending stages: " + pendingStages + " Scheduled stages: "
                + scheduledStages);
        if (pendingStages.size() == 1 && scheduledStages.isEmpty()) {
            LOGGER.info(jobId + ":" + attempt + ":No more runnable stages");
            ccs.getJobQueue().schedule(new JobCleanupEvent(ccs, jobId, attempt, JobStatus.TERMINATED));
            return;
        }

        Map<UUID, JobStageAttempt> stageAttemptMap = ja.getStageAttemptMap();

        Set<JobStage> runnableStages = new HashSet<JobStage>();
        ja.findRunnableStages(runnableStages);
        LOGGER.info(jobId + ":" + attempt + ": Found " + runnableStages.size() + " runnable stages");

        Set<JobStageAttempt> runnableStageAttempts = new HashSet<JobStageAttempt>();
        for (JobStage rs : runnableStages) {
            UUID stageId = rs.getId();
            LOGGER.info("Runnable Stage: " + jobId + ":" + rs.getId());
            pendingStages.remove(stageId);
            scheduledStages.add(stageId);
            JobStageAttempt jsa = new JobStageAttempt(rs, ja);
            stageAttemptMap.put(stageId, jsa);
            runnableStageAttempts.add(jsa);
        }

        try {
            ccs.getScheduler().schedule(runnableStageAttempts);
        } catch (HyracksException e) {
            e.printStackTrace();
            ccs.getJobQueue().schedule(new JobAbortEvent(ccs, jobId, attempt));
            return;
        }

        final JobPlan plan = run.getJobPlan();
        for (final JobStageAttempt jsa : runnableStageAttempts) {
            ISchedule schedule = jsa.getSchedule();
            final Map<OperatorDescriptorId, Integer> partCountMap = new HashMap<OperatorDescriptorId, Integer>();
            final Map<String, Map<ActivityNodeId, Set<Integer>>> targetMap = new HashMap<String, Map<ActivityNodeId, Set<Integer>>>();
            for (ActivityNodeId aid : jsa.getJobStage().getTasks()) {
                String[] locations = schedule.getPartitions(aid);
                partCountMap.put(aid.getOperatorDescriptorId(), locations.length);
                int nLoc = locations.length;
                for (int i = 0; i < nLoc; ++i) {
                    Map<ActivityNodeId, Set<Integer>> target = targetMap.get(locations[i]);
                    if (target == null) {
                        target = new HashMap<ActivityNodeId, Set<Integer>>();
                        targetMap.put(locations[i], target);
                    }
                    Set<Integer> partIdxs = target.get(aid);
                    if (partIdxs == null) {
                        partIdxs = new HashSet<Integer>();
                        target.put(aid, partIdxs);
                    }
                    partIdxs.add(i);
                }
            }

            Set<String> participatingNodeIds = ja.getParticipatingNodeIds();
            for (String nid : targetMap.keySet()) {
                ccs.getNodeMap().get(nid).getActiveJobIds().add(jobId);
                participatingNodeIds.add(nid);
            }

            ccs.getExecutor().execute(new Runnable() {
                @Override
                public void run() {
                    Phase1Installer p1is[] = new Phase1Installer[targetMap.size()];
                    int i = 0;
                    for (String nid : targetMap.keySet()) {
                        p1is[i] = new Phase1Installer(nid, plan.getJobId(), plan.getApplicationName(), plan, jsa
                                .getJobStage().getId(), jsa.getJobAttempt().getAttempt(), targetMap.get(nid),
                                partCountMap);
                        ++i;
                    }
                    LOGGER.info("Stage start - Phase 1");
                    try {
                        Map<PortInstanceId, Endpoint> globalPortMap = RemoteRunner.runRemote(ccs, p1is,
                                new PortMapMergingAccumulator());

                        Phase2Installer[] p2is = new Phase2Installer[targetMap.size()];
                        Phase3Installer[] p3is = new Phase3Installer[targetMap.size()];
                        StageStarter[] ss = new StageStarter[targetMap.size()];

                        i = 0;
                        for (String nid : targetMap.keySet()) {
                            p2is[i] = new Phase2Installer(nid, plan.getJobId(), plan.getApplicationName(), plan, jsa
                                    .getJobStage().getId(), targetMap.get(nid), partCountMap, globalPortMap);
                            p3is[i] = new Phase3Installer(nid, plan.getJobId(), jsa.getJobStage().getId());
                            ss[i] = new StageStarter(nid, plan.getJobId(), jsa.getJobStage().getId());
                            ++i;
                        }
                        LOGGER.info("Stage start - Phase 2");
                        RemoteRunner.runRemote(ccs, p2is, null);
                        LOGGER.info("Stage start - Phase 3");
                        RemoteRunner.runRemote(ccs, p3is, null);
                        LOGGER.info("Stage start");
                        RemoteRunner.runRemote(ccs, ss, null);
                        LOGGER.info("Stage started");
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            });
        }
    }
}