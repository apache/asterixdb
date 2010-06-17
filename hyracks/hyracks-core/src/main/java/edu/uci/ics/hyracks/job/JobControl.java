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

import java.rmi.RemoteException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import edu.uci.ics.hyracks.api.job.JobPlan;
import edu.uci.ics.hyracks.api.job.JobStatus;
import edu.uci.ics.hyracks.api.job.statistics.JobStatistics;
import edu.uci.ics.hyracks.api.job.statistics.StageStatistics;
import edu.uci.ics.hyracks.api.job.statistics.StageletStatistics;

public class JobControl {
    private static final long serialVersionUID = 1L;

    private final JobManager jobManager;

    private final JobPlan jobPlan;

    private final UUID jobId;

    private final Map<UUID, StageProgress> stageProgressMap;

    private final Set<UUID> completeStages;

    private JobStatus jobStatus;

    private JobStatistics jobStatistics;

    public JobControl(JobManager jobManager, JobPlan jobPlan) throws RemoteException {
        this.jobManager = jobManager;
        this.jobPlan = jobPlan;
        jobId = UUID.randomUUID();
        stageProgressMap = new HashMap<UUID, StageProgress>();
        completeStages = new HashSet<UUID>();
        jobStatus = JobStatus.INITIALIZED;
        jobStatistics = new JobStatistics();
    }

    public JobPlan getJobPlan() {
        return jobPlan;
    }

    public UUID getJobId() {
        return jobId;
    }

    public synchronized JobStatus getJobStatus() {
        return jobStatus;
    }

    public Set<UUID> getCompletedStages() {
        return completeStages;
    }

    public void setStatus(JobStatus status) {
        jobStatus = status;
    }

    public StageProgress getStageProgress(int stageId) {
        return stageProgressMap.get(stageId);
    }

    public void setStageProgress(UUID stageId, StageProgress stageProgress) {
        stageProgressMap.put(stageId, stageProgress);
    }

    public synchronized void notifyStageletComplete(UUID stageId, String nodeId, StageletStatistics ss)
            throws Exception {
        StageProgress stageProgress = stageProgressMap.get(stageId);
        stageProgress.markNodeComplete(nodeId);
        StageStatistics stageStatistics = stageProgress.getStageStatistics();
        stageStatistics.addStageletStatistics(ss);
        if (stageProgress.stageComplete()) {
            jobStatistics.addStageStatistics(stageStatistics);
            stageProgressMap.remove(stageId);
            completeStages.add(stageId);
            jobManager.advanceJob(this);
        }
    }

    public synchronized JobStatistics waitForCompletion() throws Exception {
        while (jobStatus != JobStatus.TERMINATED) {
            wait();
        }
        return jobStatistics;
    }

    public synchronized void notifyJobComplete() {
        jobStatus = JobStatus.TERMINATED;
        notifyAll();
    }

    public JobStatistics getJobStatistics() {
        return jobStatistics;
    }
}