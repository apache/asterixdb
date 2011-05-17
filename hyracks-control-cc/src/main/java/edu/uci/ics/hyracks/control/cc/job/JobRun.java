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
package edu.uci.ics.hyracks.control.cc.job;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import edu.uci.ics.hyracks.api.dataflow.ActivityId;
import edu.uci.ics.hyracks.api.exceptions.HyracksException;
import edu.uci.ics.hyracks.api.job.JobActivityGraph;
import edu.uci.ics.hyracks.api.job.JobStatus;
import edu.uci.ics.hyracks.api.partitions.PartitionId;
import edu.uci.ics.hyracks.control.cc.scheduler.IJobRunStateMachine;
import edu.uci.ics.hyracks.control.common.job.PartitionState;
import edu.uci.ics.hyracks.control.common.job.profiling.om.JobProfile;

public class JobRun implements IJobStatusConditionVariable {
    private final UUID jobId;

    private final JobActivityGraph jag;

    private final Map<PartitionId, Map<String, PartitionState>> partitionAvailabilityMap;

    private final Map<PartitionId, Map<String, PartitionState>> partitionRequestorMap;

    private final Set<String> participatingNodeIds;

    private final JobProfile profile;

    private final Map<ActivityId, ActivityCluster> activityClusterMap;

    private IJobRunStateMachine jsm;

    private JobStatus status;

    private Exception exception;

    public JobRun(UUID jobId, JobActivityGraph plan) {
        this.jobId = jobId;
        this.jag = plan;
        partitionAvailabilityMap = new HashMap<PartitionId, Map<String, PartitionState>>();
        partitionRequestorMap = new HashMap<PartitionId, Map<String, PartitionState>>();
        participatingNodeIds = new HashSet<String>();
        profile = new JobProfile(jobId);
        activityClusterMap = new HashMap<ActivityId, ActivityCluster>();
    }

    public UUID getJobId() {
        return jobId;
    }

    public JobActivityGraph getJobActivityGraph() {
        return jag;
    }

    public synchronized void setStatus(JobStatus status, Exception exception) {
        this.status = status;
        this.exception = exception;
        notifyAll();
    }

    public synchronized JobStatus getStatus() {
        return status;
    }

    public synchronized Exception getException() {
        return exception;
    }

    @Override
    public synchronized void waitForCompletion() throws Exception {
        while (status != JobStatus.TERMINATED && status != JobStatus.FAILURE) {
            wait();
        }
        if (exception != null) {
            throw new HyracksException("Job Failed", exception);
        }
    }

    public Set<String> getParticipatingNodeIds() {
        return participatingNodeIds;
    }

    public JobProfile getJobProfile() {
        return profile;
    }

    public void setStateMachine(IJobRunStateMachine jsm) {
        this.jsm = jsm;
    }

    public IJobRunStateMachine getStateMachine() {
        return jsm;
    }

    public Map<PartitionId, Map<String, PartitionState>> getPartitionAvailabilityMap() {
        return partitionAvailabilityMap;
    }

    public Map<PartitionId, Map<String, PartitionState>> getPartitionRequestorMap() {
        return partitionRequestorMap;
    }

    public Map<ActivityId, ActivityCluster> getActivityClusterMap() {
        return activityClusterMap;
    }
}