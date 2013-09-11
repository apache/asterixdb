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
package edu.uci.ics.hyracks.control.cc.work;

import java.util.Map;

import edu.uci.ics.hyracks.api.dataflow.TaskAttemptId;
import edu.uci.ics.hyracks.api.exceptions.HyracksException;
import edu.uci.ics.hyracks.api.job.JobId;
import edu.uci.ics.hyracks.control.cc.ClusterControllerService;
import edu.uci.ics.hyracks.control.cc.job.JobRun;
import edu.uci.ics.hyracks.control.cc.job.TaskAttempt;
import edu.uci.ics.hyracks.control.common.job.profiling.om.JobProfile;
import edu.uci.ics.hyracks.control.common.job.profiling.om.JobletProfile;
import edu.uci.ics.hyracks.control.common.job.profiling.om.TaskProfile;

public class TaskCompleteWork extends AbstractTaskLifecycleWork {
    private final TaskProfile statistics;

    public TaskCompleteWork(ClusterControllerService ccs, JobId jobId, TaskAttemptId taId, String nodeId,
            TaskProfile statistics) {
        super(ccs, jobId, taId, nodeId);
        this.statistics = statistics;
    }

    @Override
    protected void performEvent(TaskAttempt ta) {
        try {
            JobRun run = ccs.getActiveRunMap().get(jobId);
            if (statistics != null) {
                JobProfile jobProfile = run.getJobProfile();
                Map<String, JobletProfile> jobletProfiles = jobProfile.getJobletProfiles();
                JobletProfile jobletProfile = jobletProfiles.get(nodeId);
                if (jobletProfile == null) {
                    jobletProfile = new JobletProfile(nodeId);
                    jobletProfiles.put(nodeId, jobletProfile);
                }
                jobletProfile.getTaskProfiles().put(taId, statistics);
            }
            run.getScheduler().notifyTaskComplete(ta);
        } catch (HyracksException e) {
            e.printStackTrace();
        }
    }

    @Override
    public String toString() {
        return "TaskCompleteEvent@[" + nodeId + "[" + jobId + ":" + taId + "]";
    }
}