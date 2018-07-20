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
package org.apache.hyracks.control.cc.work;

import java.util.Map;

import org.apache.hyracks.api.dataflow.TaskAttemptId;
import org.apache.hyracks.api.job.JobId;
import org.apache.hyracks.control.cc.ClusterControllerService;
import org.apache.hyracks.control.cc.job.IJobManager;
import org.apache.hyracks.control.cc.job.JobRun;
import org.apache.hyracks.control.cc.job.TaskAttempt;
import org.apache.hyracks.control.common.job.profiling.om.JobProfile;
import org.apache.hyracks.control.common.job.profiling.om.JobletProfile;
import org.apache.hyracks.control.common.job.profiling.om.TaskProfile;

public class TaskCompleteWork extends AbstractTaskLifecycleWork {
    private final TaskProfile statistics;

    public TaskCompleteWork(ClusterControllerService ccs, JobId jobId, TaskAttemptId taId, String nodeId,
            TaskProfile statistics) {
        super(ccs, jobId, taId, nodeId);
        this.statistics = statistics;
    }

    @Override
    protected void performEvent(TaskAttempt ta) {
        IJobManager jobManager = ccs.getJobManager();
        JobRun run = jobManager.get(jobId);
        if (run == null) {
            return;
        }
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
        run.getExecutor().notifyTaskComplete(ta);
    }

    @Override
    public String toString() {
        return getName() + ": [" + nodeId + "[" + jobId + ":" + taId + "]";
    }
}
