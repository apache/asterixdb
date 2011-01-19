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

import java.util.UUID;

import edu.uci.ics.hyracks.api.job.JobStatus;
import edu.uci.ics.hyracks.control.cc.ClusterControllerService;
import edu.uci.ics.hyracks.control.cc.job.JobAttempt;
import edu.uci.ics.hyracks.control.cc.job.JobRun;

public class JobAttemptStartEvent implements Runnable {
    private ClusterControllerService ccs;
    private UUID jobId;

    public JobAttemptStartEvent(ClusterControllerService ccs, UUID jobId) {
        this.ccs = ccs;
        this.jobId = jobId;
    }

    @Override
    public void run() {
        JobRun run = ccs.getRunMap().get(jobId);
        int maxRetries = run.getJobPlan().getJobSpecification().getMaxRetries();
        if (run.getAttempts().size() > maxRetries) {
            run.setStatus(JobStatus.FAILURE);
            return;
        }
        JobAttempt attempt = run.createAttempt(ccs.getScheduler());
        new ScheduleRunnableStagesEvent(ccs, jobId, attempt.getAttempt()).run();
    }
}