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

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import edu.uci.ics.hyracks.api.constraints.expressions.ConstraintExpression;
import edu.uci.ics.hyracks.api.job.JobPlan;
import edu.uci.ics.hyracks.api.job.JobStatus;
import edu.uci.ics.hyracks.control.cc.scheduler.IScheduler;

public class JobRun implements IJobStatusConditionVariable {
    private final JobPlan plan;
    private final List<JobAttempt> attempts;
    private JobStatus status;
    private Set<ConstraintExpression> constraints;

    public JobRun(JobPlan plan, Set<ConstraintExpression> constraints) {
        this.plan = plan;
        attempts = new ArrayList<JobAttempt>();
        this.constraints = constraints;
    }

    public JobPlan getJobPlan() {
        return plan;
    }

    public synchronized void setStatus(JobStatus status) {
        this.status = status;
        notifyAll();
    }

    public JobStatus getStatus() {
        return status;
    }

    public List<JobAttempt> getAttempts() {
        return attempts;
    }

    public Set<ConstraintExpression> getConstraints() {
        return constraints;
    }

    public JobAttempt createAttempt(IScheduler scheduler) {
        int attemptNumber = attempts.size();
        JobAttempt attempt = new JobAttempt(this, plan, attemptNumber, scheduler);
        attempts.add(attempt);
        return attempt;
    }

    @Override
    public synchronized void waitForCompletion() throws Exception {
        while (status != JobStatus.TERMINATED && status != JobStatus.FAILURE) {
            wait();
        }
    }
}