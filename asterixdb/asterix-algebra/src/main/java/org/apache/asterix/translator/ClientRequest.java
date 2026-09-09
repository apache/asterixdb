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
package org.apache.asterix.translator;

import static org.apache.hyracks.api.job.resource.IJobCapacityController.JobSubmissionStatus.QUEUE;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.asterix.common.api.ICommonRequestParameters;
import org.apache.asterix.common.dataflow.ICcApplicationContext;
import org.apache.asterix.om.base.AMutableDateTime;
import org.apache.hyracks.api.client.IHyracksClientConnection;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.job.JobId;
import org.apache.hyracks.api.job.JobStatus;
import org.apache.hyracks.api.job.resource.IJobCapacityController;
import org.apache.hyracks.api.job.resource.IReadOnlyClusterCapacity;
import org.apache.hyracks.api.util.ExceptionUtils;
import org.apache.hyracks.util.LogRedactionUtil;
import org.apache.hyracks.util.StorageUtil;
import org.apache.hyracks.util.annotations.AiProvenance;

import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

public class ClientRequest extends BaseClientRequest {

    protected static final int MAX_STATEMENT_LENGTH =
            StorageUtil.getIntSizeInBytes(64, StorageUtil.StorageUnit.KILOBYTE);
    // what a request that has not created a job yet reports
    private static final JobState EMPTY_JOB_STATE = new JobState();
    protected final long creationTime = System.nanoTime();
    protected final long creationSystemTime = System.currentTimeMillis();
    protected Thread executor;
    protected final String statement;
    protected final String clientContextId;
    /**
     * The jobs of this request, in creation order: one per statement that submits one.
     * <p>
     * Guarded by {@link #jobsLock}, not by the request monitor: {@link #doCancel} holds the monitor while
     * it waits on the job manager to abort a job, and the job manager's own thread reports that job's
     * outcome back here - taking the monitor for that would deadlock.
     */
    private final List<RequestJob> jobs = new ArrayList<>();
    private final Object jobsLock = new Object();
    private volatile long compileTimeNanos;
    private volatile int statementPosition;
    private volatile boolean multiStatement;

    public ClientRequest(ICommonRequestParameters requestParameters) {
        super(requestParameters.getRequestReference());
        this.clientContextId = requestParameters.getClientContextId();
        String stmt = requestParameters.getStatement();
        this.statement = stmt.length() > MAX_STATEMENT_LENGTH ? stmt.substring(0, MAX_STATEMENT_LENGTH) : stmt;
        this.executor = Thread.currentThread();
    }

    @Override
    public String getClientContextId() {
        return clientContextId;
    }

    @Override
    public void archived() {
        executor = null;
    }

    public void setPlan(JobId jobId, String plan) {
        if (plan != null) {
            synchronized (jobsLock) {
                jobStateOf(jobId).plan =
                        plan.length() > MAX_STATEMENT_LENGTH ? plan.substring(0, MAX_STATEMENT_LENGTH) : plan;
            }
        }
    }

    /**
     * Adopts the job just submitted for the statement being executed. The job may be known here already: its
     * creation notification is delivered while the submission is still in flight.
     *
     * @return false if the request was cancelled in the meantime, in which case the caller must cancel the job
     *         itself, the cancellation not having been able to reach a job that did not yet exist
     */
    public synchronized boolean addJob(JobId jobId) {
        if (isCancelled()) {
            return false;
        }
        synchronized (jobsLock) {
            // registers the job as well, so that a handle of it is authorised whether or not the creation
            // notification has been delivered yet
            RequestJob job = jobOf(jobId);
            job.state.compileTimeNanos = compileTimeNanos;
            job.statementPosition = statementPosition;
        }
        setRunning();
        return true;
    }

    /** @return the ids of this request's jobs, in creation order */
    public List<JobId> getJobIds() {
        synchronized (jobsLock) {
            return jobs.stream().map(job -> job.jobId).toList();
        }
    }

    /** @return true if the given job is one of this request's */
    public boolean hasJob(JobId jobId) {
        synchronized (jobsLock) {
            return findJob(jobId) != null;
        }
    }

    @Override
    public void markResultPending(JobId jobId) {
        synchronized (jobsLock) {
            jobStateOf(jobId).resultPending = true;
        }
    }

    @Override
    public void resultSwept(JobId jobId) {
        synchronized (jobsLock) {
            RequestJob job = findJob(jobId);
            if (job != null) {
                job.state.resultPending = false;
            }
        }
    }

    @Override
    public boolean hasPendingResults() {
        synchronized (jobsLock) {
            return jobs.stream().anyMatch(job -> job.state.resultPending);
        }
    }

    /**
     * @return the compile time of the statement that submitted the given job, 0 if it is not this request's
     */
    public long getCompileTimeNanos(JobId jobId) {
        synchronized (jobsLock) {
            RequestJob job = findJob(jobId);
            return job != null ? job.state.compileTimeNanos : 0;
        }
    }

    /**
     * @return the jobs that have not reached a terminal state. The statements of a request run one at a time
     *         and each waits for its own job, and a job's outcome is recorded here before that wait is
     *         released, so this is the job of the statement being executed, or nothing between two statements.
     */
    public List<JobId> getUnfinishedJobIds() {
        synchronized (jobsLock) {
            return jobs.stream().filter(job -> !isTerminal(job.state.status)).map(job -> job.jobId).toList();
        }
    }

    /** A job with no status yet has been submitted and its creation not yet notified, so it is not terminal. */
    private static boolean isTerminal(JobStatus status) {
        return status == JobStatus.TERMINATED || status == JobStatus.FAILURE
                || status == JobStatus.FAILURE_BEFORE_EXECUTION;
    }

    /** @return the jobs whose results the client was never handed a handle for */
    public List<JobId> getJobsWithoutPendingResults() {
        synchronized (jobsLock) {
            return jobs.stream().filter(job -> !job.state.resultPending).map(job -> job.jobId).toList();
        }
    }

    public Thread getExecutor() {
        return executor;
    }

    @Override
    protected void doCancel(ICcApplicationContext appCtx) throws HyracksDataException {
        // Only the jobs that have not finished are aborted: a finished job's results are what a handle already
        // handed to the client points at, and cancelling it would do nothing in any case. There is at most one
        // such job - see getUnfinishedJobIds - but nothing here relies on that.
        // With no such job nothing would tell the executing thread that the request was cancelled, it being
        // between two statements or compiling one, so the thread is interrupted instead.
        List<JobId> jobIds = getUnfinishedJobIds();
        if (!jobIds.isEmpty()) {
            IHyracksClientConnection hcc = appCtx.getHcc();
            HyracksDataException failure = null;
            for (JobId jobId : jobIds) {
                try {
                    hcc.cancelJob(jobId);
                } catch (Exception e) {
                    if (failure == null) {
                        failure = HyracksDataException.create(e);
                    } else {
                        failure.addSuppressed(e);
                    }
                }
            }
            if (failure != null) {
                throw failure;
            }
        } else if (executor != null) {
            executor.interrupt();
        }
    }

    public long getCreationTime() {
        return creationTime;
    }

    public long getCreationSystemTime() {
        return creationSystemTime;
    }

    /**
     * Staged here and copied onto the job by {@link #addJob}: a statement compiles before it submits, so
     * the value last staged is the submitting statement's. The compile time is known inside the compiler,
     * where there is no job id yet.
     */
    public void setCompileTimeNanos(long compileTimeNanos) {
        this.compileTimeNanos = compileTimeNanos;
    }

    /**
     * Staged and copied onto the job by {@link #addJob}, as the compile time is: it is set as each statement
     * starts, so the value last staged is the submitting statement's.
     *
     * @param statementPosition the statement's position among the client's, counting from 1; 0 for a statement
     *                          of the request's own, which is not the client's to see
     */
    @AiProvenance(agent = AiProvenance.Agent.CLAUDE_OPUS_5, tool = AiProvenance.Tool.CLAUDE_CODE_UI, contributionKind = AiProvenance.ContributionKind.GENERATED)
    public void setStatementPosition(int statementPosition) {
        this.statementPosition = statementPosition;
    }

    /**
     * Recorded before the first statement runs, so that what the request reports as its own does not depend
     * on how many jobs it happens to have created by the time it is read. See {@link #requestJob}.
     *
     * @param multiStatement whether the request carries more than one statement of the client's own
     */
    @AiProvenance(agent = AiProvenance.Agent.CLAUDE_OPUS_5, tool = AiProvenance.Tool.CLAUDE_CODE_UI, contributionKind = AiProvenance.ContributionKind.GENERATED)
    public void setMultiStatement(boolean multiStatement) {
        this.multiStatement = multiStatement;
    }

    @Override
    public ObjectNode asJson() {
        ObjectNode json = super.asJson();
        return asJson(json, false);
    }

    @Override
    public ObjectNode asRedactedJson() {
        ObjectNode json = super.asRedactedJson();
        return asJson(json, true);
    }

    private ObjectNode asJson(ObjectNode json, boolean redact) {
        putJobDetails(json, redact);
        json.put("statement", redact ? LogRedactionUtil.statement(statement) : statement);
        json.put("clientContextID", clientContextId);
        // the plan describes the request, so it is reported here only where the request has one of its own,
        // by the same rule as the job id it belongs to; a statement's plan is reported on that statement's job
        String plan = requestPlan();
        if (plan != null) {
            json.put("plan", redact ? LogRedactionUtil.userData(plan) : plan);
        }
        return json;
    }

    @Override
    public void jobCreated(JobId jobId, IReadOnlyClusterCapacity requiredClusterCapacity,
            IJobCapacityController.JobSubmissionStatus status) {
        JobState jobState = jobState(jobId);
        jobState.createTime = System.currentTimeMillis();
        jobState.status = status == QUEUE ? JobStatus.PENDING : JobStatus.RUNNING;
        jobState.requiredCPUs = requiredClusterCapacity.getAggregatedCores();
        jobState.requiredMemoryInBytes = requiredClusterCapacity.getAggregatedMemoryByteSize();
    }

    @Override
    public void jobStarted(JobId jobId) {
        JobState jobState = jobState(jobId);
        jobState.startTime = System.currentTimeMillis();
        jobState.status = JobStatus.RUNNING;
    }

    @Override
    public void jobFinished(JobId jobId, JobStatus jobStatus, List<Exception> exceptions) {
        JobState jobState = jobState(jobId);
        jobState.endTime = System.currentTimeMillis();
        jobState.status = jobStatus;
        if (exceptions != null && !exceptions.isEmpty()) {
            jobState.errorMsg = processException(exceptions.get(0));
        }
    }

    protected String processException(Exception e) {
        return ExceptionUtils.unwrap(e).getMessage();
    }

    private JobState jobState(JobId jobId) {
        synchronized (jobsLock) {
            return jobStateOf(jobId);
        }
    }

    // must be called while holding jobsLock
    private JobState jobStateOf(JobId jobId) {
        return jobOf(jobId).state;
    }

    /** @return this request's job of the given id, adopting it if it is not known here yet */
    // must be called while holding jobsLock
    private RequestJob jobOf(JobId jobId) {
        RequestJob job = findJob(jobId);
        if (job == null) {
            job = new RequestJob(jobId);
            jobs.add(job);
        }
        return job;
    }

    // must be called while holding jobsLock
    private RequestJob findJob(JobId jobId) {
        for (RequestJob job : jobs) {
            if (job.jobId.equals(jobId)) {
                return job;
            }
        }
        return null;
    }

    @AiProvenance(agent = AiProvenance.Agent.CLAUDE_OPUS_5, tool = AiProvenance.Tool.CLAUDE_CODE_UI, contributionKind = AiProvenance.ContributionKind.REFACTORED)
    private void putJobDetails(ObjectNode json, boolean redact) {
        try {
            List<RequestJob> requestJobs;
            synchronized (jobsLock) {
                requestJobs = List.copyOf(jobs);
            }
            // The flat fields describe the request as a whole: one job reports itself, as it always has, and
            // several roll up - see rollUp. What each job did on its own is in the jobs array.
            RequestJob requestJob = requestJob(requestJobs);
            json.put("jobId", requestJob != null ? requestJob.jobId.toString() : null);
            putJobState(json, rollUp(requestJobs), queueTimeMillis(requestJobs), redact);
            // written for every request, one job or not, so that a reader has one place that is always right
            ArrayNode jobsJson = json.putArray("jobs");
            for (RequestJob job : requestJobs) {
                ObjectNode jobJson = jobsJson.addObject();
                // the statement that submitted the job, named as the response names it. Not every statement
                // submits a job, so a job's position in this array does not identify the statement.
                if (job.statementPosition > 0) {
                    jobJson.put("statement", job.statementPosition);
                }
                jobJson.put("jobId", job.jobId.toString());
                putJobState(jobJson, job.state, queueTimeMillis(job.state), redact);
                if (job.state.plan != null) {
                    jobJson.put("plan", redact ? LogRedactionUtil.userData(job.state.plan) : job.state.plan);
                }
            }
        } catch (Throwable th) {
            // ignore
        }
    }

    /**
     * The job that is this request, and whose id and plan the request reports as its own: the single job
     * that a request of a single statement submits. A request of several statements has no such job - no one
     * of its jobs is the request - and reports none for its whole life, rather than reporting its first
     * job's id until a second statement submits a job and dropping it then. Null is what a request that
     * created no job has always reported, and what the API documents.
     *
     * @return that job, or null where no one job is the request
     */
    @AiProvenance(agent = AiProvenance.Agent.CLAUDE_OPUS_5, tool = AiProvenance.Tool.CLAUDE_CODE_UI, contributionKind = AiProvenance.ContributionKind.GENERATED)
    private RequestJob requestJob(List<RequestJob> requestJobs) {
        return !multiStatement && requestJobs.size() == 1 ? requestJobs.get(0) : null;
    }

    /**
     * What the flat fields report for the request as a whole. A request with a single job reports that job's
     * state unchanged; several roll up to the first job's creation and start, the last job's end once they
     * have all finished, the peak of what they required - they run one at a time - and the first error any of
     * them reported, which for a request that stops at its first failing statement is that statement's.
     */
    @AiProvenance(agent = AiProvenance.Agent.CLAUDE_OPUS_5, tool = AiProvenance.Tool.CLAUDE_CODE_UI, contributionKind = AiProvenance.ContributionKind.GENERATED)
    private static JobState rollUp(List<RequestJob> requestJobs) {
        if (requestJobs.isEmpty()) {
            return EMPTY_JOB_STATE;
        }
        if (requestJobs.size() == 1) {
            return requestJobs.get(0).state;
        }
        JobState rolled = new JobState();
        boolean allFinished = true;
        for (RequestJob job : requestJobs) {
            JobState state = job.state;
            rolled.createTime = earlier(rolled.createTime, state.createTime);
            rolled.startTime = earlier(rolled.startTime, state.startTime);
            rolled.endTime = Math.max(rolled.endTime, state.endTime);
            rolled.requiredCPUs = Math.max(rolled.requiredCPUs, state.requiredCPUs);
            rolled.requiredMemoryInBytes = Math.max(rolled.requiredMemoryInBytes, state.requiredMemoryInBytes);
            allFinished &= isTerminal(state.status);
            if (rolled.errorMsg == null) {
                rolled.errorMsg = state.errorMsg;
            }
        }
        if (!allFinished) {
            // a job of the request is still to end, so the end of the ones that have is not the request's
            rolled.endTime = 0;
        }
        rolled.status = rollUpStatus(requestJobs);
        return rolled;
    }

    /**
     * The status of the request's jobs as one: the failure if any of them failed - the request stops at its
     * first failing statement - and otherwise the least finished of them, so that a request still doing
     * something does not report itself terminated. Null, as a single job's unreported status is, where a job
     * has been submitted and its creation not yet notified.
     */
    @AiProvenance(agent = AiProvenance.Agent.CLAUDE_OPUS_5, tool = AiProvenance.Tool.CLAUDE_CODE_UI, contributionKind = AiProvenance.ContributionKind.GENERATED)
    private static JobStatus rollUpStatus(List<RequestJob> requestJobs) {
        JobStatus failure = null;
        boolean running = false;
        boolean pending = false;
        boolean unreported = false;
        for (RequestJob job : requestJobs) {
            JobStatus status = job.state.status;
            if (status == null) {
                unreported = true;
                continue;
            }
            switch (status) {
                case FAILURE:
                case FAILURE_BEFORE_EXECUTION:
                    if (failure == null) {
                        failure = status;
                    }
                    break;
                case RUNNING:
                    running = true;
                    break;
                case PENDING:
                    pending = true;
                    break;
                default:
                    break;
            }
        }
        if (failure != null) {
            return failure;
        } else if (running) {
            return JobStatus.RUNNING;
        } else if (pending) {
            return JobStatus.PENDING;
        }
        return unreported ? null : JobStatus.TERMINATED;
    }

    /** @return the earlier of two times, either of which may be unset */
    @AiProvenance(agent = AiProvenance.Agent.CLAUDE_OPUS_5, tool = AiProvenance.Tool.CLAUDE_CODE_UI, contributionKind = AiProvenance.ContributionKind.GENERATED)
    private static long earlier(long time, long otherTime) {
        if (time == 0) {
            return otherTime;
        }
        return otherTime == 0 ? time : Math.min(time, otherTime);
    }

    /**
     * @return the plan of the request, where it has one of its own: the plan of the job that is the request,
     *         as the reported id is that job's - see {@link #requestJob}. A request of several statements has
     *         no plan of its own; each of their plans belongs to its statement, and is reported on that
     *         statement's job.
     */
    @AiProvenance(agent = AiProvenance.Agent.CLAUDE_OPUS_5, tool = AiProvenance.Tool.CLAUDE_CODE_UI, contributionKind = AiProvenance.ContributionKind.GENERATED)
    private String requestPlan() {
        RequestJob requestJob;
        synchronized (jobsLock) {
            requestJob = requestJob(jobs);
        }
        return requestJob != null ? requestJob.state.plan : null;
    }

    @AiProvenance(agent = AiProvenance.Agent.CLAUDE_OPUS_5, tool = AiProvenance.Tool.CLAUDE_CODE_UI, contributionKind = AiProvenance.ContributionKind.REFACTORED)
    private static void putJobState(ObjectNode json, JobState state, long queueTimeMillis, boolean redact) {
        AMutableDateTime dateTime = new AMutableDateTime(0);
        putTime(json, state.createTime, "jobCreateTime", dateTime);
        putTime(json, state.startTime, "jobStartTime", dateTime);
        putTime(json, state.endTime, "jobEndTime", dateTime);
        json.put("jobQueueTime", TimeUnit.MILLISECONDS.toSeconds(queueTimeMillis));
        json.put("jobStatus", String.valueOf(state.status));
        json.put("jobRequiredCPUs", state.requiredCPUs);
        json.put("jobRequiredMemory", state.requiredMemoryInBytes);
        if (state.errorMsg != null) {
            json.put("error", redact ? LogRedactionUtil.userData(state.errorMsg) : state.errorMsg);
        }
    }

    /**
     * @return the time the request spent queued: the sum over its jobs, whose queue intervals do not overlap,
     *         its statements running one at a time
     */
    @AiProvenance(agent = AiProvenance.Agent.CLAUDE_OPUS_5, tool = AiProvenance.Tool.CLAUDE_CODE_UI, contributionKind = AiProvenance.ContributionKind.GENERATED)
    private static long queueTimeMillis(List<RequestJob> requestJobs) {
        long queueTime = 0;
        for (RequestJob job : requestJobs) {
            queueTime += queueTimeMillis(job.state);
        }
        return queueTime;
    }

    /**
     * @return startTime - createTime, if the job has started; endTime - createTime, if it ended without
     *         starting (failed while in the queue, cancelled/timeout); currentTime - createTime, if it is
     *         still in the queue
     */
    @AiProvenance(agent = AiProvenance.Agent.CLAUDE_OPUS_5, tool = AiProvenance.Tool.CLAUDE_CODE_UI, contributionKind = AiProvenance.ContributionKind.GENERATED)
    private static long queueTimeMillis(JobState state) {
        if (state.createTime == 0) {
            return 0;
        }
        long queuedUntil = state.startTime > 0 ? state.startTime
                : (state.endTime > 0 ? state.endTime : System.currentTimeMillis());
        return queuedUntil - state.createTime;
    }

    private static void putTime(ObjectNode json, long time, String label, AMutableDateTime dateTime) {
        if (time > 0) {
            dateTime.setValue(time);
            json.put(label, dateTime.toSimpleString());
        }
    }

    static class JobState {
        volatile long createTime;
        volatile long startTime;
        volatile long endTime;
        volatile long requiredMemoryInBytes;
        volatile int requiredCPUs;
        volatile JobStatus status;
        volatile String errorMsg;
        volatile String plan; // can be null
        // kept per job because it is the compile time of that job's statement; not reported in the response
        volatile long compileTimeNanos;
        // the client holds a handle for these results and may still fetch them
        volatile boolean resultPending;
    }

    private static class RequestJob {
        final JobId jobId;
        final JobState state = new JobState();
        /** The position of the statement that submitted the job; 0 where it is not known. */
        volatile int statementPosition;

        RequestJob(JobId jobId) {
            this.jobId = jobId;
        }
    }
}
