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
package org.apache.asterix.test.translator;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.asterix.common.api.ICommonRequestParameters;
import org.apache.asterix.common.api.IRequestReference;
import org.apache.asterix.common.dataflow.ICcApplicationContext;
import org.apache.asterix.translator.ClientRequest;
import org.apache.hyracks.api.client.IHyracksClientConnection;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.job.JobId;
import org.apache.hyracks.api.job.JobStatus;
import org.apache.hyracks.api.job.resource.IJobCapacityController;
import org.apache.hyracks.api.job.resource.IReadOnlyClusterCapacity;
import org.apache.hyracks.util.annotations.AiProvenance;
import org.junit.Test;
import org.mockito.Mockito;

import com.fasterxml.jackson.databind.JsonNode;

/** The jobs of a request are all its own: reported and swept together, and the running one cancelled. */
public class ClientRequestTest {

    private static final JobId JOB_1 = new JobId(1);
    private static final JobId JOB_2 = new JobId(2);
    private static final JobId JOB_3 = new JobId(3);

    @Test
    public void cancelAbortsTheUnfinishedJobAndLeavesTheFinishedOnes() throws Exception {
        ClientRequest request = newRequest();
        IHyracksClientConnection hcc = mock(IHyracksClientConnection.class);
        runJob(request, JOB_1, JobStatus.TERMINATED, null);
        request.addJob(JOB_2);
        request.jobStarted(JOB_2);
        request.markCancellable();

        assertTrue(request.cancel(appCtx(hcc)));

        assertEquals(List.of(JOB_2), request.getUnfinishedJobIds());
        verify(hcc).cancelJob(JOB_2);
        verify(hcc, never()).cancelJob(JOB_1);
    }

    /**
     * Between two statements, or while one is compiling, there is no job whose abort would tell the
     * executing thread that the request was cancelled, so it is interrupted and the remaining statements
     * do not run.
     */
    @Test
    public void cancelInterruptsTheExecutorWhenNoJobIsRunning() throws Exception {
        CountDownLatch interrupted = new CountDownLatch(1);
        Thread executor = new Thread(() -> {
            try {
                Thread.sleep(TimeUnit.MINUTES.toMillis(1));
            } catch (InterruptedException e) {
                interrupted.countDown();
            }
        });
        ClientRequest request = newRequestOn(executor);
        runJob(request, JOB_1, JobStatus.TERMINATED, null);
        executor.start();
        request.markCancellable();

        assertTrue(request.cancel(appCtx(mock(IHyracksClientConnection.class))));

        assertTrue("the executor thread was not interrupted", interrupted.await(30, TimeUnit.SECONDS));
    }

    /**
     * Defensive: a request has at most one unfinished job, its statements running one at a time, and
     * cancel does not rely on that - it aborts each of them and reports the first failure.
     */
    @Test
    public void cancelAbortsEveryUnfinishedJobEvenIfOneFails() throws Exception {
        ClientRequest request = newRequest();
        IHyracksClientConnection hcc = mock(IHyracksClientConnection.class);
        Mockito.doThrow(new IllegalStateException("job 1 cannot be cancelled")).when(hcc).cancelJob(JOB_1);
        request.addJob(JOB_1);
        request.addJob(JOB_2);
        request.markCancellable();

        assertThrows(HyracksDataException.class, () -> request.cancel(appCtx(hcc)));

        verify(hcc).cancelJob(JOB_2);
    }

    /** A job submitted while the request was being cancelled must be cancelled by its caller instead. */
    @Test
    public void aJobSubmittedAfterCancellationIsRejected() throws Exception {
        ClientRequest request = newRequest();
        request.addJob(JOB_1);
        request.markCancellable();
        request.cancel(appCtx(mock(IHyracksClientConnection.class)));

        assertFalse(request.addJob(JOB_2));
    }

    @Test
    public void eachJobKeepsItsOwnPlanAndCompileTime() {
        ClientRequest request = newRequest();
        // what a statement does: compile, then submit
        request.setCompileTimeNanos(1_000_000);
        request.addJob(JOB_1);
        request.setPlan(JOB_1, "plan of statement one");
        request.setCompileTimeNanos(2_000_000);
        request.addJob(JOB_2);
        request.setPlan(JOB_2, "plan of statement two");

        assertEquals(1_000_000, request.getCompileTimeNanos(JOB_1));
        assertEquals(2_000_000, request.getCompileTimeNanos(JOB_2));
        assertEquals("a job of another request has none", 0, request.getCompileTimeNanos(JOB_3));
        // each plan belongs to its statement, so neither of them is the request's
        assertNull("the request has no plan of its own", request.asJson().get("plan"));
        assertEquals("plan of statement one", request.asJson().get("jobs").get(0).get("plan").asText());
        assertEquals("plan of statement two", request.asJson().get("jobs").get(1).get("plan").asText());
    }

    /** The plan of the one job of a single statement is the request's own, as its id is. */
    @AiProvenance(agent = AiProvenance.Agent.CLAUDE_OPUS_5, tool = AiProvenance.Tool.CLAUDE_CODE_UI, contributionKind = AiProvenance.ContributionKind.TEST_GENERATED)
    @Test
    public void thePlanOfASingleStatementIsTheRequestsOwn() {
        ClientRequest request = newRequest();
        request.addJob(JOB_1);
        request.setPlan(JOB_1, "plan of the only statement");

        assertEquals("plan of the only statement", request.asJson().get("plan").asText());
    }

    /**
     * A request of several statements has no plan of its own, whether or not one of them has yet produced a
     * plan: the plan is reported on the statement's job, where it does not appear and then vanish as the
     * next statement compiles its own.
     */
    @AiProvenance(agent = AiProvenance.Agent.CLAUDE_OPUS_5, tool = AiProvenance.Tool.CLAUDE_CODE_UI, contributionKind = AiProvenance.ContributionKind.TEST_GENERATED)
    @Test
    public void aMultiStatementRequestHasNoPlanOfItsOwn() {
        ClientRequest request = newRequest();
        request.setMultiStatement(true);
        request.addJob(JOB_1);
        request.setPlan(JOB_1, "plan of the first statement");

        JsonNode json = request.asJson();
        assertNull("the plan belongs to the statement, not to the request", json.get("plan"));
        assertEquals("plan of the first statement", json.get("jobs").get(0).get("plan").asText());
    }

    /**
     * What identifies a request does not change while a reader watches it: a request of several statements
     * reports no jobId from the moment it is tracked, rather than reporting its first statement's job and
     * dropping it when the second statement submits one.
     */
    @AiProvenance(agent = AiProvenance.Agent.CLAUDE_OPUS_5, tool = AiProvenance.Tool.CLAUDE_CODE_UI, contributionKind = AiProvenance.ContributionKind.TEST_GENERATED)
    @Test
    public void aMultiStatementRequestReportsNoJobIdWhileItsFirstStatementRuns() {
        ClientRequest request = newRequest();
        request.setMultiStatement(true);
        assertTrue("no job has been created yet", request.asJson().get("jobId").isNull());

        request.setStatementPosition(1);
        request.addJob(JOB_1);
        request.jobStarted(JOB_1);
        JsonNode json = request.asJson();
        assertTrue("no one job id is the id of a request of several statements", json.get("jobId").isNull());
        assertEquals("the job the first statement submitted is in the array", JOB_1.toString(),
                json.get("jobs").get(0).get("jobId").asText());

        request.setStatementPosition(2);
        request.addJob(JOB_2);
        assertTrue("and it is still not reported once the second statement runs",
                request.asJson().get("jobId").isNull());
    }

    @Test
    public void onlyJobsWithoutAPendingResultAreSwept() {
        ClientRequest request = newRequest();
        request.addJob(JOB_1);
        request.addJob(JOB_2);
        request.addJob(JOB_3);
        // a handle was handed out for the first and the third only
        request.markResultPending(JOB_1);
        request.markResultPending(JOB_3);

        assertTrue(request.hasPendingResults());
        assertEquals(List.of(JOB_2), request.getJobsWithoutPendingResults());

        request.resultSwept(JOB_1);
        assertTrue("the third statement's result can still be fetched", request.hasPendingResults());

        request.resultSwept(JOB_3);
        assertFalse(request.hasPendingResults());
    }

    @AiProvenance(agent = AiProvenance.Agent.CLAUDE_OPUS_5, tool = AiProvenance.Tool.CLAUDE_CODE_UI, contributionKind = AiProvenance.ContributionKind.REFACTORED)
    @Test
    public void aRequestWithOneJobReportsItFlatAndInTheArray() {
        ClientRequest request = newRequest();
        runJob(request, JOB_1, JobStatus.TERMINATED, null);

        JsonNode json = request.asJson();
        assertEquals("the flat fields are the job's, as they always were", JOB_1.toString(),
                json.get("jobId").asText());
        assertEquals("every request reports its jobs, so a reader has one place", 1, json.get("jobs").size());
        assertEquals(JOB_1.toString(), json.get("jobs").get(0).get("jobId").asText());
    }

    /**
     * A request with several jobs reports them in creation order, each entry carrying the fields the one
     * flat job block carried and no others - the per-job compile time is not reported here, and was not.
     * The flat fields describe the request, so a later statement's failure is the request's failure.
     */
    @AiProvenance(agent = AiProvenance.Agent.CLAUDE_OPUS_5, tool = AiProvenance.Tool.CLAUDE_CODE_UI, contributionKind = AiProvenance.ContributionKind.REFACTORED)
    @Test
    public void severalJobsAreReportedInOrderWithTheFieldsAJobAlwaysHad() {
        ClientRequest request = newRequest();
        runJob(request, JOB_1, JobStatus.TERMINATED, null);
        runJob(request, JOB_2, JobStatus.FAILURE, List.of(new Exception("statement two failed")));
        assertTrue(request.hasJob(JOB_1));
        assertTrue(request.hasJob(JOB_2));
        assertFalse("a job of another request is not this request's", request.hasJob(JOB_3));

        JsonNode json = request.asJson();
        assertEquals(JOB_1.toString(), json.get("jobs").get(0).get("jobId").asText());
        assertEquals(JOB_2.toString(), json.get("jobs").get(1).get("jobId").asText());
        Set<String> jobFields = Set.of("jobId", "jobCreateTime", "jobStartTime", "jobEndTime", "jobQueueTime",
                "jobStatus", "jobRequiredCPUs", "jobRequiredMemory");
        assertEquals(jobFields, fieldsOf(json.get("jobs").get(0)));
        // a failed job adds its error, as the flat block did
        Set<String> failed = new HashSet<>(jobFields);
        failed.add("error");
        assertEquals(failed, fieldsOf(json.get("jobs").get(1)));

        assertTrue("no one job id describes a request of several", json.get("jobId").isNull());
        assertEquals("a request whose statement failed is a failed request", JobStatus.FAILURE.name(),
                json.get("jobStatus").asText());
        assertEquals("statement two failed", json.get("error").asText());
        assertEquals("statement two failed", json.get("jobs").get(1).get("error").asText());
    }

    /**
     * The flat times span the request: the first job's creation and start, and the last job's end, so that a
     * reader is not told the request ended when its first statement did.
     */
    @AiProvenance(agent = AiProvenance.Agent.CLAUDE_OPUS_5, tool = AiProvenance.Tool.CLAUDE_CODE_UI, contributionKind = AiProvenance.ContributionKind.TEST_GENERATED)
    @Test
    public void theFlatTimesSpanEveryJob() throws Exception {
        ClientRequest request = newRequest();
        runJob(request, JOB_1, JobStatus.TERMINATED, null);
        // so that the two jobs cannot fall in the same millisecond, and the times can be told apart
        Thread.sleep(2);
        runJob(request, JOB_2, JobStatus.TERMINATED, null);

        JsonNode json = request.asJson();
        JsonNode firstJob = json.get("jobs").get(0);
        JsonNode lastJob = json.get("jobs").get(1);
        assertEquals(firstJob.get("jobCreateTime").asText(), json.get("jobCreateTime").asText());
        assertEquals(firstJob.get("jobStartTime").asText(), json.get("jobStartTime").asText());
        assertEquals(lastJob.get("jobEndTime").asText(), json.get("jobEndTime").asText());
    }

    /** A request one of whose jobs has not ended has not ended either, whatever the others did. */
    @AiProvenance(agent = AiProvenance.Agent.CLAUDE_OPUS_5, tool = AiProvenance.Tool.CLAUDE_CODE_UI, contributionKind = AiProvenance.ContributionKind.TEST_GENERATED)
    @Test
    public void theRequestDoesNotEndWhileAJobOfItsHasNot() {
        ClientRequest request = newRequest();
        runJob(request, JOB_1, JobStatus.TERMINATED, null);
        request.addJob(JOB_2);
        request.jobCreated(JOB_2, capacity(1, 172032L), IJobCapacityController.JobSubmissionStatus.EXECUTE);
        request.jobStarted(JOB_2);

        JsonNode json = request.asJson();
        assertEquals(JobStatus.RUNNING.name(), json.get("jobStatus").asText());
        assertNull("a job of the request is still to end", json.get("jobEndTime"));
        assertEquals("the job that did end reports its own end", JobStatus.TERMINATED.name(),
                json.get("jobs").get(0).get("jobStatus").asText());
    }

    /** The jobs run one at a time, so what the request required is the peak of them, not their sum. */
    @AiProvenance(agent = AiProvenance.Agent.CLAUDE_OPUS_5, tool = AiProvenance.Tool.CLAUDE_CODE_UI, contributionKind = AiProvenance.ContributionKind.TEST_GENERATED)
    @Test
    public void theFlatRequirementIsThePeakOfTheJobs() {
        ClientRequest request = newRequest();
        runJob(request, JOB_1, capacity(1, 172032L), JobStatus.TERMINATED, null);
        runJob(request, JOB_2, capacity(4, 688128L), JobStatus.TERMINATED, null);

        JsonNode json = request.asJson();
        assertEquals(4, json.get("jobRequiredCPUs").asInt());
        assertEquals(688128L, json.get("jobRequiredMemory").asLong());
    }

    /**
     * A job says which statement submitted it, as the response says of a statement's own report. Statements
     * that submit no job leave a gap, so a job's position in the array does not identify its statement.
     */
    @AiProvenance(agent = AiProvenance.Agent.CLAUDE_OPUS_5, tool = AiProvenance.Tool.CLAUDE_CODE_UI, contributionKind = AiProvenance.ContributionKind.TEST_GENERATED)
    @Test
    public void aJobSaysWhichStatementSubmittedIt() {
        ClientRequest request = newRequest();
        request.setStatementPosition(1);
        request.addJob(JOB_1);
        // the statements in between submitted no job of their own
        request.setStatementPosition(4);
        request.addJob(JOB_2);

        JsonNode jobs = request.asJson().get("jobs");
        assertEquals(1, jobs.get(0).get("statement").asInt());
        assertEquals(4, jobs.get(1).get("statement").asInt());
    }

    /** A job submitted by no statement of the client's - the request's own - says nothing of a statement. */
    @AiProvenance(agent = AiProvenance.Agent.CLAUDE_OPUS_5, tool = AiProvenance.Tool.CLAUDE_CODE_UI, contributionKind = AiProvenance.ContributionKind.TEST_GENERATED)
    @Test
    public void aJobOfNoStatementOfTheClientsSaysNothing() {
        ClientRequest request = newRequest();
        request.addJob(JOB_1);

        assertNull(request.asJson().get("jobs").get(0).get("statement"));
    }

    private static Set<String> fieldsOf(JsonNode node) {
        Set<String> fields = new HashSet<>();
        node.fieldNames().forEachRemaining(fields::add);
        return fields;
    }

    /** Takes a job through the lifecycle the cluster controller notifies, so every timing is set. */
    private static void runJob(ClientRequest request, JobId jobId, JobStatus status, List<Exception> exceptions) {
        runJob(request, jobId, capacity(1, 172032L), status, exceptions);
    }

    @AiProvenance(agent = AiProvenance.Agent.CLAUDE_OPUS_5, tool = AiProvenance.Tool.CLAUDE_CODE_UI, contributionKind = AiProvenance.ContributionKind.REFACTORED)
    private static void runJob(ClientRequest request, JobId jobId, IReadOnlyClusterCapacity capacity, JobStatus status,
            List<Exception> exceptions) {
        request.addJob(jobId);
        request.jobCreated(jobId, capacity, IJobCapacityController.JobSubmissionStatus.EXECUTE);
        request.jobStarted(jobId);
        request.jobFinished(jobId, status, exceptions);
    }

    @AiProvenance(agent = AiProvenance.Agent.CLAUDE_OPUS_5, tool = AiProvenance.Tool.CLAUDE_CODE_UI, contributionKind = AiProvenance.ContributionKind.TEST_GENERATED)
    private static IReadOnlyClusterCapacity capacity(int cores, long memoryByteSize) {
        IReadOnlyClusterCapacity capacity = mock(IReadOnlyClusterCapacity.class);
        when(capacity.getAggregatedCores()).thenReturn(cores);
        when(capacity.getAggregatedMemoryByteSize()).thenReturn(memoryByteSize);
        return capacity;
    }

    private static ClientRequest newRequest() {
        return newRequestOn(Thread.currentThread());
    }

    private static ClientRequest newRequestOn(Thread requestExecutor) {
        IRequestReference requestReference = mock(IRequestReference.class);
        when(requestReference.getUuid()).thenReturn("request-uuid");
        ICommonRequestParameters requestParameters = mock(ICommonRequestParameters.class);
        when(requestParameters.getRequestReference()).thenReturn(requestReference);
        when(requestParameters.getStatement()).thenReturn("select 1; select 2;");
        // ClientRequest takes its executor from the creating thread
        return new ClientRequest(requestParameters) {
            {
                this.executor = requestExecutor;
            }
        };
    }

    private static ICcApplicationContext appCtx(IHyracksClientConnection hcc) throws HyracksDataException {
        ICcApplicationContext appCtx = mock(ICcApplicationContext.class);
        when(appCtx.getHcc()).thenReturn(hcc);
        return appCtx;
    }
}
