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
package org.apache.asterix.app.cc;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.asterix.app.message.AtomicJobCommitMessage;
import org.apache.asterix.app.message.AtomicJobRollbackMessage;
import org.apache.asterix.common.cluster.IGlobalTxManager.TransactionStatus;
import org.apache.asterix.common.exceptions.ACIDException;
import org.apache.asterix.common.messaging.api.ICCMessageBroker;
import org.apache.asterix.common.messaging.api.INcAddressedMessage;
import org.apache.asterix.common.transactions.IGlobalTransactionContext;
import org.apache.hyracks.api.application.ICCServiceContext;
import org.apache.hyracks.api.job.JobId;
import org.apache.hyracks.api.job.JobSpecification;
import org.apache.hyracks.control.cc.ClusterControllerService;
import org.apache.hyracks.control.common.controllers.CCConfig;
import org.apache.hyracks.control.nc.io.IOManager;
import org.apache.hyracks.storage.am.lsm.common.impls.LSMComponentId;
import org.apache.hyracks.util.annotations.AiProvenance;
import org.junit.Before;
import org.junit.Test;

/**
 * Covers the two ways the rollback phase can end: every node acknowledges, or the wait expires. Both used to
 * be indistinguishable, because the wait was a bare timed {@code wait()} with no predicate and no check of
 * what it woke up to.
 */
@AiProvenance(agent = AiProvenance.Agent.CLAUDE_OPUS_5, tool = AiProvenance.Tool.CLAUDE_CODE_UI, contributionKind = AiProvenance.ContributionKind.TEST_GENERATED, notes = "Covers rollback completion and rollback timeout of the atomic statement protocol")
public class GlobalTxManagerTest {

    private static final long ROLLBACK_TIMEOUT_MILLIS = 500;
    private static final long COMMIT_TIMEOUT_MILLIS = 500;
    private static final String NODE_ID = "node_0";
    private static final int DATASET_ID = 101;

    private ICCServiceContext serviceContext;
    private ICCMessageBroker messageBroker;
    private IOManager ioManager;
    private GlobalTxManager globalTxManager;

    @Before
    public void setUp() {
        CCConfig ccConfig = mock(CCConfig.class);
        when(ccConfig.getGlobalTxRollbackTimeout()).thenReturn(ROLLBACK_TIMEOUT_MILLIS);
        when(ccConfig.getGlobalTxCommitTimeout()).thenReturn(COMMIT_TIMEOUT_MILLIS);
        ClusterControllerService controllerService = mock(ClusterControllerService.class);
        when(controllerService.getCCConfig()).thenReturn(ccConfig);
        messageBroker = mock(ICCMessageBroker.class);
        serviceContext = mock(ICCServiceContext.class);
        when(serviceContext.getControllerService()).thenReturn(controllerService);
        when(serviceContext.getMessageBroker()).thenReturn(messageBroker);
        ioManager = mock(IOManager.class);
        globalTxManager = new GlobalTxManager(serviceContext, ioManager);
    }

    /**
     * The acknowledgement can land while the rollback messages are still being sent, i.e. before the sender
     * reaches its wait. Testing the predicate only after waiting misses that notify and blocks for the whole
     * timeout, so this asserts the abort returns well inside it.
     */
    @Test
    public void rollbackAcknowledgedBeforeWaitDoesNotBlock() throws Exception {
        JobId jobId = new JobId(1);
        prepareTransaction(jobId);
        when(messageBroker.sendRealTimeApplicationMessageToNC(any(INcAddressedMessage.class), anyString()))
                .thenAnswer(invocation -> {
                    globalTxManager.handleJobRollbackCompletionMessage(jobId, NODE_ID);
                    return true;
                });

        long elapsed = timeAbort(jobId);

        assertTrue("the abort waited for the timeout despite being acknowledged, took " + elapsed + "ms",
                elapsed < ROLLBACK_TIMEOUT_MILLIS);
        verify(ioManager).delete(any());
    }

    /**
     * When no node acknowledges, the rollback must not be mistaken for a completed one: the transaction log is
     * the only record of what is still to be undone, so it has to survive for startup recovery.
     * <p>
     * This pins the invariants rather than reproducing a past defect: the timed-out rollback was always
     * observably like this, the difference is that it is now reported instead of passing unnoticed, and the
     * report is a log line no assertion here can see. What it does catch is a future change that deletes the
     * log on timeout, flips the status as though the rollback had finished, or returns before it expires.
     */
    @Test
    public void rollbackTimeoutRetainsTransactionLog() throws Exception {
        JobId jobId = new JobId(2);
        IGlobalTransactionContext context = prepareTransaction(jobId);
        when(messageBroker.sendRealTimeApplicationMessageToNC(any(INcAddressedMessage.class), anyString()))
                .thenReturn(true);

        long elapsed = timeAbort(jobId);

        assertTrue("the abort returned before the rollback timeout expired, took " + elapsed + "ms",
                elapsed >= ROLLBACK_TIMEOUT_MILLIS);
        assertNotEquals("a timed-out rollback must not be reported as rolled back", TransactionStatus.ROLLBACK,
                context.getTxnStatus());
        verify(ioManager, never()).delete(any());
    }

    /**
     * A duplicate prepared message can still arrive once the prepare phase has closed. The acknowledgement
     * counter is shared by both phases, so accepting one inflates the commit count and makes the next genuine
     * acknowledgement look like the last: the transaction would be reported committed, and its log deleted,
     * while a node that never acknowledged is still holding uncommitted components.
     */
    @Test
    public void latePreparedMessageDoesNotCommitEarly() throws Exception {
        JobId jobId = new JobId(3);
        String secondNode = "node_1";
        IGlobalTransactionContext context =
                globalTxManager.beginTransaction(jobId, 2, 2, Collections.singletonList(DATASET_ID));
        globalTxManager.handleJobPreparedMessage(jobId, NODE_ID,
                Collections.singletonMap("resource_0", new LSMComponentId(1, 1)));
        globalTxManager.handleJobPreparedMessage(jobId, secondNode,
                Collections.singletonMap("resource_1", new LSMComponentId(1, 1)));

        // while the commit messages go out, NODE_ID repeats its prepared message and then acknowledges the
        // commit. node_1 never acknowledges, so one of the two commits is still outstanding throughout.
        AtomicInteger sends = new AtomicInteger();
        when(messageBroker.sendRealTimeApplicationMessageToNC(any(INcAddressedMessage.class), anyString()))
                .thenAnswer(invocation -> {
                    if (sends.getAndIncrement() == 0) {
                        globalTxManager.handleJobPreparedMessage(jobId, NODE_ID,
                                Collections.singletonMap("resource_0", new LSMComponentId(1, 1)));
                        globalTxManager.handleJobCompletionMessage(jobId, NODE_ID);
                    }
                    return true;
                });

        try {
            globalTxManager.commitTransaction(jobId);
            fail("the commit was reported as successful while one node had not acknowledged it");
        } catch (ACIDException e) {
            assertTrue("expected a commit timeout, got: " + e.getMessage(),
                    e.getMessage() != null && e.getMessage().contains("Timed out"));
        }
        assertNotEquals("an incomplete commit must not be reported as committed", TransactionStatus.COMMITTED,
                context.getTxnStatus());
        verify(ioManager, never()).delete(any());
    }

    /**
     * A commit that times out is followed by a rollback, and the slow node's commit acknowledgement can still
     * turn up while the rollback is in flight. The two phases share one counter, so counting that straggler
     * would let a single rollback acknowledgement satisfy a two-node target: the rollback would be declared
     * complete and its log deleted while the other node had not rolled back at all.
     */
    @Test
    public void lateCommitAckDoesNotCompleteRollbackEarly() throws Exception {
        JobId jobId = new JobId(4);
        String secondNode = "node_1";
        IGlobalTransactionContext context =
                globalTxManager.beginTransaction(jobId, 2, 2, Collections.singletonList(DATASET_ID));
        globalTxManager.handleJobPreparedMessage(jobId, NODE_ID,
                Collections.singletonMap("resource_0", new LSMComponentId(1, 1)));
        globalTxManager.handleJobPreparedMessage(jobId, secondNode,
                Collections.singletonMap("resource_1", new LSMComponentId(1, 1)));

        AtomicInteger commitSends = new AtomicInteger();
        AtomicInteger rollbackSends = new AtomicInteger();
        when(messageBroker.sendRealTimeApplicationMessageToNC(any(INcAddressedMessage.class), anyString()))
                .thenAnswer(invocation -> {
                    Object message = invocation.getArgument(0);
                    if (message instanceof AtomicJobCommitMessage && commitSends.getAndIncrement() == 0) {
                        // only NODE_ID acknowledges the commit, so it has to time out
                        globalTxManager.handleJobCompletionMessage(jobId, NODE_ID);
                    } else if (message instanceof AtomicJobRollbackMessage && rollbackSends.getAndIncrement() == 0) {
                        // node_1's commit acknowledgement finally arrives, mid-rollback, and NODE_ID rolls
                        // back. node_1 never acknowledges the rollback.
                        globalTxManager.handleJobCompletionMessage(jobId, secondNode);
                        globalTxManager.handleJobRollbackCompletionMessage(jobId, NODE_ID);
                    }
                    return true;
                });

        try {
            globalTxManager.commitTransaction(jobId);
            fail("the commit was reported as successful while one node had not acknowledged it");
        } catch (ACIDException e) {
            assertTrue("expected a commit timeout, got: " + e.getMessage(),
                    e.getMessage() != null && e.getMessage().contains("Timed out"));
        }
        globalTxManager.abortTransaction(jobId);

        assertNotEquals("an incomplete rollback must not be reported as rolled back", TransactionStatus.ROLLBACK,
                context.getTxnStatus());
        verify(ioManager, never()).delete(any());
    }

    /**
     * The repository is filled from the job lifecycle but drained only from the statement path, so an abort
     * that throws on its way out is the last chance to drop the entry. Leaking it strands the context, and
     * the resource map it holds, for the lifetime of the cluster controller.
     */
    @Test
    public void failedRollbackSendStillDropsTheContext() throws Exception {
        JobId jobId = new JobId(5);
        prepareTransaction(jobId);
        when(messageBroker.sendRealTimeApplicationMessageToNC(any(INcAddressedMessage.class), anyString()))
                .thenThrow(new IllegalStateException("broker is down"));

        try {
            globalTxManager.abortTransaction(jobId);
            fail("the rollback send failed, so the abort should not have reported success");
        } catch (RuntimeException expected) {
            // the send failure propagates to the caller, which is what marks the abort as unfinished
        }
        assertEquals("the context outlived a failed abort", 0, countTracked(jobId));
    }

    /**
     * The nodes flush and report their components before {@code commitTransaction} moves the transaction to
     * PREPARED, so a statement cancelled in between - after {@code waitForCompletion} succeeds and before the
     * commit - aborts a transaction that is fully prepared but still reads ACTIVE. It has to be rolled back
     * anyway: nothing else discards those components. The nodes only self-abort when their own task fails,
     * and no transaction log has been persisted yet for recovery to act on.
     */
    @Test
    public void abortRollsBackNodesThatPreparedBeforeTheCommitBegan() throws Exception {
        JobId jobId = new JobId(6);
        String secondNode = "node_1";
        IGlobalTransactionContext context =
                globalTxManager.beginTransaction(jobId, 2, 2, Collections.singletonList(DATASET_ID));
        globalTxManager.handleJobPreparedMessage(jobId, NODE_ID,
                Collections.singletonMap("resource_0", new LSMComponentId(1, 1)));
        globalTxManager.handleJobPreparedMessage(jobId, secondNode,
                Collections.singletonMap("resource_1", new LSMComponentId(1, 1)));
        assertEquals("the commit has not run, so the transaction is still active", TransactionStatus.ACTIVE,
                context.getTxnStatus());

        List<String> rolledBack = new ArrayList<>();
        when(messageBroker.sendRealTimeApplicationMessageToNC(any(INcAddressedMessage.class), anyString()))
                .thenAnswer(invocation -> {
                    assertTrue("expected a rollback message, got " + invocation.getArgument(0),
                            invocation.getArgument(0) instanceof AtomicJobRollbackMessage);
                    rolledBack.add(invocation.getArgument(1));
                    globalTxManager.handleJobRollbackCompletionMessage(jobId, invocation.getArgument(1));
                    return true;
                });

        globalTxManager.abortTransaction(jobId);

        assertEquals("both prepared nodes must be told to discard their components", 2, rolledBack.size());
        assertTrue("node_0 was not rolled back", rolledBack.contains(NODE_ID));
        assertTrue("node_1 was not rolled back", rolledBack.contains(secondNode));
        assertEquals(TransactionStatus.ROLLBACK, context.getTxnStatus());
    }

    /**
     * A partially prepared transaction never reaches PREPARED at all, and the partitions that did report are
     * still holding components. The rollback goes to exactly those, and to no one else.
     */
    @Test
    public void abortRollsBackOnlyThePartitionsThatPrepared() throws Exception {
        JobId jobId = new JobId(7);
        globalTxManager.beginTransaction(jobId, 2, 2, Collections.singletonList(DATASET_ID));
        globalTxManager.handleJobPreparedMessage(jobId, NODE_ID,
                Collections.singletonMap("resource_0", new LSMComponentId(1, 1)));

        List<String> rolledBack = new ArrayList<>();
        when(messageBroker.sendRealTimeApplicationMessageToNC(any(INcAddressedMessage.class), anyString()))
                .thenAnswer(invocation -> {
                    rolledBack.add(invocation.getArgument(1));
                    globalTxManager.handleJobRollbackCompletionMessage(jobId, invocation.getArgument(1));
                    return true;
                });

        globalTxManager.abortTransaction(jobId);

        assertEquals("only the partition that reported should be rolled back", Collections.singletonList(NODE_ID),
                rolledBack);
    }

    /**
     * JobManager.add registers the transaction and only then queues or executes the job. When that throws, the
     * job id never reaches the client, so the statement path has nothing to abort with and the context - and
     * the resource map it holds - is retained for the lifetime of the cluster controller: the repository is an
     * unbounded map filled from the job lifecycle and drained only from the statement path.
     */
    @Test
    @AiProvenance(agent = AiProvenance.Agent.CLAUDE_OPUS_5, tool = AiProvenance.Tool.CLAUDE_CODE_UI, contributionKind = AiProvenance.ContributionKind.TEST_GENERATED, notes = "Covers deregistration of a transaction whose job was never submitted")
    public void failedSubmissionDeregistersTheTransaction() {
        JobId jobId = new JobId(8);
        globalTxManager.beginTransaction(jobId, 1, 1, Collections.singletonList(DATASET_ID));
        assertEquals("the transaction should be registered before the submission fails", 1, countTracked(jobId));

        globalTxManager.notifyJobSubmissionFailed(jobId, mock(JobSpecification.class));

        assertEquals("the transaction outlived the failed submission", 0, countTracked(jobId));
    }

    /**
     * Every job's failed submission reaches every listener, so a job with no global transaction - which is
     * most of them - has to pass through silently rather than fail looking up a context it never had.
     */
    @Test
    @AiProvenance(agent = AiProvenance.Agent.CLAUDE_OPUS_5, tool = AiProvenance.Tool.CLAUDE_CODE_UI, contributionKind = AiProvenance.ContributionKind.TEST_GENERATED, notes = "Pins that a non-atomic job's failed submission is ignored")
    public void failedSubmissionOfNonAtomicJobIsIgnored() throws Exception {
        globalTxManager.notifyJobSubmissionFailed(new JobId(9), mock(JobSpecification.class));

        verify(messageBroker, never()).sendRealTimeApplicationMessageToNC(any(INcAddressedMessage.class), anyString());
    }

    private IGlobalTransactionContext prepareTransaction(JobId jobId) {
        IGlobalTransactionContext context =
                globalTxManager.beginTransaction(jobId, 1, 1, Collections.singletonList(DATASET_ID));
        context.addPreparedNodeResources(NODE_ID, Collections.singletonMap("resource_0", new LSMComponentId(1, 1)));
        context.setTxnStatus(TransactionStatus.PREPARED);
        return context;
    }

    private long timeAbort(JobId jobId) throws Exception {
        long start = System.nanoTime();
        globalTxManager.abortTransaction(jobId);
        long elapsed = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);
        assertEquals("the transaction context outlived the abort", 0, countTracked(jobId));
        return elapsed;
    }

    private int countTracked(JobId jobId) {
        try {
            globalTxManager.getTransactionContext(jobId);
            return 1;
        } catch (Exception e) {
            return 0;
        }
    }
}
