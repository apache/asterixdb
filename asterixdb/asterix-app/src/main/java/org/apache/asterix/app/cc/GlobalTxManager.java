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

import static org.apache.hyracks.util.ExitUtil.EC_FAILED_TO_ROLLBACK_ATOMIC_STATEMENT;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import org.apache.asterix.app.message.AtomicJobCommitMessage;
import org.apache.asterix.app.message.AtomicJobRollbackMessage;
import org.apache.asterix.app.message.EnableMergeMessage;
import org.apache.asterix.common.cluster.IGlobalTxManager;
import org.apache.asterix.common.exceptions.ACIDException;
import org.apache.asterix.common.messaging.api.ICCMessageBroker;
import org.apache.asterix.common.transactions.IGlobalTransactionContext;
import org.apache.asterix.common.transactions.IGlobalTransactionContext.TxnPhase;
import org.apache.asterix.common.utils.AsterixJobProperty;
import org.apache.asterix.common.utils.StorageConstants;
import org.apache.asterix.transaction.management.service.transaction.GlobalTransactionContext;
import org.apache.asterix.transaction.management.service.transaction.GlobalTxInfo;
import org.apache.hyracks.api.application.ICCServiceContext;
import org.apache.hyracks.api.exceptions.HyracksException;
import org.apache.hyracks.api.io.FileReference;
import org.apache.hyracks.api.job.JobId;
import org.apache.hyracks.api.job.JobSpecification;
import org.apache.hyracks.api.job.JobStatus;
import org.apache.hyracks.api.job.resource.IJobCapacityController;
import org.apache.hyracks.control.cc.ClusterControllerService;
import org.apache.hyracks.control.nc.io.IOManager;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMComponentId;
import org.apache.hyracks.util.ExitUtil;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class GlobalTxManager implements IGlobalTxManager {

    private static final Logger LOGGER = LogManager.getLogger();
    private final Map<JobId, IGlobalTransactionContext> txnContextRepository = new ConcurrentHashMap<>();
    private final ICCServiceContext serviceContext;
    private final IOManager ioManager;

    public GlobalTxManager(ICCServiceContext serviceContext, IOManager ioManager) {
        this.serviceContext = serviceContext;
        this.ioManager = ioManager;
    }

    @Override
    public IGlobalTransactionContext beginTransaction(JobId jobId, int numParticipatingNodes,
            int numParticipatingPartitions, List<Integer> participatingDatasetIds) throws ACIDException {
        GlobalTransactionContext context = new GlobalTransactionContext(jobId, participatingDatasetIds,
                numParticipatingNodes, numParticipatingPartitions);
        txnContextRepository.put(jobId, context);
        return context;
    }

    @Override
    public void commitTransaction(JobId jobId) throws ACIDException {
        IGlobalTransactionContext context = getTransactionContext(jobId);
        try {
            // the predicate must be tested while holding the monitor; testing it outside loses a notifyAll
            // issued between the test and the wait, which hangs the statement for good
            // TODO: bound this wait. Unlike the commit phase below it waits forever, so a node that dies
            // after reporting some of its partitions but not the rest hangs the statement indefinitely.
            // Bounding it needs a decision on which timeout governs the prepare phase; reusing
            // GLOBAL_TXN_COMMIT_TIMEOUT would silently repurpose an option named for the other phase.
            synchronized (context) {
                while (context.getAcksReceived() != context.getNumPartitions()) {
                    context.wait();
                }
            }
            context.setTxnStatus(TransactionStatus.PREPARED);
            context.persist(ioManager);

            // every participating node reports at least one partition, so anything less means a prepared
            // message was dropped and committing now would leave the missing node's partitions behind
            int preparedNodes = context.getNodeResourceMap().size();
            if (preparedNodes < context.getNumNodes()) {
                throw new ACIDException("Prepared resources of " + jobId + " cover " + preparedNodes + " node(s) but "
                        + context.getNumNodes() + " node(s) participated in the job");
            }

            sendJobCommitMessages(context);

            long timeout = ((ClusterControllerService) serviceContext.getControllerService()).getCCConfig()
                    .getGlobalTxCommitTimeout();
            awaitStatus(context, TransactionStatus.COMMITTED, timeout);
            if (context.getTxnStatus() != TransactionStatus.COMMITTED) {
                throw new ACIDException("Timed out after " + timeout + "ms waiting for " + context.getExpectedAcks()
                        + " node(s) to commit " + jobId + "; " + context.getAcksReceived()
                        + " acknowledged. The transaction log is retained for rollback");
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new ACIDException(e);
        }
        // deliberately not in a finally: on failure the context is handed to the caller, which aborts the
        // transaction, and abortTransaction has to be able to look it up to roll the prepared nodes back.
        // Removing it here would turn every failure into "Transaction for jobId ... does not exist" and skip
        // the rollback, stranding the flushed components on the nodes.
        txnContextRepository.remove(jobId);
    }

    @Override
    public IGlobalTransactionContext getTransactionContext(JobId jobId) throws ACIDException {
        IGlobalTransactionContext context = txnContextRepository.get(jobId);
        if (context == null) {
            throw new ACIDException("Transaction for jobId " + jobId + " does not exist");
        }
        return context;
    }

    @Override
    public void handleJobPreparedMessage(JobId jobId, String nodeId, Map<String, ILSMComponentId> componentIdMap) {
        IGlobalTransactionContext context = txnContextRepository.get(jobId);
        if (context == null) {
            LOGGER.warn("JobPreparedMessage received for jobId " + jobId
                    + ", which does not exist. The transaction for the job is already aborted");
            return;
        }
        if (context.getPhase() != TxnPhase.PREPARE) {
            // the prepare phase ends only once every participating partition has reported, so anything
            // arriving later is a duplicate. Accepting it would add to the map the next phase has already
            // snapshotted and inflate the acknowledgement counter, acking that phase a node early.
            LOGGER.warn("ignoring late JobPreparedMessage from {} for {}, which is in phase {}", nodeId, jobId,
                    context.getPhase());
            return;
        }
        context.addPreparedNodeResources(nodeId, componentIdMap);
        if (context.incrementAndGetAcksReceived() == context.getNumPartitions()) {
            synchronized (context) {
                context.notifyAll();
            }
        }
    }

    private void sendJobCommitMessages(IGlobalTransactionContext context) {
        // snapshot: the nodes messaged and the ack target taken from them must be one set, not two reads
        List<String> nodeIds = new ArrayList<>(context.getNodeResourceMap().keySet());
        context.beginPhase(TxnPhase.COMMIT, nodeIds.size());
        for (String nodeId : nodeIds) {
            AtomicJobCommitMessage message = new AtomicJobCommitMessage(context.getJobId(), context.getDatasetIds());
            try {
                ((ICCMessageBroker) serviceContext.getMessageBroker()).sendRealTimeApplicationMessageToNC(message,
                        nodeId);
            } catch (Exception e) {
                throw new ACIDException(e);
            }
        }
    }

    @Override
    public void handleJobCompletionMessage(JobId jobId, String nodeId) {
        IGlobalTransactionContext context = getTransactionContext(jobId);
        if (rejectStrayAck(context, TxnPhase.COMMIT, jobId, nodeId, "JobCompletionMessage")) {
            return;
        }
        if (context.incrementAndGetAcksReceived() == context.getExpectedAcks()) {
            context.delete(ioManager);
            context.setTxnStatus(TransactionStatus.COMMITTED);
            synchronized (context) {
                context.notifyAll();
            }
            sendEnableMergeMessages(context);
        }
    }

    @Override
    public void handleJobRollbackCompletionMessage(JobId jobId, String nodeId) {
        IGlobalTransactionContext context = getTransactionContext(jobId);
        if (rejectStrayAck(context, TxnPhase.ROLLBACK, jobId, nodeId, "JobRollbackCompletionMessage")) {
            return;
        }
        if (context.incrementAndGetAcksReceived() == context.getExpectedAcks()) {
            context.setTxnStatus(TransactionStatus.ROLLBACK);
            context.delete(ioManager);
            synchronized (context) {
                context.notifyAll();
            }
        }
    }

    private void sendEnableMergeMessages(IGlobalTransactionContext context) {
        for (String nodeId : context.getNodeResourceMap().keySet()) {
            for (Integer datasetId : context.getDatasetIds()) {
                EnableMergeMessage message = new EnableMergeMessage(context.getJobId(), datasetId);
                try {
                    ((ICCMessageBroker) serviceContext.getMessageBroker()).sendRealTimeApplicationMessageToNC(message,
                            nodeId);
                } catch (Exception e) {
                    throw new ACIDException(e);
                }
            }
        }
    }

    @Override
    public void rollback() throws Exception {
        Set<FileReference> txnLogFileRefs = ioManager.list(ioManager.resolve(StorageConstants.GLOBAL_TXN_DIR_NAME));
        for (FileReference txnLogFileRef : txnLogFileRefs) {
            IGlobalTransactionContext context = null;
            try {
                context = new GlobalTransactionContext(txnLogFileRef, ioManager);
                txnContextRepository.put(context.getJobId(), context);
                sendJobRollbackMessages(context);
            } catch (Exception e) {
                LOGGER.error("Error rolling back transaction for {}", txnLogFileRef, e);
                cleanup(txnLogFileRef);
            } finally {
                if (context != null) {
                    txnContextRepository.remove(context.getJobId());
                }
            }
        }
    }

    private void cleanup(FileReference resourceFile) {
        if (resourceFile.getFile().exists()) {
            try {
                ioManager.delete(resourceFile);
            } catch (Throwable th) {
                LOGGER.error("Error cleaning up corrupted resource {}", resourceFile, th);
                ExitUtil.halt(ExitUtil.EC_FAILED_TO_DELETE_CORRUPTED_RESOURCES);
            }
        }
    }

    private void sendJobRollbackMessages(IGlobalTransactionContext context) throws Exception {
        JobId jobId = context.getJobId();
        // snapshot: the nodes messaged and the ack target taken from them must be one set, not two reads
        List<String> nodeIds = new ArrayList<>(context.getNodeResourceMap().keySet());
        context.beginPhase(TxnPhase.ROLLBACK, nodeIds.size());
        for (String nodeId : nodeIds) {
            AtomicJobRollbackMessage rollbackMessage = new AtomicJobRollbackMessage(jobId, context.getDatasetIds(),
                    context.getNodeResourceMap().get(nodeId));
            ((ICCMessageBroker) serviceContext.getMessageBroker()).sendRealTimeApplicationMessageToNC(rollbackMessage,
                    nodeId);
        }
        long timeout = ((ClusterControllerService) serviceContext.getControllerService()).getCCConfig()
                .getGlobalTxRollbackTimeout();
        try {
            awaitStatus(context, TransactionStatus.ROLLBACK, timeout);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            LOGGER.error("Error while rolling back atomic statement for {}, halting JVM", jobId);
            ExitUtil.halt(EC_FAILED_TO_ROLLBACK_ATOMIC_STATEMENT);
        }
        if (context.getTxnStatus() != TransactionStatus.ROLLBACK) {
            // deliberately reported rather than rethrown: the caller in rollback() treats a failure here as a
            // corrupted log and deletes it, which would discard the only record of what is left to undo
            LOGGER.error(
                    "Timed out after {}ms waiting for {} node(s) to roll back {}; {} acknowledged. The "
                            + "transaction log is retained for recovery",
                    timeout, context.getExpectedAcks(), jobId, context.getAcksReceived());
        }
    }

    @Override
    public void abortTransaction(JobId jobId) throws Exception {
        try {
            IGlobalTransactionContext context = getTransactionContext(jobId);
            // the status cannot be the test here. The nodes flush their components and report them before
            // commitTransaction moves the transaction to PREPARED, so a statement that fails in between - a
            // cancellation between waitForCompletion and the commit, say - leaves a fully prepared
            // transaction still reading ACTIVE, and a partially prepared one never reaches PREPARED at all.
            // Either way the nodes are holding components that only a rollback message will discard, and the
            // recorded resources are what say so. COMMITTED and ROLLBACK cannot be seen here: both paths
            // remove the context, so the lookup above would have thrown first.
            if (!context.getNodeResourceMap().isEmpty()) {
                sendJobRollbackMessages(context);
            }
        } finally {
            // nothing else ever removes it: the repository is filled from the job lifecycle but drained only
            // from the statement path, so a throw on the way out would strand the entry for the CC's lifetime
            txnContextRepository.remove(jobId);
        }
    }

    @Override
    public void notifyJobCreation(JobId jobId, JobSpecification spec, IJobCapacityController.JobSubmissionStatus status)
            throws HyracksException {
        GlobalTxInfo globalTxInfo = (GlobalTxInfo) spec.getProperty(AsterixJobProperty.GLOBAL_TX);
        if (globalTxInfo != null) {
            beginTransaction(jobId, globalTxInfo.getNumNodes(), globalTxInfo.getNumPartitions(),
                    globalTxInfo.getDatasetIds());
        }
    }

    @Override
    public void notifyJobSubmissionFailed(JobId jobId, JobSpecification spec) {
        txnContextRepository.remove(jobId);
    }

    @Override
    public void notifyJobStart(JobId jobId, JobSpecification spec) throws HyracksException {
    }

    @Override
    public void notifyJobFinish(JobId jobId, JobSpecification spec, JobStatus jobStatus, List<Exception> exceptions)
            throws HyracksException {

    }

    /**
     * An acknowledgement that belongs to a phase the transaction has already left must be dropped, not
     * counted: the counter is shared across phases, so a straggler would push the current phase over its
     * target early and declare it complete while a node has yet to answer.
     */
    private static boolean rejectStrayAck(IGlobalTransactionContext context, TxnPhase expected, JobId jobId,
            String nodeId, String messageKind) {
        if (context.getPhase() != expected) {
            LOGGER.warn("ignoring {} from {} for {}: expected phase {} but the transaction is in phase {}", messageKind,
                    nodeId, jobId, expected, context.getPhase());
            return true;
        }
        return false;
    }

    /**
     * Waits until the acknowledgements of every messaged node have moved the transaction to {@code target}, or
     * the timeout expires. The caller must re-read the status to tell those two outcomes apart.
     */
    private static void awaitStatus(IGlobalTransactionContext context, TransactionStatus target, long timeoutMillis)
            throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(timeoutMillis);
        synchronized (context) {
            long remaining = timeoutMillis;
            while (context.getTxnStatus() != target && remaining > 0) {
                context.wait(remaining);
                remaining = TimeUnit.NANOSECONDS.toMillis(deadline - System.nanoTime());
            }
        }
    }
}
