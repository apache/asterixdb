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

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.asterix.app.message.AtomicJobCommitMessage;
import org.apache.asterix.app.message.AtomicJobRollbackMessage;
import org.apache.asterix.app.message.EnableMergeMessage;
import org.apache.asterix.common.cluster.IGlobalTxManager;
import org.apache.asterix.common.exceptions.ACIDException;
import org.apache.asterix.common.messaging.api.ICCMessageBroker;
import org.apache.asterix.common.transactions.IGlobalTransactionContext;
import org.apache.asterix.common.utils.StorageConstants;
import org.apache.asterix.transaction.management.service.transaction.GlobalTransactionContext;
import org.apache.asterix.transaction.management.service.transaction.GlobalTxInfo;
import org.apache.hyracks.api.application.ICCServiceContext;
import org.apache.hyracks.api.exceptions.HyracksException;
import org.apache.hyracks.api.io.FileReference;
import org.apache.hyracks.api.job.JobId;
import org.apache.hyracks.api.job.JobSpecification;
import org.apache.hyracks.api.job.JobStatus;
import org.apache.hyracks.control.cc.ClusterControllerService;
import org.apache.hyracks.control.common.controllers.CCConfig;
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
    public static final String GlOBAL_TX_PROPERTY_NAME = "GlobalTxProperty";

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
        if (context.getAcksReceived() != context.getNumPartitions()) {
            synchronized (context) {
                try {
                    context.wait();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new ACIDException(e);
                }
            }
        }
        context.setTxnStatus(TransactionStatus.PREPARED);
        context.persist(ioManager);
        context.resetAcksReceived();
        sendJobCommitMessages(context);

        synchronized (context) {
            try {
                CCConfig config = ((ClusterControllerService) serviceContext.getControllerService()).getCCConfig();
                context.wait(config.getGlobalTxCommitTimeout());
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new ACIDException(e);
            }
        }
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
        if (context.getNodeResourceMap().containsKey(nodeId)) {
            context.getNodeResourceMap().get(nodeId).putAll(componentIdMap);
        } else {
            context.getNodeResourceMap().put(nodeId, componentIdMap);
        }
        if (context.incrementAndGetAcksReceived() == context.getNumPartitions()) {
            synchronized (context) {
                context.notifyAll();
            }
        }
    }

    private void sendJobCommitMessages(IGlobalTransactionContext context) {
        for (String nodeId : context.getNodeResourceMap().keySet()) {
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
        if (context.incrementAndGetAcksReceived() == context.getNumNodes()) {
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
        if (context.incrementAndGetAcksReceived() == context.getNumNodes()) {
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
            IGlobalTransactionContext context = new GlobalTransactionContext(txnLogFileRef, ioManager);
            txnContextRepository.put(context.getJobId(), context);
            sendJobRollbackMessages(context);
        }
    }

    private void sendJobRollbackMessages(IGlobalTransactionContext context) throws Exception {
        JobId jobId = context.getJobId();
        for (String nodeId : context.getNodeResourceMap().keySet()) {
            AtomicJobRollbackMessage rollbackMessage = new AtomicJobRollbackMessage(jobId, context.getDatasetIds(),
                    context.getNodeResourceMap().get(nodeId));
            ((ICCMessageBroker) serviceContext.getMessageBroker()).sendRealTimeApplicationMessageToNC(rollbackMessage,
                    nodeId);
        }
        synchronized (context) {
            try {
                CCConfig config = ((ClusterControllerService) serviceContext.getControllerService()).getCCConfig();
                context.wait(config.getGlobalTxRollbackTimeout());
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                LOGGER.error("Error while rolling back atomic statement for {}, halting JVM", jobId);
                ExitUtil.halt(EC_FAILED_TO_ROLLBACK_ATOMIC_STATEMENT);
            }
        }
        txnContextRepository.remove(jobId);
    }

    @Override
    public void abortTransaction(JobId jobId) throws Exception {
        IGlobalTransactionContext context = getTransactionContext(jobId);
        if (context.getTxnStatus() == TransactionStatus.PREPARED) {
            sendJobRollbackMessages(context);
        }
        txnContextRepository.remove(jobId);
    }

    @Override
    public void notifyJobCreation(JobId jobId, JobSpecification spec) throws HyracksException {
        GlobalTxInfo globalTxInfo = (GlobalTxInfo) spec.getProperty(GlOBAL_TX_PROPERTY_NAME);
        if (globalTxInfo != null) {
            beginTransaction(jobId, globalTxInfo.getNumNodes(), globalTxInfo.getNumPartitions(),
                    globalTxInfo.getDatasetIds());
        }
    }

    @Override
    public void notifyJobStart(JobId jobId) throws HyracksException {

    }

    @Override
    public void notifyJobFinish(JobId jobId, JobStatus jobStatus, List<Exception> exceptions) throws HyracksException {
    }
}
