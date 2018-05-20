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
package org.apache.asterix.app.nc;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;

import org.apache.asterix.common.api.INcApplicationContext;
import org.apache.asterix.common.config.ReplicationProperties;
import org.apache.asterix.common.config.TransactionProperties;
import org.apache.asterix.common.transactions.Checkpoint;
import org.apache.asterix.common.transactions.CheckpointProperties;
import org.apache.asterix.common.transactions.ICheckpointManager;
import org.apache.asterix.common.transactions.ILockManager;
import org.apache.asterix.common.transactions.ILogManager;
import org.apache.asterix.common.transactions.IRecoveryManager;
import org.apache.asterix.common.transactions.IRecoveryManagerFactory;
import org.apache.asterix.common.transactions.ITransactionManager;
import org.apache.asterix.common.transactions.ITransactionSubsystem;
import org.apache.asterix.transaction.management.service.locking.ConcurrentLockManager;
import org.apache.asterix.transaction.management.service.logging.LogManager;
import org.apache.asterix.transaction.management.service.logging.LogManagerWithReplication;
import org.apache.asterix.transaction.management.service.recovery.CheckpointManagerFactory;
import org.apache.asterix.transaction.management.service.transaction.TransactionManager;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.Logger;

/**
 * Provider for all the sub-systems (transaction/lock/log/recovery) managers.
 * Users of transaction sub-systems must obtain them from the provider.
 */
public class TransactionSubsystem implements ITransactionSubsystem {
    private static final Logger LOGGER = org.apache.logging.log4j.LogManager.getLogger();
    private final String id;
    private final ILogManager logManager;
    private final ILockManager lockManager;
    private final ITransactionManager transactionManager;
    private final IRecoveryManager recoveryManager;
    private final TransactionProperties txnProperties;
    private final ICheckpointManager checkpointManager;
    private final INcApplicationContext appCtx;

    //for profiling purpose
    private long profilerEntityCommitLogCount = 0;
    private EntityCommitProfiler ecp;

    public TransactionSubsystem(INcApplicationContext appCtx, IRecoveryManagerFactory recoveryManagerFactory) {
        this.appCtx = appCtx;
        this.id = appCtx.getServiceContext().getNodeId();
        this.txnProperties = appCtx.getTransactionProperties();
        this.transactionManager = new TransactionManager(this);
        this.lockManager = new ConcurrentLockManager(txnProperties.getLockManagerShrinkTimer());
        final ReplicationProperties repProperties = appCtx.getReplicationProperties();
        final boolean replicationEnabled = repProperties.isReplicationEnabled();
        final CheckpointProperties checkpointProperties = new CheckpointProperties(txnProperties, id);
        if (LOGGER.isInfoEnabled()) {
            LOGGER.log(Level.INFO, "Checkpoint Properties: " + checkpointProperties);
        }
        checkpointManager = CheckpointManagerFactory.create(this, checkpointProperties);
        final Checkpoint latestCheckpoint = checkpointManager.getLatest();
        if (latestCheckpoint != null) {
            transactionManager.ensureMaxTxnId(latestCheckpoint.getMaxTxnId());
        }

        this.logManager = replicationEnabled ? new LogManagerWithReplication(this) : new LogManager(this);
        this.recoveryManager = recoveryManagerFactory.createRecoveryManager(appCtx.getServiceContext(), this);
        if (txnProperties.isCommitProfilerEnabled()) {
            ecp = new EntityCommitProfiler(this, this.txnProperties.getCommitProfilerReportInterval());
            ((ExecutorService) appCtx.getThreadExecutor()).submit(ecp);
        }
    }

    @Override
    public ILogManager getLogManager() {
        return logManager;
    }

    @Override
    public ILockManager getLockManager() {
        return lockManager;
    }

    @Override
    public ITransactionManager getTransactionManager() {
        return transactionManager;
    }

    @Override
    public IRecoveryManager getRecoveryManager() {
        return recoveryManager;
    }

    @Override
    public INcApplicationContext getApplicationContext() {
        return appCtx;
    }

    @Override
    public TransactionProperties getTransactionProperties() {
        return txnProperties;
    }

    @Override
    public String getId() {
        return id;
    }

    @Override
    public void incrementEntityCommitCount() {
        ++profilerEntityCommitLogCount;
    }

    @Override
    public ICheckpointManager getCheckpointManager() {
        return checkpointManager;
    }

    /**
     * Thread for profiling entity level commit count
     * This thread takes a report interval (in seconds) parameter and
     * reports entity level commit count every report interval (in seconds)
     * only if IS_PROFILE_MODE is set to true.
     * However, the thread doesn't start reporting the count until the entityCommitCount > 0.
     */
    static class EntityCommitProfiler implements Callable<Boolean> {
        private static final Logger LOGGER = org.apache.logging.log4j.LogManager.getLogger();
        private final long reportIntervalInMillisec;
        private long lastEntityCommitCount;
        private int reportIntervalInSeconds;
        private TransactionSubsystem txnSubsystem;
        private boolean firstReport = true;
        private long startTimeStamp = 0;
        private long reportRound = 1;

        public EntityCommitProfiler(TransactionSubsystem txnSubsystem, int reportIntervalInSeconds) {
            Thread.currentThread().setName("EntityCommitProfiler-Thread");
            this.txnSubsystem = txnSubsystem;
            this.reportIntervalInSeconds = reportIntervalInSeconds;
            this.reportIntervalInMillisec = reportIntervalInSeconds * 1000L;
            lastEntityCommitCount = txnSubsystem.profilerEntityCommitLogCount;
        }

        @Override
        public Boolean call() throws Exception {
            while (true) {
                Thread.sleep(reportIntervalInMillisec);
                if (txnSubsystem.profilerEntityCommitLogCount > 0) {
                    if (firstReport) {
                        startTimeStamp = System.currentTimeMillis();
                        firstReport = false;
                    }
                    outputCount();
                }
            }
        }

        private void outputCount() {
            long currentTimeStamp = System.currentTimeMillis();
            long currentEntityCommitCount = txnSubsystem.profilerEntityCommitLogCount;

            LOGGER.error("EntityCommitProfiler ReportRound[" + reportRound + "], AbsoluteTimeStamp[" + currentTimeStamp
                    + "], ActualRelativeTimeStamp[" + (currentTimeStamp - startTimeStamp)
                    + "], ExpectedRelativeTimeStamp[" + (reportIntervalInSeconds * reportRound) + "], IIPS["
                    + ((currentEntityCommitCount - lastEntityCommitCount) / reportIntervalInSeconds) + "], IPS["
                    + (currentEntityCommitCount / (reportRound * reportIntervalInSeconds)) + "]");

            lastEntityCommitCount = currentEntityCommitCount;
            ++reportRound;
        }
    }

}
