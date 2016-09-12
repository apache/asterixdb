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
package org.apache.asterix.transaction.management.service.transaction;

import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import java.util.logging.Logger;

import org.apache.asterix.common.config.AsterixReplicationProperties;
import org.apache.asterix.common.config.AsterixTransactionProperties;
import org.apache.asterix.common.config.IAsterixPropertiesProvider;
import org.apache.asterix.common.exceptions.ACIDException;
import org.apache.asterix.common.transactions.IAsterixAppRuntimeContextProvider;
import org.apache.asterix.common.transactions.ILockManager;
import org.apache.asterix.common.transactions.ILogManager;
import org.apache.asterix.common.transactions.IRecoveryManager;
import org.apache.asterix.common.transactions.ITransactionManager;
import org.apache.asterix.common.transactions.ITransactionSubsystem;
import org.apache.asterix.transaction.management.service.locking.ConcurrentLockManager;
import org.apache.asterix.transaction.management.service.logging.LogManager;
import org.apache.asterix.transaction.management.service.logging.LogManagerWithReplication;
import org.apache.asterix.transaction.management.service.recovery.CheckpointThread;
import org.apache.asterix.transaction.management.service.recovery.RecoveryManager;

/**
 * Provider for all the sub-systems (transaction/lock/log/recovery) managers.
 * Users of transaction sub-systems must obtain them from the provider.
 */
public class TransactionSubsystem implements ITransactionSubsystem {
    private final String id;
    private final ILogManager logManager;
    private final ILockManager lockManager;
    private final ITransactionManager transactionManager;
    private final IRecoveryManager recoveryManager;
    private final IAsterixAppRuntimeContextProvider asterixAppRuntimeContextProvider;
    private final CheckpointThread checkpointThread;
    private final AsterixTransactionProperties txnProperties;

    //for profiling purpose
    public static final boolean IS_PROFILE_MODE = false;//true
    public long profilerEntityCommitLogCount = 0;
    private EntityCommitProfiler ecp;
    private Future<Object> fecp;

    public TransactionSubsystem(String id, IAsterixAppRuntimeContextProvider asterixAppRuntimeContextProvider,
            AsterixTransactionProperties txnProperties) throws ACIDException {
        this.asterixAppRuntimeContextProvider = asterixAppRuntimeContextProvider;
        this.id = id;
        this.txnProperties = txnProperties;
        this.transactionManager = new TransactionManager(this);
        this.lockManager = new ConcurrentLockManager(txnProperties.getLockManagerShrinkTimer());

        AsterixReplicationProperties asterixReplicationProperties = null;
        if (asterixAppRuntimeContextProvider != null) {
            asterixReplicationProperties = ((IAsterixPropertiesProvider) asterixAppRuntimeContextProvider
                    .getAppContext()).getReplicationProperties();
        }

        if (asterixReplicationProperties != null && asterixReplicationProperties.isReplicationEnabled()) {
            this.logManager = new LogManagerWithReplication(this);
        } else {
            this.logManager = new LogManager(this);
        }

        this.recoveryManager = new RecoveryManager(this);

        if (asterixAppRuntimeContextProvider != null) {
            this.checkpointThread = new CheckpointThread(recoveryManager, logManager,
                    this.txnProperties.getCheckpointLSNThreshold(), this.txnProperties.getCheckpointPollFrequency());
            this.checkpointThread.start();
        } else {
            this.checkpointThread = null;
        }

        if (IS_PROFILE_MODE) {
            ecp = new EntityCommitProfiler(this, this.txnProperties.getCommitProfilerReportInterval());
            fecp = (Future<Object>) getAsterixAppRuntimeContextProvider().getThreadExecutor().submit(ecp);
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
    public IAsterixAppRuntimeContextProvider getAsterixAppRuntimeContextProvider() {
        return asterixAppRuntimeContextProvider;
    }

    public AsterixTransactionProperties getTransactionProperties() {
        return txnProperties;
    }

    @Override
    public String getId() {
        return id;
    }

    public void incrementEntityCommitCount() {
        ++profilerEntityCommitLogCount;
    }

    /**
     * Thread for profiling entity level commit count
     * This thread takes a report interval (in seconds) parameter and
     * reports entity level commit count every report interval (in seconds)
     * only if IS_PROFILE_MODE is set to true.
     * However, the thread doesn't start reporting the count until the entityCommitCount > 0.
     */
    static class EntityCommitProfiler implements Callable<Boolean> {
        private static final Logger LOGGER = Logger.getLogger(EntityCommitProfiler.class.getName());
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
            this.reportIntervalInMillisec = reportIntervalInSeconds * 1000;
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
                    //output the count
                    outputCount();
                }
            }
        }

        private void outputCount() {
            long currentTimeStamp = System.currentTimeMillis();
            long currentEntityCommitCount = txnSubsystem.profilerEntityCommitLogCount;

            LOGGER.severe("EntityCommitProfiler ReportRound[" + reportRound + "], AbsoluteTimeStamp[" + currentTimeStamp
                    + "], ActualRelativeTimeStamp[" + (currentTimeStamp - startTimeStamp)
                    + "], ExpectedRelativeTimeStamp[" + (reportIntervalInSeconds * reportRound) + "], IIPS["
                    + ((currentEntityCommitCount - lastEntityCommitCount) / reportIntervalInSeconds) + "], IPS["
                    + (currentEntityCommitCount / (reportRound * reportIntervalInSeconds)) + "]");

            lastEntityCommitCount = currentEntityCommitCount;
            ++reportRound;
        }
    }

}
