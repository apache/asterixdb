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

    public TransactionSubsystem(String id, IAsterixAppRuntimeContextProvider asterixAppRuntimeContextProvider,
            AsterixTransactionProperties txnProperties) throws ACIDException {
        this.asterixAppRuntimeContextProvider = asterixAppRuntimeContextProvider;
        this.id = id;
        this.txnProperties = txnProperties;
        this.transactionManager = new TransactionManager(this);
        this.lockManager = new ConcurrentLockManager(this);

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
            this.checkpointThread = new CheckpointThread(recoveryManager,
                    asterixAppRuntimeContextProvider.getDatasetLifecycleManager(),logManager,
                    this.txnProperties.getCheckpointLSNThreshold(), this.txnProperties.getCheckpointPollFrequency());
            this.checkpointThread.start();
        } else {
            this.checkpointThread = null;
        }
    }

    public ILogManager getLogManager() {
        return logManager;
    }

    public ILockManager getLockManager() {
        return lockManager;
    }

    public ITransactionManager getTransactionManager() {
        return transactionManager;
    }

    public IRecoveryManager getRecoveryManager() {
        return recoveryManager;
    }

    public IAsterixAppRuntimeContextProvider getAsterixAppRuntimeContextProvider() {
        return asterixAppRuntimeContextProvider;
    }

    public AsterixTransactionProperties getTransactionProperties() {
        return txnProperties;
    }

    public String getId() {
        return id;
    }

}