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
package edu.uci.ics.asterix.transaction.management.service.transaction;

import edu.uci.ics.asterix.transaction.management.exception.ACIDException;
import edu.uci.ics.asterix.transaction.management.resource.TransactionalResourceManagerRepository;
import edu.uci.ics.asterix.transaction.management.service.locking.ILockManager;
import edu.uci.ics.asterix.transaction.management.service.locking.LockManager;
import edu.uci.ics.asterix.transaction.management.service.logging.ILogManager;
import edu.uci.ics.asterix.transaction.management.service.logging.IndexLoggerRepository;
import edu.uci.ics.asterix.transaction.management.service.logging.LogManager;
import edu.uci.ics.asterix.transaction.management.service.recovery.CheckpointThread;
import edu.uci.ics.asterix.transaction.management.service.recovery.IAsterixAppRuntimeContextProvider;
import edu.uci.ics.asterix.transaction.management.service.recovery.IRecoveryManager;
import edu.uci.ics.asterix.transaction.management.service.recovery.RecoveryManager;

/**
 * Provider for all the sub-systems (transaction/lock/log/recovery) managers.
 * Users of transaction sub-systems must obtain them from the provider.
 */
public class TransactionSubsystem {
    private final String id;
    private final ILogManager logManager;
    private final ILockManager lockManager;
    private final ITransactionManager transactionManager;
    private final IRecoveryManager recoveryManager;
    private final TransactionalResourceManagerRepository resourceRepository;
    private final IndexLoggerRepository loggerRepository;
    private final IAsterixAppRuntimeContextProvider asterixAppRuntimeContextProvider;
    private final CheckpointThread checkpointThread;

    public TransactionSubsystem(String id, IAsterixAppRuntimeContextProvider asterixAppRuntimeContextProvider)
            throws ACIDException {
        this.id = id;
        this.transactionManager = new TransactionManager(this);
        this.logManager = new LogManager(this);
        this.lockManager = new LockManager(this);
        this.recoveryManager = new RecoveryManager(this);
        this.loggerRepository = new IndexLoggerRepository(this);
        this.resourceRepository = new TransactionalResourceManagerRepository();
        this.asterixAppRuntimeContextProvider = asterixAppRuntimeContextProvider;
        if (asterixAppRuntimeContextProvider != null) {
	        this.checkpointThread = new CheckpointThread(recoveryManager,
	                asterixAppRuntimeContextProvider.getIndexLifecycleManager(), 0);
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

    public TransactionalResourceManagerRepository getTransactionalResourceRepository() {
        return resourceRepository;
    }

    public IndexLoggerRepository getTreeLoggerRepository() {
        return loggerRepository;
    }

    public IAsterixAppRuntimeContextProvider getAsterixAppRuntimeContextProvider() {
        return asterixAppRuntimeContextProvider;
    }

    public String getId() {
        return id;
    }

}