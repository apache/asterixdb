/*
 * Copyright 2009-2011 by The Regents of the University of California
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

import java.util.HashMap;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import edu.uci.ics.asterix.transaction.management.exception.ACIDException;
import edu.uci.ics.asterix.transaction.management.service.logging.LogActionType;
import edu.uci.ics.asterix.transaction.management.service.logging.LogType;

/**
 * An implementation of the @see ITransactionManager interface that provides
 * implementation of APIs for governing the lifecycle of a transaction.
 */
public class TransactionManager implements ITransactionManager {
    private static final Logger LOGGER = Logger.getLogger(TransactionManager.class.getName());
    private final TransactionProvider transactionProvider;
    private Map<JobId, TransactionContext> transactionContextRepository = new HashMap<JobId, TransactionContext>();

    public TransactionManager(TransactionProvider provider) {
        this.transactionProvider = provider;
    }

    @Override
    public void abortTransaction(TransactionContext txnContext) throws ACIDException {
        synchronized (txnContext) {
            if (txnContext.getTxnState().equals(TransactionState.ABORTED)) {
                return;
            }

            try {
                transactionProvider.getRecoveryManager().rollbackTransaction(txnContext);
            } catch (Exception ae) {
                String msg = "Could not complete rollback! System is in an inconsistent state";
                if (LOGGER.isLoggable(Level.SEVERE)) {
                    LOGGER.severe(msg);
                }
                throw new Error(msg);
            } finally {
                txnContext.releaseResources();
                transactionProvider.getLockManager().releaseLocks(txnContext);
                transactionContextRepository.remove(txnContext.getJobId());
                txnContext.setTxnState(TransactionState.ABORTED);
            }
        }
    }

    @Override
    public TransactionContext beginTransaction(JobId jobId) throws ACIDException {
        TransactionContext txnContext = new TransactionContext(jobId, transactionProvider);
        synchronized (this) {
            transactionContextRepository.put(jobId, txnContext);
        }
        return txnContext;
    }

    @Override
    public TransactionContext getTransactionContext(JobId jobId) throws ACIDException {
        synchronized (transactionContextRepository) {
            TransactionContext context = transactionContextRepository.get(jobId);
            if (context == null) {
                context = transactionContextRepository.get(jobId);
                context = new TransactionContext(jobId, transactionProvider);
                transactionContextRepository.put(jobId, context);
            }
            return context;
        }
    }

    @Override
    public void commitTransaction(TransactionContext txnContext) throws ACIDException {
        synchronized (txnContext) {
            if ((txnContext.getTxnState().equals(TransactionState.COMMITTED))) {
                return;
            }

            try {
                if (txnContext.getTransactionType().equals(TransactionContext.TransactionType.READ_WRITE)) { // conditionally
                    // write
                    // commit
                    // log
                    // record
                    transactionProvider.getLogManager().log(txnContext.getLastLogLocator(), txnContext, (byte) (-1), 0,
                            LogType.COMMIT, LogActionType.NO_OP, 0, null, null);
                }
            } catch (ACIDException ae) {
                if (LOGGER.isLoggable(Level.SEVERE)) {
                    LOGGER.severe(" caused exception in commit !" + txnContext.getJobId());
                }
                throw ae;
            } finally {
                txnContext.releaseResources();
                transactionProvider.getLockManager().releaseLocks(txnContext); // release
                transactionContextRepository.remove(txnContext.getJobId());
                txnContext.setTxnState(TransactionState.COMMITTED);
            }
        }
    }

    @Override
    public void completedTransaction(TransactionContext txnContext, boolean success) throws ACIDException {
        if (!success) {
            abortTransaction(txnContext);
        } else {
            commitTransaction(txnContext);
        }
    }

    @Override
    public TransactionProvider getTransactionProvider() {
        return transactionProvider;
    }

}
