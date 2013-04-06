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
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;

import edu.uci.ics.asterix.transaction.management.exception.ACIDException;
import edu.uci.ics.asterix.transaction.management.service.logging.LogType;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;

/**
 * An implementation of the @see ITransactionManager interface that provides
 * implementation of APIs for governing the lifecycle of a transaction.
 */
public class TransactionManager implements ITransactionManager {
    private static final Logger LOGGER = Logger.getLogger(TransactionManager.class.getName());
    private final TransactionSubsystem transactionProvider;
    private Map<JobId, TransactionContext> transactionContextRepository = new HashMap<JobId, TransactionContext>();
    private AtomicInteger maxJobId = new AtomicInteger(0);

    public TransactionManager(TransactionSubsystem provider) {
        this.transactionProvider = provider;
    }

    @Override
    public void abortTransaction(TransactionContext txnContext, DatasetId datasetId, int PKHashVal)
            throws ACIDException {
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
                ae.printStackTrace();
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
        setMaxJobId(jobId.getId());
        TransactionContext txnContext = new TransactionContext(jobId, transactionProvider);
        synchronized (this) {
            transactionContextRepository.put(jobId, txnContext);
        }
        return txnContext;
    }

    @Override
    public TransactionContext getTransactionContext(JobId jobId) throws ACIDException {
        setMaxJobId(jobId.getId());
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
    public void commitTransaction(TransactionContext txnContext, DatasetId datasetId, int PKHashVal)
            throws ACIDException {
        synchronized (txnContext) {
            if ((txnContext.getTxnState().equals(TransactionState.COMMITTED))) {
                return;
            }

            //There is either job-level commit or entity-level commit.
            //The job-level commit will have -1 value both for datasetId and PKHashVal.

            //for entity-level commit
            if (PKHashVal != -1) {
                transactionProvider.getLockManager().unlock(datasetId, PKHashVal, txnContext, true);
                /*****************************
                try {
                    //decrease the transaction reference count on index
                    txnContext.decreaseActiveTransactionCountOnIndexes();
                } catch (HyracksDataException e) {
                    throw new ACIDException("failed to complete index operation", e);
                }
                *****************************/
                return;
            }

            //for job-level commit
            try {
                if (txnContext.getTransactionType().equals(TransactionContext.TransactionType.READ_WRITE)) {
                    transactionProvider.getLogManager().log(LogType.COMMIT, txnContext, -1, -1, -1, (byte) 0, 0, null,
                            null, txnContext.getLastLogLocator());
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
    public void completedTransaction(TransactionContext txnContext, DatasetId datasetId, int PKHashVal, boolean success)
            throws ACIDException {
        if (!success) {
            abortTransaction(txnContext, datasetId, PKHashVal);
        } else {
            commitTransaction(txnContext, datasetId, PKHashVal);
        }
    }

    @Override
    public TransactionSubsystem getTransactionProvider() {
        return transactionProvider;
    }
    
    public void setMaxJobId(int jobId) {
        maxJobId.set(Math.max(maxJobId.get(), jobId));
    }
    
    public int getMaxJobId() {
        return maxJobId.get();
    }
}
