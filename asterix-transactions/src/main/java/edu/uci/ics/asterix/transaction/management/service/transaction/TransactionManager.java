/*
 * Copyright 2009-2013 by The Regents of the University of California
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

import java.io.IOException;
import java.io.OutputStream;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;

import edu.uci.ics.asterix.common.exceptions.ACIDException;
import edu.uci.ics.asterix.common.transactions.DatasetId;
import edu.uci.ics.asterix.common.transactions.ITransactionContext;
import edu.uci.ics.asterix.common.transactions.ITransactionManager;
import edu.uci.ics.asterix.common.transactions.JobId;
import edu.uci.ics.asterix.transaction.management.service.logging.LogRecord;
import edu.uci.ics.hyracks.api.lifecycle.ILifeCycleComponent;

/**
 * An implementation of the @see ITransactionManager interface that provides
 * implementation of APIs for governing the lifecycle of a transaction.
 */
public class TransactionManager implements ITransactionManager, ILifeCycleComponent {

    public static final boolean IS_DEBUG_MODE = false;//true
    private static final Logger LOGGER = Logger.getLogger(TransactionManager.class.getName());
    private final TransactionSubsystem txnSubsystem;
    private Map<JobId, ITransactionContext> transactionContextRepository = new ConcurrentHashMap<JobId, ITransactionContext>();
    private AtomicInteger maxJobId = new AtomicInteger(0);

    public TransactionManager(TransactionSubsystem provider) {
        this.txnSubsystem = provider;
    }

    @Override
    public void abortTransaction(ITransactionContext txnCtx, DatasetId datasetId, int PKHashVal) throws ACIDException {
        if (txnCtx.getTxnState() != ITransactionManager.ABORTED) {
            txnCtx.setTxnState(ITransactionManager.ABORTED);
        }
        try {
            if (txnCtx.isWriteTxn()) {
                LogRecord logRecord = ((TransactionContext) txnCtx).getLogRecord();
                logRecord.formJobTerminateLogRecord(txnCtx, false);
                txnSubsystem.getLogManager().log(logRecord);
                txnSubsystem.getRecoveryManager().rollbackTransaction(txnCtx);
            }
        } catch (Exception ae) {
            String msg = "Could not complete rollback! System is in an inconsistent state";
            if (LOGGER.isLoggable(Level.SEVERE)) {
                LOGGER.severe(msg);
            }
            ae.printStackTrace();
            throw new ACIDException(msg, ae);
        } finally {
            ((TransactionContext) txnCtx).cleanupForAbort();
            txnSubsystem.getLockManager().releaseLocks(txnCtx);
            transactionContextRepository.remove(txnCtx.getJobId());
        }
    }

    @Override
    public ITransactionContext beginTransaction(JobId jobId) throws ACIDException {
        return getTransactionContext(jobId, true);
    }

    @Override
    public ITransactionContext getTransactionContext(JobId jobId, boolean createIfNotExist) throws ACIDException {
        setMaxJobId(jobId.getId());
        ITransactionContext txnCtx = transactionContextRepository.get(jobId);
        if (txnCtx == null) {
            if (createIfNotExist) {
                synchronized (this) {
                    txnCtx = transactionContextRepository.get(jobId);
                    if (txnCtx == null) {
                        txnCtx = new TransactionContext(jobId, txnSubsystem);
                        transactionContextRepository.put(jobId, txnCtx);
                    }
                }
            } else {
                throw new ACIDException("TransactionContext of " + jobId + " doesn't exist.");
            }
        }
        return txnCtx;
    }

    @Override
    public void commitTransaction(ITransactionContext txnCtx, DatasetId datasetId, int PKHashVal) throws ACIDException {
        //Only job-level commits call this method. 
        try {
            if (txnCtx.isWriteTxn()) {
                LogRecord logRecord = ((TransactionContext) txnCtx).getLogRecord();
                logRecord.formJobTerminateLogRecord(txnCtx, true);
                txnSubsystem.getLogManager().log(logRecord);
            }
        } catch (Exception ae) {
            if (LOGGER.isLoggable(Level.SEVERE)) {
                LOGGER.severe(" caused exception in commit !" + txnCtx.getJobId());
            }
            throw ae;
        } finally {
            txnSubsystem.getLockManager().releaseLocks(txnCtx); // release
            transactionContextRepository.remove(txnCtx.getJobId());
            txnCtx.setTxnState(ITransactionManager.COMMITTED);
        }
    }

    @Override
    public void completedTransaction(ITransactionContext txnContext, DatasetId datasetId, int PKHashVal, boolean success)
            throws ACIDException {
        if (!success) {
            abortTransaction(txnContext, datasetId, PKHashVal);
        } else {
            commitTransaction(txnContext, datasetId, PKHashVal);
        }
    }

    @Override
    public TransactionSubsystem getTransactionProvider() {
        return txnSubsystem;
    }

    public void setMaxJobId(int jobId) {
        int maxId = maxJobId.get();
        if (jobId > maxId) {
            maxJobId.compareAndSet(maxId, jobId);
        }
    }

    public int getMaxJobId() {
        return maxJobId.get();
    }

    @Override
    public void start() {
        //no op
    }

    @Override
    public void stop(boolean dumpState, OutputStream os) {
        if (dumpState) {
            //#. dump TxnContext
            dumpTxnContext(os);

            try {
                os.flush();
            } catch (IOException e) {
                //ignore
            }
        }
    }

    private void dumpTxnContext(OutputStream os) {
        JobId jobId;
        ITransactionContext txnCtx;
        StringBuilder sb = new StringBuilder();

        try {
            sb.append("\n>>dump_begin\t>>----- [ConfVars] -----");
            Set<Map.Entry<JobId, ITransactionContext>> entrySet = transactionContextRepository.entrySet();
            if (entrySet != null) {
                for (Map.Entry<JobId, ITransactionContext> entry : entrySet) {
                    if (entry != null) {
                        jobId = entry.getKey();
                        if (jobId != null) {
                            sb.append("\n" + jobId);
                        } else {
                            sb.append("\nJID:null");
                        }

                        txnCtx = entry.getValue();
                        if (txnCtx != null) {
                            sb.append(txnCtx.prettyPrint());
                        } else {
                            sb.append("\nTxnCtx:null");
                        }
                    }
                }
            }

            sb.append("\n>>dump_end\t>>----- [ConfVars] -----\n");
            os.write(sb.toString().getBytes());
        } catch (Exception e) {
            //ignore exception and continue dumping as much as possible.
            if (IS_DEBUG_MODE) {
                e.printStackTrace();
            }
        }
    }
}
