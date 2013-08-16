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
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;

import edu.uci.ics.asterix.common.exceptions.ACIDException;
import edu.uci.ics.asterix.common.transactions.DatasetId;
import edu.uci.ics.asterix.common.transactions.ILogRecord;
import edu.uci.ics.asterix.common.transactions.ITransactionContext;
import edu.uci.ics.asterix.common.transactions.ITransactionManager;
import edu.uci.ics.asterix.common.transactions.JobId;
import edu.uci.ics.asterix.transaction.management.service.logging.LogRecord;
import edu.uci.ics.asterix.transaction.management.service.logging.LogType;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.api.lifecycle.ILifeCycleComponent;

/**
 * An implementation of the @see ITransactionManager interface that provides
 * implementation of APIs for governing the lifecycle of a transaction.
 */
public class TransactionManager implements ITransactionManager, ILifeCycleComponent {

    public static final boolean IS_DEBUG_MODE = false;//true
    private static final Logger LOGGER = Logger.getLogger(TransactionManager.class.getName());
    private final TransactionSubsystem txnSubsystem;
    private Map<JobId, ITransactionContext> transactionContextRepository = new HashMap<JobId, ITransactionContext>();
    private AtomicInteger maxJobId = new AtomicInteger(0);
    private final ILogRecord logRecord;

    public TransactionManager(TransactionSubsystem provider) {
        this.txnSubsystem = provider;
        logRecord = new LogRecord();
    }

    @Override
    public void abortTransaction(ITransactionContext txnContext, DatasetId datasetId, int PKHashVal)
            throws ACIDException {
        synchronized (txnContext) {
            if (txnContext.getTxnState().equals(TransactionState.ABORTED)) {
                return;
            }

            try {
                txnSubsystem.getRecoveryManager().rollbackTransaction(txnContext);
            } catch (Exception ae) {
                String msg = "Could not complete rollback! System is in an inconsistent state";
                if (LOGGER.isLoggable(Level.SEVERE)) {
                    LOGGER.severe(msg);
                }
                ae.printStackTrace();
                throw new Error(msg);
            } finally {
                txnSubsystem.getLockManager().releaseLocks(txnContext);
                transactionContextRepository.remove(txnContext.getJobId());
                txnContext.setTxnState(TransactionState.ABORTED);
            }
        }
    }

    @Override
    public ITransactionContext beginTransaction(JobId jobId) throws ACIDException {
        setMaxJobId(jobId.getId());
        ITransactionContext txnContext = new TransactionContext(jobId, txnSubsystem);
        synchronized (this) {
            transactionContextRepository.put(jobId, txnContext);
        }
        return txnContext;
    }

    @Override
    public ITransactionContext getTransactionContext(JobId jobId) throws ACIDException {
        setMaxJobId(jobId.getId());
        synchronized (transactionContextRepository) {

            ITransactionContext context = transactionContextRepository.get(jobId);
            if (context == null) {
                context = transactionContextRepository.get(jobId);
                context = new TransactionContext(jobId, txnSubsystem);
                transactionContextRepository.put(jobId, context);
            }
            return context;
        }
    }

    @Override
    public void commitTransaction(ITransactionContext txnCtx, DatasetId datasetId, int PKHashVal) throws ACIDException {
        synchronized (txnCtx) {
            if ((txnCtx.getTxnState().equals(TransactionState.COMMITTED))) {
                return;
            }

            //There is either job-level commit or entity-level commit.
            //The job-level commit will have -1 value both for datasetId and PKHashVal.

            //for entity-level commit
            if (PKHashVal != -1) {
                txnSubsystem.getLockManager().unlock(datasetId, PKHashVal, txnCtx, true);
                return;
            }

            //for job-level commit
            try {
                if (txnCtx.getTransactionType().equals(ITransactionContext.TransactionType.READ_WRITE)) {
                    logRecord.formCommitLogRecord(txnCtx, LogType.JOB_COMMIT, txnCtx.getJobId().getId(), -1, -1);
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
                txnCtx.setTxnState(TransactionState.COMMITTED);
            }
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
        maxJobId.set(Math.max(maxJobId.get(), jobId));
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
