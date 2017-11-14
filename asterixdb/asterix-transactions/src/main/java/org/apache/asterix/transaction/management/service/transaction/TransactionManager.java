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

import java.io.OutputStream;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.asterix.common.exceptions.ACIDException;
import org.apache.asterix.common.transactions.DatasetId;
import org.apache.asterix.common.transactions.ITransactionContext;
import org.apache.asterix.common.transactions.ITransactionManager;
import org.apache.asterix.common.transactions.ITransactionSubsystem;
import org.apache.asterix.common.transactions.TxnId;
import org.apache.asterix.common.transactions.LogRecord;
import org.apache.asterix.common.utils.TransactionUtil;
import org.apache.hyracks.api.lifecycle.ILifeCycleComponent;

/**
 * An implementation of the @see ITransactionManager interface that provides
 * implementation of APIs for governing the lifecycle of a transaction.
 */
public class TransactionManager implements ITransactionManager, ILifeCycleComponent {

    public static final boolean IS_DEBUG_MODE = false;//true
    private static final Logger LOGGER = Logger.getLogger(TransactionManager.class.getName());
    private final ITransactionSubsystem txnSubsystem;
    private Map<TxnId, ITransactionContext> transactionContextRepository = new ConcurrentHashMap<>();
    private AtomicLong maxTxnId = new AtomicLong(0);

    public TransactionManager(ITransactionSubsystem provider) {
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
                TransactionUtil.formJobTerminateLogRecord(txnCtx, logRecord, false);
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
            txnCtx.complete();
            txnSubsystem.getLockManager().releaseLocks(txnCtx);
            transactionContextRepository.remove(txnCtx.getTxnId());
        }
    }

    @Override
    public ITransactionContext beginTransaction(TxnId txnId) throws ACIDException {
        return getTransactionContext(txnId, true);
    }

    @Override
    public ITransactionContext getTransactionContext(TxnId txnId, boolean createIfNotExist) throws ACIDException {
        setMaxTxnId(txnId.getId());
        ITransactionContext txnCtx = transactionContextRepository.get(txnId);
        if (txnCtx == null) {
            if (createIfNotExist) {
                synchronized (this) {
                    txnCtx = transactionContextRepository.get(txnId);
                    if (txnCtx == null) {
                        txnCtx = new TransactionContext(txnId);
                        transactionContextRepository.put(txnId, txnCtx);
                    }
                }
            } else {
                throw new ACIDException("TransactionContext of " + txnId + " doesn't exist.");
            }
        }
        return txnCtx;
    }

    @Override
    public void commitTransaction(ITransactionContext txnCtx, DatasetId datasetId, int PKHashVal)
            throws ACIDException {
        //Only job-level commits call this method.
        try {
            if (txnCtx.isWriteTxn()) {
                LogRecord logRecord = ((TransactionContext) txnCtx).getLogRecord();
                TransactionUtil.formJobTerminateLogRecord(txnCtx, logRecord, true);
                txnSubsystem.getLogManager().log(logRecord);
            }
        } catch (Exception ae) {
            if (LOGGER.isLoggable(Level.SEVERE)) {
                LOGGER.severe(" caused exception in commit !" + txnCtx.getTxnId());
            }
            throw ae;
        } finally {
            txnCtx.complete();
            txnSubsystem.getLockManager().releaseLocks(txnCtx);
            transactionContextRepository.remove(txnCtx.getTxnId());
            txnCtx.setTxnState(ITransactionManager.COMMITTED);
        }
    }

    @Override
    public void completedTransaction(ITransactionContext txnContext, DatasetId datasetId, int PKHashVal,
            boolean success) throws ACIDException {
        if (!success) {
            abortTransaction(txnContext, datasetId, PKHashVal);
        } else {
            commitTransaction(txnContext, datasetId, PKHashVal);
        }
    }

    @Override
    public ITransactionSubsystem getTransactionSubsystem() {
        return txnSubsystem;
    }

    public void setMaxTxnId(long txnId) {
        long maxId = maxTxnId.get();
        if (txnId > maxId) {
            maxTxnId.compareAndSet(maxId, txnId);
        }
    }

    @Override
    public long getMaxTxnId() {
        return maxTxnId.get();
    }

    @Override
    public void start() {
        //no op
    }

    @Override
    public void stop(boolean dumpState, OutputStream os) {
        if (dumpState) {
            dumpState(os);
        }
    }

    @Override
    public void dumpState(OutputStream os) {
        //#. dump TxnContext
        dumpTxnContext(os);
    }

    private void dumpTxnContext(OutputStream os) {
        TxnId txnId;
        ITransactionContext txnCtx;
        StringBuilder sb = new StringBuilder();

        try {
            sb.append("\n>>dump_begin\t>>----- [ConfVars] -----");
            Set<Map.Entry<TxnId, ITransactionContext>> entrySet = transactionContextRepository.entrySet();
            if (entrySet != null) {
                for (Map.Entry<TxnId, ITransactionContext> entry : entrySet) {
                    if (entry != null) {
                        txnId = entry.getKey();
                        if (txnId != null) {
                            sb.append("\n" + txnId);
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
