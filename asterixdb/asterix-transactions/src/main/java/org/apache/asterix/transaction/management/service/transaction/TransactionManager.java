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

import java.io.IOException;
import java.io.OutputStream;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.asterix.common.exceptions.ACIDException;
import org.apache.asterix.common.transactions.ITransactionContext;
import org.apache.asterix.common.transactions.ITransactionManager;
import org.apache.asterix.common.transactions.ITransactionSubsystem;
import org.apache.asterix.common.transactions.LogRecord;
import org.apache.asterix.common.transactions.TransactionOptions;
import org.apache.asterix.common.transactions.TxnId;
import org.apache.asterix.common.utils.TransactionUtil;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.lifecycle.ILifeCycleComponent;
import org.apache.hyracks.util.annotations.ThreadSafe;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

@ThreadSafe
public class TransactionManager implements ITransactionManager, ILifeCycleComponent {

    private static final Logger LOGGER = LogManager.getLogger();
    private final ITransactionSubsystem txnSubsystem;
    private final Map<TxnId, ITransactionContext> txnCtxRepository = new ConcurrentHashMap<>();
    private final AtomicLong maxTxnId = new AtomicLong(0);

    public TransactionManager(ITransactionSubsystem provider) {
        this.txnSubsystem = provider;
    }

    @Override
    public synchronized ITransactionContext beginTransaction(TxnId txnId, TransactionOptions options)
            throws ACIDException {
        ITransactionContext txnCtx = txnCtxRepository.get(txnId);
        if (txnCtx != null) {
            throw new ACIDException("Transaction with the same (" + txnId + ") already exists");
        }
        txnCtx = TransactionContextFactory.create(txnId, options);
        txnCtxRepository.put(txnId, txnCtx);
        ensureMaxTxnId(txnId.getId());
        return txnCtx;
    }

    @Override
    public ITransactionContext getTransactionContext(TxnId txnId) throws ACIDException {
        ITransactionContext txnCtx = txnCtxRepository.get(txnId);
        if (txnCtx == null) {
            throw new ACIDException("Transaction " + txnId + " doesn't exist.");
        }
        return txnCtx;
    }

    @Override
    public void commitTransaction(TxnId txnId) throws ACIDException {
        final ITransactionContext txnCtx = getTransactionContext(txnId);
        try {
            if (txnCtx.isWriteTxn()) {
                LogRecord logRecord = new LogRecord();
                TransactionUtil.formJobTerminateLogRecord(txnCtx, logRecord, true);
                txnSubsystem.getLogManager().log(logRecord);
                txnCtx.setTxnState(ITransactionManager.COMMITTED);
            }
        } catch (Exception e) {
            if (LOGGER.isErrorEnabled()) {
                LOGGER.error(" caused exception in commit !" + txnCtx.getTxnId());
            }
            throw e;
        } finally {
            txnCtx.complete();
            txnSubsystem.getLockManager().releaseLocks(txnCtx);
            txnCtxRepository.remove(txnCtx.getTxnId());
        }
    }

    @Override
    public void abortTransaction(TxnId txnId) throws ACIDException {
        final ITransactionContext txnCtx = getTransactionContext(txnId);
        try {
            if (txnCtx.isWriteTxn()) {
                LogRecord logRecord = new LogRecord();
                TransactionUtil.formJobTerminateLogRecord(txnCtx, logRecord, false);
                txnSubsystem.getLogManager().log(logRecord);
                txnSubsystem.getCheckpointManager().secure(txnId);
                txnSubsystem.getRecoveryManager().rollbackTransaction(txnCtx);
                txnCtx.setTxnState(ITransactionManager.ABORTED);
            }
        } catch (HyracksDataException e) {
            String msg = "Could not complete rollback! System is in an inconsistent state";
            if (LOGGER.isErrorEnabled()) {
                LOGGER.log(Level.ERROR, msg, e);
            }
            throw new ACIDException(msg, e);
        } finally {
            txnCtx.complete();
            txnSubsystem.getLockManager().releaseLocks(txnCtx);
            txnCtxRepository.remove(txnCtx.getTxnId());
            txnSubsystem.getCheckpointManager().completed(txnId);
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
        dumpTxnContext(os);
    }

    @Override
    public void ensureMaxTxnId(long txnId) {
        maxTxnId.updateAndGet(current -> Math.max(current, txnId));
    }

    private void dumpTxnContext(OutputStream os) {
        TxnId txnId;
        ITransactionContext txnCtx;
        StringBuilder sb = new StringBuilder();
        try {
            sb.append("\n>>dump_begin\t>>----- [ConfVars] -----");
            Set<Map.Entry<TxnId, ITransactionContext>> entrySet = txnCtxRepository.entrySet();
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
                        sb.append(((AbstractTransactionContext) txnCtx).prettyPrint());
                    } else {
                        sb.append("\nTxnCtx:null");
                    }
                }
            }
            sb.append("\n>>dump_end\t>>----- [ConfVars] -----\n");
            os.write(sb.toString().getBytes());
        } catch (IOException e) {
            LOGGER.log(Level.WARN, "exception while dumping state", e);
        }
    }
}
