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
package org.apache.asterix.transaction.management.service.logging;

import org.apache.asterix.common.exceptions.ACIDException;
import org.apache.asterix.common.replication.IReplicationManager;
import org.apache.asterix.common.transactions.ILogRecord;
import org.apache.asterix.common.transactions.ITransactionContext;
import org.apache.asterix.common.transactions.ITransactionManager;
import org.apache.asterix.common.transactions.LogSource;
import org.apache.asterix.common.transactions.LogType;
import org.apache.asterix.transaction.management.service.transaction.TransactionSubsystem;

public class LogManagerWithReplication extends LogManager {

    private IReplicationManager replicationManager;

    public LogManagerWithReplication(TransactionSubsystem txnSubsystem) throws ACIDException {
        super(txnSubsystem);
    }

    @Override
    public void log(ILogRecord logRecord) throws ACIDException {
        if (logRecord.getLogSize() > logPageSize) {
            throw new IllegalStateException();
        }

        if (logRecord.getLogType() == LogType.FLUSH) {
            flushLogsQ.offer(logRecord);
            return;
        }

        appendToLogTail(logRecord);
    }

    @Override
    protected void appendToLogTail(ILogRecord logRecord) throws ACIDException {
        syncAppendToLogTail(logRecord);

        if (logRecord.getLogSource() == LogSource.LOCAL) {
            replicationManager.replicateLog(logRecord);
        }

        if (logRecord.getLogSource() == LogSource.LOCAL) {
            if ((logRecord.getLogType() == LogType.JOB_COMMIT || logRecord.getLogType() == LogType.ABORT)
                    && !logRecord.isFlushed()) {
                synchronized (logRecord) {
                    while (!logRecord.isFlushed()) {
                        try {
                            logRecord.wait();
                        } catch (InterruptedException e) {
                            //ignore
                        }
                    }
                }
            }

            //wait for job ACK from replicas
            //TODO should JOB_ABORT be added as well?
            if ((logRecord.getLogType() == LogType.JOB_COMMIT) && !replicationManager.hasBeenReplicated(logRecord)) {
                synchronized (logRecord) {
                    while (!replicationManager.hasBeenReplicated(logRecord)) {
                        try {
                            logRecord.wait();
                        } catch (InterruptedException e) {
                            //ignore
                        }
                    }
                }
            }
        }
    }

    @Override
    protected synchronized void syncAppendToLogTail(ILogRecord logRecord) throws ACIDException {
        ITransactionContext txnCtx = null;

        if (logRecord.getLogSource() == LogSource.LOCAL) {
            if (logRecord.getLogType() != LogType.FLUSH) {
                txnCtx = logRecord.getTxnCtx();
                if (txnCtx.getTxnState() == ITransactionManager.ABORTED && logRecord.getLogType() != LogType.ABORT) {
                    throw new ACIDException(
                            "Aborted job(" + txnCtx.getJobId() + ") tried to write non-abort type log record.");
                }
            }
        }

        if (getLogFileOffset(appendLSN.get()) + logRecord.getLogSize() > logFileSize) {
            prepareNextLogFile();
            appendPage.isFull(true);
            getAndInitNewPage();
        } else if (!appendPage.hasSpace(logRecord.getLogSize())) {
            appendPage.isFull(true);
            getAndInitNewPage();
        }
        if (logRecord.getLogSource() == LogSource.LOCAL) {
            if (logRecord.getLogType() == LogType.UPDATE) {
                logRecord.setPrevLSN(txnCtx.getLastLSN());
            }
        }
        appendPage.appendWithReplication(logRecord, appendLSN.get());

        if (logRecord.getLogType() == LogType.FLUSH) {
            logRecord.setLSN(appendLSN.get());
        }

        appendLSN.addAndGet(logRecord.getLogSize());
    }

    @Override
    public void setReplicationManager(IReplicationManager replicationManager) {
        this.replicationManager = replicationManager;
    }

}