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

import java.util.logging.Logger;

import org.apache.asterix.common.transactions.ILogRecord;
import org.apache.asterix.common.transactions.ITransactionSubsystem;
import org.apache.asterix.common.transactions.LogSource;
import org.apache.asterix.common.transactions.LogType;
import org.apache.asterix.common.transactions.MutableLong;

public class ReplicatingLogBuffer extends LogBuffer {
    private static final Logger LOGGER = Logger.getLogger(ReplicatingLogBuffer.class.getName());

    public ReplicatingLogBuffer(ITransactionSubsystem txnSubsystem, int logPageSize, MutableLong flushLsn) {
        super(txnSubsystem, logPageSize, flushLsn);
    }

    @Override
    public void append(ILogRecord logRecord, long appendLsn) {
        logRecord.writeLogRecord(appendBuffer);

        if (logRecord.getLogSource() == LogSource.LOCAL && logRecord.getLogType() != LogType.FLUSH
                && logRecord.getLogType() != LogType.WAIT) {
            logRecord.getTxnCtx().setLastLSN(appendLsn);
        }

        synchronized (this) {
            appendOffset += logRecord.getLogSize();
            if (IS_DEBUG_MODE) {
                LOGGER.info("append()| appendOffset: " + appendOffset);
            }
            if (logRecord.getLogSource() == LogSource.LOCAL) {
                if (logRecord.getLogType() == LogType.JOB_COMMIT || logRecord.getLogType() == LogType.ABORT
                        || logRecord.getLogType() == LogType.WAIT) {
                    logRecord.isFlushed(false);
                    syncCommitQ.offer(logRecord);
                }
                if (logRecord.getLogType() == LogType.FLUSH) {
                    logRecord.isFlushed(false);
                    flushQ.offer(logRecord);
                }
            } else if (logRecord.getLogSource() == LogSource.REMOTE
                    && (logRecord.getLogType() == LogType.JOB_COMMIT || logRecord.getLogType() == LogType.ABORT)) {
                remoteJobsQ.offer(logRecord);
            }
            this.notify();
        }
    }

}
