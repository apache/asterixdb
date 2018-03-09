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
package org.apache.asterix.replication.logging;

import java.nio.ByteBuffer;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.asterix.common.api.INcApplicationContext;
import org.apache.asterix.common.transactions.ILogManager;
import org.apache.asterix.common.transactions.ILogRecord;
import org.apache.asterix.common.transactions.ILogRequester;
import org.apache.asterix.common.transactions.LogSource;
import org.apache.asterix.common.transactions.LogType;
import org.apache.asterix.common.utils.TransactionUtil;
import org.apache.asterix.replication.api.IReplicationWorker;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class RemoteLogsProcessor implements ILogRequester {

    private static final Logger LOGGER = LogManager.getLogger();
    private final LinkedBlockingQueue<RemoteLogRecord> remoteLogsQ;
    private final ILogManager logManager;

    public RemoteLogsProcessor(INcApplicationContext appCtx) {
        logManager = appCtx.getTransactionSubsystem().getLogManager();
        remoteLogsQ = new LinkedBlockingQueue<>();
        appCtx.getThreadExecutor().execute(new RemoteLogsNotifier(appCtx, remoteLogsQ));
    }

    public void process(ByteBuffer logsBatch, RemoteLogRecord reusableLog, IReplicationWorker worker) {
        while (logsBatch.hasRemaining()) {
            // get rid of log size
            logsBatch.getInt();
            reusableLog.readRemoteLog(logsBatch);
            reusableLog.setLogSource(LogSource.REMOTE);
            switch (reusableLog.getLogType()) {
                case LogType.UPDATE:
                case LogType.ENTITY_COMMIT:
                case LogType.FILTER:
                    logManager.log(reusableLog);
                    break;
                case LogType.JOB_COMMIT:
                case LogType.ABORT:
                    RemoteLogRecord jobTerminationLog = new RemoteLogRecord();
                    TransactionUtil.formJobTerminateLogRecord(jobTerminationLog, reusableLog.getTxnId(),
                            reusableLog.getLogType() == LogType.JOB_COMMIT);
                    jobTerminationLog.setRequester(this);
                    jobTerminationLog.setReplicationWorker(worker);
                    jobTerminationLog.setLogSource(LogSource.REMOTE);
                    logManager.log(jobTerminationLog);
                    break;
                case LogType.FLUSH:
                    RemoteLogRecord flushLog = new RemoteLogRecord();
                    TransactionUtil.formFlushLogRecord(flushLog, reusableLog.getDatasetId(),
                            reusableLog.getResourcePartition(), reusableLog.getFlushingComponentMinId(),
                            reusableLog.getFlushingComponentMaxId(), null);
                    flushLog.setRequester(this);
                    flushLog.setLogSource(LogSource.REMOTE);
                    flushLog.setMasterLsn(reusableLog.getLSN());
                    logManager.log(flushLog);
                    break;
                default:
                    LOGGER.error(() -> "Unsupported LogType: " + reusableLog.getLogType());
            }
        }
    }

    @Override
    public void notifyFlushed(ILogRecord logRecord) {
        remoteLogsQ.add((RemoteLogRecord) logRecord);
    }
}
