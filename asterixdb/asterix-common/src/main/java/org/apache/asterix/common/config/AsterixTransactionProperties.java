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
package org.apache.asterix.common.config;

import java.util.Map;

import org.apache.hyracks.util.StorageUtil;
import org.apache.hyracks.util.StorageUtil.StorageUnit;

public class AsterixTransactionProperties extends AbstractAsterixProperties {

    private static final String TXN_LOG_BUFFER_NUMPAGES_KEY = "txn.log.buffer.numpages";
    private static int TXN_LOG_BUFFER_NUMPAGES_DEFAULT = 8;

    private static final String TXN_LOG_BUFFER_PAGESIZE_KEY = "txn.log.buffer.pagesize";
    private static final int TXN_LOG_BUFFER_PAGESIZE_DEFAULT = (128 << 10); // 128KB

    private static final String TXN_LOG_PARTITIONSIZE_KEY = "txn.log.partitionsize";
    private static final long TXN_LOG_PARTITIONSIZE_DEFAULT = StorageUtil.getSizeInBytes(256L, StorageUnit.MEGABYTE);

    private static final String TXN_LOG_CHECKPOINT_LSNTHRESHOLD_KEY = "txn.log.checkpoint.lsnthreshold";
    private static final int TXN_LOG_CHECKPOINT_LSNTHRESHOLD_DEFAULT = (64 << 20); // 64M

    private static final String TXN_LOG_CHECKPOINT_POLLFREQUENCY_KEY = "txn.log.checkpoint.pollfrequency";
    private static int TXN_LOG_CHECKPOINT_POLLFREQUENCY_DEFAULT = 120; // 120s

    private static final String TXN_LOG_CHECKPOINT_HISTORY_KEY = "txn.log.checkpoint.history";
    private static int TXN_LOG_CHECKPOINT_HISTORY_DEFAULT = 0;

    private static final String TXN_LOCK_ESCALATIONTHRESHOLD_KEY = "txn.lock.escalationthreshold";
    private static int TXN_LOCK_ESCALATIONTHRESHOLD_DEFAULT = 1000;

    private static final String TXN_LOCK_SHRINKTIMER_KEY = "txn.lock.shrinktimer";
    private static int TXN_LOCK_SHRINKTIMER_DEFAULT = 5000; // 5s

    private static final String TXN_LOCK_TIMEOUT_WAITTHRESHOLD_KEY = "txn.lock.timeout.waitthreshold";
    private static final int TXN_LOCK_TIMEOUT_WAITTHRESHOLD_DEFAULT = 60000; // 60s

    private static final String TXN_LOCK_TIMEOUT_SWEEPTHRESHOLD_KEY = "txn.lock.timeout.sweepthreshold";
    private static final int TXN_LOCK_TIMEOUT_SWEEPTHRESHOLD_DEFAULT = 10000; // 10s

    private static final String TXN_COMMIT_PROFILER_REPORT_INTERVAL_KEY = "txn.commitprofiler.reportinterval";
    private static final int TXN_COMMIT_PROFILER_REPORT_INTERVAL_DEFAULT = 5; // 5 seconds

    private static final String TXN_JOB_RECOVERY_MEMORY_SIZE_KEY = "txn.job.recovery.memorysize";
    private static final long TXN_JOB_RECOVERY_MEMORY_SIZE_DEFAULT = StorageUtil.getSizeInBytes(64L,
            StorageUnit.MEGABYTE);

    public AsterixTransactionProperties(AsterixPropertiesAccessor accessor) {
        super(accessor);
    }

    public String getLogDirectory(String nodeId) {
        return accessor.getTransactionLogDirs().get(nodeId);
    }

    public Map<String, String> getLogDirectories() {
        return accessor.getTransactionLogDirs();
    }

    public int getLogBufferNumPages() {
        return accessor.getProperty(TXN_LOG_BUFFER_NUMPAGES_KEY, TXN_LOG_BUFFER_NUMPAGES_DEFAULT,
                PropertyInterpreters.getIntegerPropertyInterpreter());
    }

    public int getLogBufferPageSize() {
        return accessor.getProperty(TXN_LOG_BUFFER_PAGESIZE_KEY, TXN_LOG_BUFFER_PAGESIZE_DEFAULT,
                PropertyInterpreters.getIntegerBytePropertyInterpreter());
    }

    public long getLogPartitionSize() {
        return accessor.getProperty(TXN_LOG_PARTITIONSIZE_KEY, TXN_LOG_PARTITIONSIZE_DEFAULT,
                PropertyInterpreters.getLongBytePropertyInterpreter());
    }

    public int getCheckpointLSNThreshold() {
        return accessor.getProperty(TXN_LOG_CHECKPOINT_LSNTHRESHOLD_KEY, TXN_LOG_CHECKPOINT_LSNTHRESHOLD_DEFAULT,
                PropertyInterpreters.getIntegerPropertyInterpreter());
    }

    public int getCheckpointPollFrequency() {
        return accessor.getProperty(TXN_LOG_CHECKPOINT_POLLFREQUENCY_KEY, TXN_LOG_CHECKPOINT_POLLFREQUENCY_DEFAULT,
                PropertyInterpreters.getIntegerPropertyInterpreter());
    }

    public int getCheckpointHistory() {
        return accessor.getProperty(TXN_LOG_CHECKPOINT_HISTORY_KEY, TXN_LOG_CHECKPOINT_HISTORY_DEFAULT,
                PropertyInterpreters.getIntegerPropertyInterpreter());
    }

    public int getEntityToDatasetLockEscalationThreshold() {
        return accessor.getProperty(TXN_LOCK_ESCALATIONTHRESHOLD_KEY, TXN_LOCK_ESCALATIONTHRESHOLD_DEFAULT,
                PropertyInterpreters.getIntegerPropertyInterpreter());
    }

    public int getLockManagerShrinkTimer() {
        return accessor.getProperty(TXN_LOCK_SHRINKTIMER_KEY, TXN_LOCK_SHRINKTIMER_DEFAULT,
                PropertyInterpreters.getIntegerPropertyInterpreter());
    }

    public int getTimeoutWaitThreshold() {
        return accessor.getProperty(TXN_LOCK_TIMEOUT_WAITTHRESHOLD_KEY, TXN_LOCK_TIMEOUT_WAITTHRESHOLD_DEFAULT,
                PropertyInterpreters.getIntegerPropertyInterpreter());
    }

    public int getTimeoutSweepThreshold() {
        return accessor.getProperty(TXN_LOCK_TIMEOUT_SWEEPTHRESHOLD_KEY, TXN_LOCK_TIMEOUT_SWEEPTHRESHOLD_DEFAULT,
                PropertyInterpreters.getIntegerPropertyInterpreter());
    }

    public int getCommitProfilerReportInterval() {
        return accessor.getProperty(TXN_COMMIT_PROFILER_REPORT_INTERVAL_KEY,
                TXN_COMMIT_PROFILER_REPORT_INTERVAL_DEFAULT, PropertyInterpreters.getIntegerPropertyInterpreter());
    }

    public long getJobRecoveryMemorySize() {
        return accessor.getProperty(TXN_JOB_RECOVERY_MEMORY_SIZE_KEY, TXN_JOB_RECOVERY_MEMORY_SIZE_DEFAULT,
                PropertyInterpreters.getLongBytePropertyInterpreter());
    }
}
