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

import static org.apache.hyracks.control.common.config.OptionTypes.BOOLEAN;
import static org.apache.hyracks.control.common.config.OptionTypes.INTEGER_BYTE_UNIT;
import static org.apache.hyracks.control.common.config.OptionTypes.LONG_BYTE_UNIT;
import static org.apache.hyracks.control.common.config.OptionTypes.POSITIVE_INTEGER;
import static org.apache.hyracks.control.common.config.OptionTypes.UNSIGNED_INTEGER;
import static org.apache.hyracks.util.StorageUtil.StorageUnit.MEGABYTE;

import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.hyracks.api.config.IOption;
import org.apache.hyracks.api.config.IOptionType;
import org.apache.hyracks.api.config.Section;
import org.apache.hyracks.util.StorageUtil;

public class TransactionProperties extends AbstractProperties {

    public enum Option implements IOption {
        TXN_DATASET_CHECKPOINT_INTERVAL(
                POSITIVE_INTEGER,
                (int) TimeUnit.MINUTES.toSeconds(60),
                "The interval (in seconds) after which a dataset is considered idle and persisted to disk"),
        TXN_LOG_BUFFER_NUMPAGES(POSITIVE_INTEGER, 8, "The number of pages in the transaction log tail"),
        TXN_LOG_BUFFER_PAGESIZE(
                INTEGER_BYTE_UNIT,
                StorageUtil.getIntSizeInBytes(4, MEGABYTE),
                "The page size (in bytes) for transaction log buffer"),
        TXN_LOG_PARTITIONSIZE(
                LONG_BYTE_UNIT,
                StorageUtil.getLongSizeInBytes(256L, MEGABYTE),
                "The maximum size (in bytes) of each transaction log file"),
        TXN_LOG_CHECKPOINT_LSNTHRESHOLD(
                INTEGER_BYTE_UNIT,
                StorageUtil.getIntSizeInBytes(64, MEGABYTE),
                "The checkpoint threshold (in terms of LSNs (log sequence numbers) that have been written to the "
                        + "transaction log, i.e., the length of the transaction log) for transaction logs"),
        TXN_LOG_CHECKPOINT_POLLFREQUENCY(
                POSITIVE_INTEGER,
                120,
                "The frequency (in seconds) the checkpoint thread should check to see if a checkpoint should be "
                        + "written"),
        TXN_LOG_CHECKPOINT_HISTORY(UNSIGNED_INTEGER, 2, "The number of checkpoints to keep in the transaction log"),
        TXN_LOCK_ESCALATIONTHRESHOLD(
                UNSIGNED_INTEGER,
                1000,
                "The maximum number of entity locks to obtain before upgrading to a dataset lock"),
        TXN_LOCK_SHRINKTIMER(
                POSITIVE_INTEGER,
                5000,
                "The time (in milliseconds) where under utilization of resources will trigger a shrink phase"),
        TXN_LOCK_TIMEOUT_WAITTHRESHOLD(POSITIVE_INTEGER, 60000, "Time out (in milliseconds) of waiting for a lock"),
        TXN_LOCK_TIMEOUT_SWEEPTHRESHOLD(
                POSITIVE_INTEGER,
                10000,
                "Interval (in milliseconds) for checking lock " + "timeout"),
        TXN_COMMITPROFILER_ENABLED(BOOLEAN, false, "Enable output of commit profiler logs"),
        TXN_COMMITPROFILER_REPORTINTERVAL(POSITIVE_INTEGER, 5, "Interval (in seconds) to report commit profiler logs"),
        TXN_JOB_RECOVERY_MEMORYSIZE(
                LONG_BYTE_UNIT,
                StorageUtil.getLongSizeInBytes(64L, MEGABYTE),
                "The memory budget (in bytes) used for recovery");

        private final IOptionType type;
        private final Object defaultValue;
        private final String description;

        Option(IOptionType type, Object defaultValue, String description) {
            this.type = type;
            this.defaultValue = defaultValue;
            this.description = description;
        }

        @Override
        public Section section() {
            return Section.COMMON;
        }

        @Override
        public String description() {
            return description;
        }

        @Override
        public IOptionType type() {
            return type;
        }

        @Override
        public Object defaultValue() {
            return defaultValue;
        }
    }

    public static final String TXN_LOG_PARTITIONSIZE_KEY = Option.TXN_LOG_PARTITIONSIZE.ini();

    public static final String TXN_LOG_CHECKPOINT_POLLFREQUENCY_KEY = Option.TXN_LOG_CHECKPOINT_POLLFREQUENCY.ini();

    public TransactionProperties(PropertiesAccessor accessor) {
        super(accessor);
    }

    public String getLogDirectory(String nodeId) {
        return accessor.getTransactionLogDirs().get(nodeId);
    }

    public Map<String, String> getLogDirectories() {
        return accessor.getTransactionLogDirs();
    }

    public int getLogBufferNumPages() {
        return accessor.getInt(Option.TXN_LOG_BUFFER_NUMPAGES);
    }

    public int getLogBufferPageSize() {
        return accessor.getInt(Option.TXN_LOG_BUFFER_PAGESIZE);
    }

    public long getLogPartitionSize() {
        return accessor.getLong(Option.TXN_LOG_PARTITIONSIZE);
    }

    public int getCheckpointLSNThreshold() {
        return accessor.getInt(Option.TXN_LOG_CHECKPOINT_LSNTHRESHOLD);
    }

    public int getCheckpointPollFrequency() {
        return accessor.getInt(Option.TXN_LOG_CHECKPOINT_POLLFREQUENCY);
    }

    public int getCheckpointHistory() {
        return accessor.getInt(Option.TXN_LOG_CHECKPOINT_HISTORY);
    }

    public int getEntityToDatasetLockEscalationThreshold() {
        return accessor.getInt(Option.TXN_LOCK_ESCALATIONTHRESHOLD);
    }

    public int getLockManagerShrinkTimer() {
        return accessor.getInt(Option.TXN_LOCK_SHRINKTIMER);
    }

    public int getTimeoutWaitThreshold() {
        return accessor.getInt(Option.TXN_LOCK_TIMEOUT_WAITTHRESHOLD);
    }

    public int getTimeoutSweepThreshold() {
        return accessor.getInt(Option.TXN_LOCK_TIMEOUT_SWEEPTHRESHOLD);
    }

    public boolean isCommitProfilerEnabled() {
        return accessor.getBoolean(Option.TXN_COMMITPROFILER_ENABLED);
    }

    public int getCommitProfilerReportInterval() {
        return accessor.getInt(Option.TXN_COMMITPROFILER_REPORTINTERVAL);
    }

    public long getJobRecoveryMemorySize() {
        return accessor.getLong(Option.TXN_JOB_RECOVERY_MEMORYSIZE);
    }

    public int getDatasetCheckpointInterval() {
        return accessor.getInt(Option.TXN_DATASET_CHECKPOINT_INTERVAL);
    }
}
