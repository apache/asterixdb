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

import static org.apache.hyracks.control.common.config.OptionTypes.*;
import static org.apache.hyracks.util.StorageUtil.StorageUnit.*;

import java.util.Map;

import org.apache.hyracks.api.config.IOption;
import org.apache.hyracks.api.config.IOptionType;
import org.apache.hyracks.api.config.Section;
import org.apache.hyracks.util.StorageUtil;

public class TransactionProperties extends AbstractProperties {

    public enum Option implements IOption {
        TXN_LOG_BUFFER_NUMPAGES(INTEGER, 8),
        TXN_LOG_BUFFER_PAGESIZE(INTEGER_BYTE_UNIT, StorageUtil.getIntSizeInBytes(128, KILOBYTE)),
        TXN_LOG_PARTITIONSIZE(LONG_BYTE_UNIT, StorageUtil.getLongSizeInBytes(256L, MEGABYTE)),
        TXN_LOG_CHECKPOINT_LSNTHRESHOLD(INTEGER_BYTE_UNIT, StorageUtil.getIntSizeInBytes(64, MEGABYTE)),
        TXN_LOG_CHECKPOINT_POLLFREQUENCY(INTEGER, 120),
        TXN_LOG_CHECKPOINT_HISTORY(INTEGER, 0),
        TXN_LOCK_ESCALATIONTHRESHOLD(INTEGER, 1000),
        TXN_LOCK_SHRINKTIMER(INTEGER, 5000),
        TXN_LOCK_TIMEOUT_WAITTHRESHOLD(INTEGER, 60000),
        TXN_LOCK_TIMEOUT_SWEEPTHRESHOLD(INTEGER, 10000),
        TXN_COMMITPROFILER_REPORTINTERVAL(INTEGER, 5),
        TXN_JOB_RECOVERY_MEMORYSIZE(LONG_BYTE_UNIT, StorageUtil.getLongSizeInBytes(64L, MEGABYTE));

        private final IOptionType type;
        private final Object defaultValue;

        Option(IOptionType type, Object defaultValue) {
            this.type = type;
            this.defaultValue = defaultValue;
        }

        @Override
        public Section section() {
            return Section.COMMON;
        }

        @Override
        public String description() {
            // TODO(mblow): add missing descriptions
            return null;
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

    public int getCommitProfilerReportInterval() {
        return accessor.getInt(Option.TXN_COMMITPROFILER_REPORTINTERVAL);
    }

    public long getJobRecoveryMemorySize() {
        return accessor.getLong(Option.TXN_JOB_RECOVERY_MEMORYSIZE);
    }
}
