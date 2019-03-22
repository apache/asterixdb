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
import static org.apache.hyracks.control.common.config.OptionTypes.LONG;
import static org.apache.hyracks.control.common.config.OptionTypes.POSITIVE_INTEGER;
import static org.apache.hyracks.control.common.config.OptionTypes.STRING;
import static org.apache.hyracks.control.common.config.OptionTypes.UNSIGNED_INTEGER;

import java.util.concurrent.TimeUnit;

import org.apache.hyracks.api.config.IOption;
import org.apache.hyracks.api.config.IOptionType;
import org.apache.hyracks.api.config.Section;
import org.apache.hyracks.control.common.controllers.NCConfig;
import org.apache.hyracks.util.StorageUtil;
import org.apache.hyracks.util.StorageUtil.StorageUnit;

public class ReplicationProperties extends AbstractProperties {

    public enum Option implements IOption {
        REPLICATION_LOG_BUFFER_PAGESIZE(
                INTEGER_BYTE_UNIT,
                StorageUtil.getIntSizeInBytes(128, StorageUnit.KILOBYTE),
                "The size in bytes of each log buffer page"),
        REPLICATION_LOG_BUFFER_NUMPAGES(POSITIVE_INTEGER, 8, "The number of log buffer pages"),
        REPLICATION_LOG_BATCHSIZE(
                INTEGER_BYTE_UNIT,
                StorageUtil.getIntSizeInBytes(4, StorageUnit.KILOBYTE),
                "The size in bytes to replicate in each batch"),
        REPLICATION_TIMEOUT(
                LONG,
                TimeUnit.SECONDS.toSeconds(30),
                "The time in seconds to timeout waiting for master or replica to ack"),
        REPLICATION_ENABLED(BOOLEAN, false, "Whether or not data replication is enabled"),
        REPLICATION_FACTOR(UNSIGNED_INTEGER, 2, "Number of replicas (backups) to maintain per master replica"),
        REPLICATION_STRATEGY(STRING, "none", "Replication strategy to choose");

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

    public boolean isReplicationEnabled() {
        return accessor.getBoolean(Option.REPLICATION_ENABLED);
    }

    public ReplicationProperties(PropertiesAccessor accessor) {
        super(accessor);
    }

    public int getLogBufferPageSize() {
        return accessor.getInt(Option.REPLICATION_LOG_BUFFER_PAGESIZE);
    }

    public int getLogBufferNumOfPages() {
        return accessor.getInt(Option.REPLICATION_LOG_BUFFER_NUMPAGES);
    }

    public int getLogBatchSize() {
        return accessor.getInt(Option.REPLICATION_LOG_BATCHSIZE);
    }

    public String getReplicationAddress() {
        return accessor.getString(NCConfig.Option.REPLICATION_LISTEN_ADDRESS);
    }

    public int getReplicationPort() {
        return accessor.getInt(NCConfig.Option.REPLICATION_LISTEN_PORT);
    }

    public String getReplicationStrategy() {
        return accessor.getString(Option.REPLICATION_STRATEGY);
    }

    public long getReplicationTimeOut() {
        return accessor.getLong(Option.REPLICATION_TIMEOUT);
    }

    public int getReplicationFactor() {
        return accessor.getInt(Option.REPLICATION_FACTOR);
    }
}
