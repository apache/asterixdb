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

import java.util.List;
import org.apache.hyracks.api.config.IApplicationConfig;
import org.apache.hyracks.api.config.IOption;
import org.apache.hyracks.api.config.IOptionType;
import org.apache.hyracks.api.config.Section;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.control.common.controllers.NCConfig;
import org.apache.hyracks.util.StorageUtil;
import org.apache.hyracks.util.StorageUtil.StorageUnit;

import static org.apache.hyracks.control.common.config.OptionTypes.*;

public class ReplicationProperties extends AbstractProperties {

    public enum Option implements IOption {
        REPLICATION_MAX_REMOTE_RECOVERY_ATTEMPTS(
                INTEGER,
                5,
                "The maximum number of times to attempt to recover from a replica on failure before giving up"),
        REPLICATION_LOG_BUFFER_PAGESIZE(
                INTEGER_BYTE_UNIT,
                StorageUtil.getIntSizeInBytes(128, StorageUnit.KILOBYTE),
                "The size in bytes of each log buffer page"),
        REPLICATION_LOG_BUFFER_NUMPAGES(INTEGER, 8, "The number of log buffer pages"),
        REPLICATION_LOG_BATCHSIZE(
                INTEGER_BYTE_UNIT,
                StorageUtil.getIntSizeInBytes(4, StorageUnit.KILOBYTE),
                "The size in bytes to replicate in each batch"),
        REPLICATION_TIMEOUT(
                INTEGER,
                REPLICATION_TIME_OUT_DEFAULT,
                "The time in seconds to timeout when trying to contact a replica, before assuming it is dead"),

        REPLICATION_ENABLED(BOOLEAN, false, "Whether or not data replication is enabled"),
        REPLICATION_FACTOR(INTEGER, 3, "Number of node controller faults to tolerate with replication"),
        REPLICATION_STRATEGY(STRING, "chained_declustering", "Replication strategy to choose"),
        REPLICATION_PORT(INTEGER, 2000, "port on which to run replication related communications"),;

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

        @Override
        public Object get(IApplicationConfig config) {
            switch (this) {
                case REPLICATION_TIMEOUT:
                    return REPLICATION_TIME_OUT_DEFAULT;
                default:
                    return config.getStatic(this);
            }
        }
    }

    public boolean isReplicationEnabled() {
        return accessor.getBoolean(Option.REPLICATION_ENABLED);
    }

    private static final int REPLICATION_TIME_OUT_DEFAULT = 15;

    public ReplicationProperties(PropertiesAccessor accessor) throws HyracksDataException {
        super(accessor);
    }

    public int getMaxRemoteRecoveryAttempts() {
        return accessor.getInt(Option.REPLICATION_MAX_REMOTE_RECOVERY_ATTEMPTS);
    }

    public int getReplicationFactor() {
        return accessor.getInt(Option.REPLICATION_FACTOR);
    }

    public List<String> getNodeIds() {
        return accessor.getNCNames();
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

    public String getNodeIpFromId(String id) {
        return accessor.getNCEffectiveConfig(id).getString(NCConfig.Option.PUBLIC_ADDRESS);
    }

    public String getReplicationStrategy() {
        return accessor.getString(Option.REPLICATION_STRATEGY);
    }

    public int getReplicationTimeOut() {
        return accessor.getInt(Option.REPLICATION_TIMEOUT);
    }

    public MetadataProperties getMetadataProperties() {
        return new MetadataProperties(accessor);
    }

}