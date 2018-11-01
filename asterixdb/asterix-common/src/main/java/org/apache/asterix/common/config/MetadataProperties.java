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

import static org.apache.hyracks.control.common.config.OptionTypes.POSITIVE_INTEGER;
import static org.apache.hyracks.control.common.config.OptionTypes.STRING;
import static org.apache.hyracks.control.common.config.OptionTypes.UNSIGNED_INTEGER;

import java.util.List;
import java.util.Map;
import java.util.SortedMap;

import org.apache.asterix.common.cluster.ClusterPartition;
import org.apache.hyracks.api.config.IApplicationConfig;
import org.apache.hyracks.api.config.IOption;
import org.apache.hyracks.api.config.IOptionType;
import org.apache.hyracks.api.config.Section;

public class MetadataProperties extends AbstractProperties {

    public enum Option implements IOption {
        METADATA_NODE(STRING, null),
        METADATA_REGISTRATION_TIMEOUT_SECS(POSITIVE_INTEGER, 60),
        METADATA_LISTEN_PORT(UNSIGNED_INTEGER, 0),
        METADATA_CALLBACK_PORT(UNSIGNED_INTEGER, 0);

        private final IOptionType type;
        private final Object defaultValue;

        <T> Option(IOptionType<T> type, T defaultValue) {
            this.type = type;
            this.defaultValue = defaultValue;
        }

        @Override
        public Section section() {
            return Section.COMMON;
        }

        @Override
        public String description() {
            switch (this) {
                case METADATA_NODE:
                    return "the node which should serve as the metadata node";
                case METADATA_REGISTRATION_TIMEOUT_SECS:
                    return "how long in seconds to wait for the metadata node to register with the CC";
                case METADATA_LISTEN_PORT:
                    return "IP port to bind metadata listener (0 = random port)";
                case METADATA_CALLBACK_PORT:
                    return "IP port to bind metadata callback listener (0 = random port)";
                default:
                    throw new IllegalStateException("NYI: " + this);
            }
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
        public Object get(IApplicationConfig cfg) {
            if (this == METADATA_NODE) {
                Object value = cfg.getStatic(this);
                return value != null ? value : cfg.getNCNames().isEmpty() ? null : cfg.getNCNames().get(0);
            } else {
                return cfg.getStatic(this);
            }
        }

    }

    public MetadataProperties(PropertiesAccessor accessor) {
        super(accessor);
    }

    public String getMetadataNodeName() {
        return accessor.getString(Option.METADATA_NODE);
    }

    public Map<String, String[]> getStores() {
        return accessor.getStores();
    }

    public List<String> getNodeNames() {
        return accessor.getNCNames();
    }

    public String getCoredumpPath(String nodeId) {
        return accessor.getCoredumpPath(nodeId);
    }

    public Map<String, String> getCoredumpPaths() {
        return accessor.getCoredumpConfig();
    }

    public Map<String, ClusterPartition[]> getNodePartitions() {
        return accessor.getNodePartitions();
    }

    public SortedMap<Integer, ClusterPartition> getClusterPartitions() {
        return accessor.getClusterPartitions();
    }

    public Map<String, String> getTransactionLogDirs() {
        return accessor.getTransactionLogDirs();
    }

    public int getRegistrationTimeoutSecs() {
        return accessor.getInt(Option.METADATA_REGISTRATION_TIMEOUT_SECS);
    }

    public int getMetadataPort() {
        return accessor.getInt(Option.METADATA_LISTEN_PORT);
    }

    public int getMetadataCallbackPort() {
        return accessor.getInt(Option.METADATA_CALLBACK_PORT);
    }
}
