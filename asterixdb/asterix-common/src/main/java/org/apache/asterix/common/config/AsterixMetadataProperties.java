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
import java.util.Map;
import java.util.SortedMap;

import org.apache.asterix.common.cluster.ClusterPartition;

public class AsterixMetadataProperties extends AbstractAsterixProperties {

    private static final String METADATA_REGISTRATION_TIMEOUT_KEY = "metadata.registration.timeout.secs";
    private static final long METADATA_REGISTRATION_TIMEOUT_DEFAULT = 60;

    private static final String METADATA_PORT_KEY = "metadata.port";
    private static final int METADATA_PORT_DEFAULT = 0;

    private static final String METADATA_CALLBACK_PORT_KEY = "metadata.callback.port";
    private static final int METADATA_CALLBACK_PORT_DEFAULT = 0;

    public AsterixMetadataProperties(AsterixPropertiesAccessor accessor) {
        super(accessor);
    }

    @PropertyKey("instance.name")
    public String getInstanceName() {
        return accessor.getInstanceName();
    }

    @PropertyKey("metadata.node")
    public String getMetadataNodeName() {
        return accessor.getMetadataNodeName();
    }

    @PropertyKey("metadata.partition")
    public ClusterPartition getMetadataPartition() {
        return accessor.getMetadataPartition();
    }

    @PropertyKey("node.stores")
    public Map<String, String[]> getStores() {
        return accessor.getStores();
    }

    public List<String> getNodeNames() {
        return accessor.getNodeNames();
    }

    public String getCoredumpPath(String nodeId) {
        return accessor.getCoredumpPath(nodeId);
    }

    @PropertyKey("core.dump.paths")
    public Map<String, String> getCoredumpPaths() {
        return accessor.getCoredumpConfig();
    }

    @PropertyKey("node.partitions")
    public Map<String, ClusterPartition[]> getNodePartitions() {
        return accessor.getNodePartitions();
    }

    @PropertyKey("cluster.partitions")
    public SortedMap<Integer, ClusterPartition> getClusterPartitions() {
        return accessor.getClusterPartitions();
    }

    @PropertyKey("transaction.log.dirs")
    public Map<String, String> getTransactionLogDirs() {
        return accessor.getTransactionLogDirs();
    }

    @PropertyKey(METADATA_REGISTRATION_TIMEOUT_KEY)
    public long getRegistrationTimeoutSecs() {
        return accessor.getProperty(METADATA_REGISTRATION_TIMEOUT_KEY, METADATA_REGISTRATION_TIMEOUT_DEFAULT,
                PropertyInterpreters.getLongPropertyInterpreter());
    }

    @PropertyKey(METADATA_PORT_KEY)
    public int getMetadataPort() {
        return accessor.getProperty(METADATA_PORT_KEY, METADATA_PORT_DEFAULT,
                PropertyInterpreters.getIntegerPropertyInterpreter());
    }

    @PropertyKey(METADATA_CALLBACK_PORT_KEY)
    public int getMetadataCallbackPort() {
        return accessor.getProperty(METADATA_CALLBACK_PORT_KEY, METADATA_CALLBACK_PORT_DEFAULT,
                PropertyInterpreters.getIntegerPropertyInterpreter());
    }
}
