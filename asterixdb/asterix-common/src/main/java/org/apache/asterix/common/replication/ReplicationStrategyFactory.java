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
package org.apache.asterix.common.replication;

import java.util.HashMap;
import java.util.Map;

import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.common.exceptions.RuntimeDataException;
import org.apache.asterix.event.schema.cluster.Cluster;
import org.apache.hyracks.api.exceptions.HyracksDataException;

public class ReplicationStrategyFactory {

    private static final Map<String, Class<? extends IReplicationStrategy>>
    BUILT_IN_REPLICATION_STRATEGY = new HashMap<>();

    static {
        BUILT_IN_REPLICATION_STRATEGY.put("no_replication", NoReplicationStrategy.class);
        BUILT_IN_REPLICATION_STRATEGY.put("chained_declustering", ChainedDeclusteringReplicationStrategy.class);
        BUILT_IN_REPLICATION_STRATEGY.put("metadata_only", MetadataOnlyReplicationStrategy.class);
    }

    private ReplicationStrategyFactory() {
        throw new AssertionError();
    }

    public static IReplicationStrategy create(Cluster cluster) throws HyracksDataException {
        boolean highAvailabilityEnabled = cluster.getHighAvailability() != null
                && cluster.getHighAvailability().getEnabled() != null
                && Boolean.valueOf(cluster.getHighAvailability().getEnabled());

        if (!highAvailabilityEnabled || cluster.getHighAvailability().getDataReplication() == null
                || cluster.getHighAvailability().getDataReplication().getStrategy() == null) {
            return new NoReplicationStrategy();
        }
        String strategyName = cluster.getHighAvailability().getDataReplication().getStrategy().toLowerCase();
        if (!BUILT_IN_REPLICATION_STRATEGY.containsKey(strategyName)) {
            throw new RuntimeDataException(ErrorCode.UNSUPPORTED_REPLICATION_STRATEGY,
                    String.format("%s. Available strategies: %s", strategyName,
                            BUILT_IN_REPLICATION_STRATEGY.keySet().toString()));
        }
        Class<? extends IReplicationStrategy> clazz = BUILT_IN_REPLICATION_STRATEGY.get(strategyName);
        try {
            return clazz.newInstance().from(cluster);
        } catch (InstantiationException | IllegalAccessException e) {
            throw new RuntimeDataException(ErrorCode.INSTANTIATION_ERROR, e, clazz.getName());
        }
    }
}