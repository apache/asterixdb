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
package org.apache.asterix.app.replication;

import java.util.HashMap;
import java.util.Map;

import org.apache.asterix.common.replication.IFaultToleranceStrategy;
import org.apache.asterix.common.replication.IReplicationStrategy;
import org.apache.asterix.event.schema.cluster.Cluster;
import org.apache.hyracks.api.application.ICCServiceContext;

public class FaultToleranceStrategyFactory {

    private static final Map<String, Class<? extends IFaultToleranceStrategy>> BUILT_IN_FAULT_TOLERANCE_STRATEGY =
            new HashMap<>();

    static {
        BUILT_IN_FAULT_TOLERANCE_STRATEGY.put("no_fault_tolerance", NoFaultToleranceStrategy.class);
        BUILT_IN_FAULT_TOLERANCE_STRATEGY.put("metadata_node", MetadataNodeFaultToleranceStrategy.class);
        BUILT_IN_FAULT_TOLERANCE_STRATEGY.put("auto", AutoFaultToleranceStrategy.class);
    }

    private FaultToleranceStrategyFactory() {
        throw new AssertionError();
    }

    public static IFaultToleranceStrategy create(Cluster cluster, IReplicationStrategy repStrategy,
            ICCServiceContext serviceCtx) {
        boolean highAvailabilityEnabled =
                cluster.getHighAvailability() != null && cluster.getHighAvailability().getEnabled() != null
                        && Boolean.valueOf(cluster.getHighAvailability().getEnabled());

        if (!highAvailabilityEnabled || cluster.getHighAvailability().getFaultTolerance() == null
                || cluster.getHighAvailability().getFaultTolerance().getStrategy() == null) {
            return new NoFaultToleranceStrategy().from(serviceCtx, repStrategy);
        }
        String strategyName = cluster.getHighAvailability().getFaultTolerance().getStrategy().toLowerCase();
        if (!BUILT_IN_FAULT_TOLERANCE_STRATEGY.containsKey(strategyName)) {
            throw new IllegalArgumentException(String.format("Unsupported Replication Strategy. Available types: %s",
                    BUILT_IN_FAULT_TOLERANCE_STRATEGY.keySet().toString()));
        }
        Class<? extends IFaultToleranceStrategy> clazz = BUILT_IN_FAULT_TOLERANCE_STRATEGY.get(strategyName);
        try {
            return clazz.newInstance().from(serviceCtx, repStrategy);
        } catch (InstantiationException | IllegalAccessException e) {
            throw new IllegalStateException(e);
        }
    }
}