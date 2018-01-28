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
package org.apache.hyracks.api.client.impl;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.hyracks.api.dataflow.ActivityId;
import org.apache.hyracks.api.dataflow.ConnectorDescriptorId;
import org.apache.hyracks.api.dataflow.IActivity;
import org.apache.hyracks.api.dataflow.IActivityGraphBuilder;
import org.apache.hyracks.api.dataflow.IConnectorDescriptor;
import org.apache.hyracks.api.dataflow.IOperatorDescriptor;
import org.apache.hyracks.api.job.JobActivityGraph;
import org.apache.hyracks.api.job.JobFlag;
import org.apache.hyracks.api.job.JobSpecification;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class JobActivityGraphBuilder implements IActivityGraphBuilder {
    private static final Logger LOGGER = LogManager.getLogger();

    private final Map<ActivityId, IOperatorDescriptor> activityOperatorMap;

    private final JobActivityGraph jag;

    private final JobSpecification jobSpec;

    private final Map<ConnectorDescriptorId, Pair<IActivity, Integer>> connectorProducerMap;

    private final Map<ConnectorDescriptorId, Pair<IActivity, Integer>> connectorConsumerMap;

    public JobActivityGraphBuilder(JobSpecification jobSpec, Set<JobFlag> jobFlags) {
        activityOperatorMap = new HashMap<>();
        jag = new JobActivityGraph();
        this.jobSpec = jobSpec;
        connectorProducerMap = new HashMap<>();
        connectorConsumerMap = new HashMap<>();
    }

    public void addConnector(IConnectorDescriptor conn) {
        jag.getConnectorMap().put(conn.getConnectorId(), conn);
        jag.getConnectorRecordDescriptorMap().put(conn.getConnectorId(), jobSpec.getConnectorRecordDescriptor(conn));
    }

    @Override
    public void addBlockingEdge(IActivity blocker, IActivity blocked) {
        addToValueSet(jag.getBlocked2BlockerMap(), blocked.getActivityId(), blocker.getActivityId());
    }

    @Override
    public void addSourceEdge(int operatorInputIndex, IActivity task, int taskInputIndex) {
        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace("Adding source edge: " + task.getActivityId() + ":" + operatorInputIndex + " -> "
                    + task.getActivityId() + ":" + taskInputIndex);
        }
        IOperatorDescriptor op = activityOperatorMap.get(task.getActivityId());
        IConnectorDescriptor conn = jobSpec.getInputConnectorDescriptor(op, operatorInputIndex);
        insertIntoIndexedMap(jag.getActivityInputMap(), task.getActivityId(), taskInputIndex, conn);
        connectorConsumerMap.put(conn.getConnectorId(), Pair.of(task, taskInputIndex));
    }

    @Override
    public void addTargetEdge(int operatorOutputIndex, IActivity task, int taskOutputIndex) {
        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace("Adding target edge: " + task.getActivityId() + ":" + operatorOutputIndex + " -> "
                    + task.getActivityId() + ":" + taskOutputIndex);
        }
        IOperatorDescriptor op = activityOperatorMap.get(task.getActivityId());
        IConnectorDescriptor conn = jobSpec.getOutputConnectorDescriptor(op, operatorOutputIndex);
        insertIntoIndexedMap(jag.getActivityOutputMap(), task.getActivityId(), taskOutputIndex, conn);
        connectorProducerMap.put(conn.getConnectorId(), Pair.of(task, taskOutputIndex));
    }

    @Override
    public void addActivity(IOperatorDescriptor op, IActivity task) {
        activityOperatorMap.put(task.getActivityId(), op);
        ActivityId activityId = task.getActivityId();
        jag.getActivityMap().put(activityId, task);
    }

    public void finish() {
        Map<ConnectorDescriptorId, Pair<Pair<IActivity, Integer>, Pair<IActivity, Integer>>> caMap =
                jag.getConnectorActivityMap();
        connectorProducerMap
                .forEach((cdId, producer) -> caMap.put(cdId, Pair.of(producer, connectorConsumerMap.get(cdId))));
    }

    private <K, V> void addToValueSet(Map<K, Set<V>> map, K n1, V n2) {
        Set<V> targets = map.get(n1);
        if (targets == null) {
            targets = new HashSet<>();
            map.put(n1, targets);
        }
        targets.add(n2);
    }

    private <T> void extend(List<T> list, int index) {
        int n = list.size();
        for (int i = n; i <= index; ++i) {
            list.add(null);
        }
    }

    private <K, V> void insertIntoIndexedMap(Map<K, List<V>> map, K key, int index, V value) {
        List<V> vList = map.get(key);
        if (vList == null) {
            vList = new ArrayList<>();
            map.put(key, vList);
        }
        extend(vList, index);
        vList.set(index, value);
    }

    public JobActivityGraph getActivityGraph() {
        return jag;
    }
}
