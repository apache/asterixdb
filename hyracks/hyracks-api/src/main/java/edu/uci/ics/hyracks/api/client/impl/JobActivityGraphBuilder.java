/*
 * Copyright 2009-2013 by The Regents of the University of California
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * you may obtain a copy of the License from
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.uci.ics.hyracks.api.client.impl;

import java.util.ArrayList;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.commons.lang3.tuple.Pair;

import edu.uci.ics.hyracks.api.dataflow.ActivityId;
import edu.uci.ics.hyracks.api.dataflow.ConnectorDescriptorId;
import edu.uci.ics.hyracks.api.dataflow.IActivity;
import edu.uci.ics.hyracks.api.dataflow.IActivityGraphBuilder;
import edu.uci.ics.hyracks.api.dataflow.IConnectorDescriptor;
import edu.uci.ics.hyracks.api.dataflow.IOperatorDescriptor;
import edu.uci.ics.hyracks.api.job.JobActivityGraph;
import edu.uci.ics.hyracks.api.job.JobFlag;
import edu.uci.ics.hyracks.api.job.JobSpecification;

public class JobActivityGraphBuilder implements IActivityGraphBuilder {
    private static final Logger LOGGER = Logger.getLogger(JobActivityGraphBuilder.class.getName());

    private final Map<ActivityId, IOperatorDescriptor> activityOperatorMap;

    private final JobActivityGraph jag;

    private final JobSpecification jobSpec;

    private final Map<ConnectorDescriptorId, Pair<IActivity, Integer>> connectorProducerMap;

    private final Map<ConnectorDescriptorId, Pair<IActivity, Integer>> connectorConsumerMap;

    public JobActivityGraphBuilder(JobSpecification jobSpec, EnumSet<JobFlag> jobFlags) {
        activityOperatorMap = new HashMap<ActivityId, IOperatorDescriptor>();
        jag = new JobActivityGraph();
        this.jobSpec = jobSpec;
        connectorProducerMap = new HashMap<ConnectorDescriptorId, Pair<IActivity, Integer>>();
        connectorConsumerMap = new HashMap<ConnectorDescriptorId, Pair<IActivity, Integer>>();
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
        if (LOGGER.isLoggable(Level.FINEST)) {
            LOGGER.finest("Adding source edge: " + task.getActivityId() + ":" + operatorInputIndex + " -> "
                    + task.getActivityId() + ":" + taskInputIndex);
        }
        IOperatorDescriptor op = activityOperatorMap.get(task.getActivityId());
        IConnectorDescriptor conn = jobSpec.getInputConnectorDescriptor(op, operatorInputIndex);
        insertIntoIndexedMap(jag.getActivityInputMap(), task.getActivityId(), taskInputIndex, conn);
        connectorConsumerMap.put(conn.getConnectorId(), Pair.of(task, taskInputIndex));
    }

    @Override
    public void addTargetEdge(int operatorOutputIndex, IActivity task, int taskOutputIndex) {
        if (LOGGER.isLoggable(Level.FINEST)) {
            LOGGER.finest("Adding target edge: " + task.getActivityId() + ":" + operatorOutputIndex + " -> "
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
        Map<ConnectorDescriptorId, Pair<Pair<IActivity, Integer>, Pair<IActivity, Integer>>> caMap = jag
                .getConnectorActivityMap();
        for (Map.Entry<ConnectorDescriptorId, Pair<IActivity, Integer>> e : connectorProducerMap.entrySet()) {
            ConnectorDescriptorId cdId = e.getKey();
            Pair<IActivity, Integer> producer = e.getValue();
            Pair<IActivity, Integer> consumer = connectorConsumerMap.get(cdId);
            caMap.put(cdId, Pair.of(producer, consumer));
        }
    }

    private <K, V> void addToValueSet(Map<K, Set<V>> map, K n1, V n2) {
        Set<V> targets = map.get(n1);
        if (targets == null) {
            targets = new HashSet<V>();
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
            vList = new ArrayList<V>();
            map.put(key, vList);
        }
        extend(vList, index);
        vList.set(index, value);
    }

    public JobActivityGraph getActivityGraph() {
        return jag;
    }
}