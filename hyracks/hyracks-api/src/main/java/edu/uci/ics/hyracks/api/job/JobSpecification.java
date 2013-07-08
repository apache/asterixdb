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
package edu.uci.ics.hyracks.api.job;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang3.tuple.Pair;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import edu.uci.ics.hyracks.api.constraints.Constraint;
import edu.uci.ics.hyracks.api.dataflow.ConnectorDescriptorId;
import edu.uci.ics.hyracks.api.dataflow.IConnectorDescriptor;
import edu.uci.ics.hyracks.api.dataflow.IOperatorDescriptor;
import edu.uci.ics.hyracks.api.dataflow.OperatorDescriptorId;
import edu.uci.ics.hyracks.api.dataflow.connectors.IConnectorPolicyAssignmentPolicy;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.dataset.ResultSetId;

public class JobSpecification implements Serializable, IOperatorDescriptorRegistry, IConnectorDescriptorRegistry {
    private static final long serialVersionUID = 1L;

    private final List<OperatorDescriptorId> roots;

    private final List<ResultSetId> resultSetIds;

    private final Map<OperatorDescriptorId, IOperatorDescriptor> opMap;

    private final Map<ConnectorDescriptorId, IConnectorDescriptor> connMap;

    private final Map<OperatorDescriptorId, List<IConnectorDescriptor>> opInputMap;

    private final Map<OperatorDescriptorId, List<IConnectorDescriptor>> opOutputMap;

    private final Map<ConnectorDescriptorId, Pair<Pair<IOperatorDescriptor, Integer>, Pair<IOperatorDescriptor, Integer>>> connectorOpMap;

    private final Map<String, Serializable> properties;

    private final Set<Constraint> userConstraints;

    private IConnectorPolicyAssignmentPolicy connectorPolicyAssignmentPolicy;

    private int frameSize;

    private int maxReattempts;

    private IJobletEventListenerFactory jobletEventListenerFactory;

    private IGlobalJobDataFactory globalJobDataFactory;

    private boolean useConnectorPolicyForScheduling;

    private transient int operatorIdCounter;

    private transient int connectorIdCounter;

    public JobSpecification() {
        roots = new ArrayList<OperatorDescriptorId>();
        resultSetIds = new ArrayList<ResultSetId>();
        opMap = new HashMap<OperatorDescriptorId, IOperatorDescriptor>();
        connMap = new HashMap<ConnectorDescriptorId, IConnectorDescriptor>();
        opInputMap = new HashMap<OperatorDescriptorId, List<IConnectorDescriptor>>();
        opOutputMap = new HashMap<OperatorDescriptorId, List<IConnectorDescriptor>>();
        connectorOpMap = new HashMap<ConnectorDescriptorId, Pair<Pair<IOperatorDescriptor, Integer>, Pair<IOperatorDescriptor, Integer>>>();
        properties = new HashMap<String, Serializable>();
        userConstraints = new HashSet<Constraint>();
        operatorIdCounter = 0;
        connectorIdCounter = 0;
        frameSize = 32768;
        maxReattempts = 2;
        useConnectorPolicyForScheduling = true;
    }

    @Override
    public OperatorDescriptorId createOperatorDescriptorId(IOperatorDescriptor op) {
        OperatorDescriptorId odId = new OperatorDescriptorId(operatorIdCounter++);
        opMap.put(odId, op);
        return odId;
    }

    @Override
    public ConnectorDescriptorId createConnectorDescriptor(IConnectorDescriptor conn) {
        ConnectorDescriptorId cdId = new ConnectorDescriptorId(connectorIdCounter++);
        connMap.put(cdId, conn);
        return cdId;
    }

    public void addRoot(IOperatorDescriptor op) {
        roots.add(op.getOperatorId());
    }

    public void addResultSetId(ResultSetId rsId) {
        resultSetIds.add(rsId);
    }

    public void connect(IConnectorDescriptor conn, IOperatorDescriptor producerOp, int producerPort,
            IOperatorDescriptor consumerOp, int consumerPort) {
        insertIntoIndexedMap(opInputMap, consumerOp.getOperatorId(), consumerPort, conn);
        insertIntoIndexedMap(opOutputMap, producerOp.getOperatorId(), producerPort, conn);
        connectorOpMap.put(
                conn.getConnectorId(),
                Pair.<Pair<IOperatorDescriptor, Integer>, Pair<IOperatorDescriptor, Integer>> of(
                        Pair.<IOperatorDescriptor, Integer> of(producerOp, producerPort),
                        Pair.<IOperatorDescriptor, Integer> of(consumerOp, consumerPort)));
    }

    public void setProperty(String name, Serializable value) {
        properties.put(name, value);
    }

    public Serializable getProperty(String name) {
        return properties.get(name);
    }

    private <T> void extend(List<T> list, int index) {
        int n = list.size();
        for (int i = n; i <= index; ++i) {
            list.add(null);
        }
    }

    public Map<ConnectorDescriptorId, IConnectorDescriptor> getConnectorMap() {
        return connMap;
    }

    public Map<ConnectorDescriptorId, Pair<Pair<IOperatorDescriptor, Integer>, Pair<IOperatorDescriptor, Integer>>> getConnectorOperatorMap() {
        return connectorOpMap;
    }

    public RecordDescriptor getConnectorRecordDescriptor(IConnectorDescriptor conn) {
        Pair<Pair<IOperatorDescriptor, Integer>, Pair<IOperatorDescriptor, Integer>> connInfo = connectorOpMap.get(conn
                .getConnectorId());
        return connInfo.getLeft().getLeft().getOutputRecordDescriptors()[connInfo.getLeft().getRight()];
    }

    public IOperatorDescriptor getConsumer(IConnectorDescriptor conn) {
        Pair<Pair<IOperatorDescriptor, Integer>, Pair<IOperatorDescriptor, Integer>> connInfo = connectorOpMap.get(conn
                .getConnectorId());
        return connInfo.getRight().getLeft();
    }

    public int getConsumerInputIndex(IConnectorDescriptor conn) {
        Pair<Pair<IOperatorDescriptor, Integer>, Pair<IOperatorDescriptor, Integer>> connInfo = connectorOpMap.get(conn
                .getConnectorId());
        return connInfo.getRight().getRight();
    }

    public IConnectorDescriptor getInputConnectorDescriptor(IOperatorDescriptor op, int inputIndex) {
        return getInputConnectorDescriptor(op.getOperatorId(), inputIndex);
    }

    public IConnectorDescriptor getInputConnectorDescriptor(OperatorDescriptorId odId, int inputIndex) {
        return opInputMap.get(odId).get(inputIndex);
    }

    public Map<OperatorDescriptorId, List<IConnectorDescriptor>> getOperatorInputMap() {
        return opInputMap;
    }

    public RecordDescriptor getOperatorInputRecordDescriptor(OperatorDescriptorId odId, int inputIndex) {
        return getConnectorRecordDescriptor(getInputConnectorDescriptor(odId, inputIndex));
    }

    public Map<OperatorDescriptorId, IOperatorDescriptor> getOperatorMap() {
        return opMap;
    }

    public Map<OperatorDescriptorId, List<IConnectorDescriptor>> getOperatorOutputMap() {
        return opOutputMap;
    }

    public RecordDescriptor getOperatorOutputRecordDescriptor(OperatorDescriptorId odId, int outputIndex) {
        return getConnectorRecordDescriptor(getOutputConnectorDescriptor(odId, outputIndex));
    }

    public IConnectorDescriptor getOutputConnectorDescriptor(IOperatorDescriptor op, int outputIndex) {
        return getOutputConnectorDescriptor(op.getOperatorId(), outputIndex);
    }

    public IConnectorDescriptor getOutputConnectorDescriptor(OperatorDescriptorId odId, int outputIndex) {
        return opOutputMap.get(odId).get(outputIndex);
    }

    public IOperatorDescriptor getProducer(IConnectorDescriptor conn) {
        Pair<Pair<IOperatorDescriptor, Integer>, Pair<IOperatorDescriptor, Integer>> connInfo = connectorOpMap.get(conn
                .getConnectorId());
        return connInfo.getLeft().getLeft();
    }

    public int getProducerOutputIndex(IConnectorDescriptor conn) {
        Pair<Pair<IOperatorDescriptor, Integer>, Pair<IOperatorDescriptor, Integer>> connInfo = connectorOpMap.get(conn
                .getConnectorId());
        return connInfo.getLeft().getRight();
    }

    public List<OperatorDescriptorId> getRoots() {
        return roots;
    }

    public List<ResultSetId> getResultSetIds() {
        return resultSetIds;
    }

    public IConnectorPolicyAssignmentPolicy getConnectorPolicyAssignmentPolicy() {
        return connectorPolicyAssignmentPolicy;
    }

    public void setConnectorPolicyAssignmentPolicy(IConnectorPolicyAssignmentPolicy connectorPolicyAssignmentPolicy) {
        this.connectorPolicyAssignmentPolicy = connectorPolicyAssignmentPolicy;
    }

    public void setFrameSize(int frameSize) {
        this.frameSize = frameSize;
    }

    public int getFrameSize() {
        return frameSize;
    }

    public void setMaxReattempts(int maxReattempts) {
        this.maxReattempts = maxReattempts;
    }

    public int getMaxReattempts() {
        return maxReattempts;
    }

    public void addUserConstraint(Constraint constraint) {
        userConstraints.add(constraint);
    }

    public Set<Constraint> getUserConstraints() {
        return userConstraints;
    }

    public IJobletEventListenerFactory getJobletEventListenerFactory() {
        return jobletEventListenerFactory;
    }

    public void setJobletEventListenerFactory(IJobletEventListenerFactory jobletEventListenerFactory) {
        this.jobletEventListenerFactory = jobletEventListenerFactory;
    }

    public IGlobalJobDataFactory getGlobalJobDataFactory() {
        return globalJobDataFactory;
    }

    public void setGlobalJobDataFactory(IGlobalJobDataFactory globalJobDataFactory) {
        this.globalJobDataFactory = globalJobDataFactory;
    }

    public boolean isUseConnectorPolicyForScheduling() {
        return useConnectorPolicyForScheduling;
    }

    public void setUseConnectorPolicyForScheduling(boolean useConnectorPolicyForScheduling) {
        this.useConnectorPolicyForScheduling = useConnectorPolicyForScheduling;
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

    public String toString() {
        StringBuilder buffer = new StringBuilder();

        for (Map.Entry<OperatorDescriptorId, IOperatorDescriptor> e : opMap.entrySet()) {
            buffer.append(e.getKey().getId()).append(" : ").append(e.getValue().toString()).append("\n");
            List<IConnectorDescriptor> inputs = opInputMap.get(e.getKey());
            if (inputs != null && !inputs.isEmpty()) {
                buffer.append("   Inputs:\n");
                for (IConnectorDescriptor c : inputs) {
                    buffer.append("      ").append(c.getConnectorId().getId()).append(" : ").append(c.toString())
                            .append("\n");
                }
            }
            List<IConnectorDescriptor> outputs = opOutputMap.get(e.getKey());
            if (outputs != null && !outputs.isEmpty()) {
                buffer.append("   Outputs:\n");
                for (IConnectorDescriptor c : outputs) {
                    buffer.append("      ").append(c.getConnectorId().getId()).append(" : ").append(c.toString())
                            .append("\n");
                }
            }
        }

        buffer.append("\n").append("Constraints:\n").append(userConstraints);

        return buffer.toString();
    }

    public JSONObject toJSON() throws JSONException {
        JSONObject jjob = new JSONObject();

        JSONArray jopArray = new JSONArray();
        for (Map.Entry<OperatorDescriptorId, IOperatorDescriptor> e : opMap.entrySet()) {
            jopArray.put(e.getValue().toJSON());
        }
        jjob.put("operators", jopArray);

        JSONArray jcArray = new JSONArray();
        for (Map.Entry<ConnectorDescriptorId, IConnectorDescriptor> e : connMap.entrySet()) {
            JSONObject conn = new JSONObject();
            Pair<Pair<IOperatorDescriptor, Integer>, Pair<IOperatorDescriptor, Integer>> connection = connectorOpMap
                    .get(e.getKey());
            if (connection != null) {
                conn.put("in-operator-id", connection.getLeft().getLeft().getOperatorId().toString());
                conn.put("in-operator-port", connection.getLeft().getRight().intValue());
                conn.put("out-operator-id", connection.getRight().getLeft().getOperatorId().toString());
                conn.put("out-operator-port", connection.getRight().getRight().intValue());
            }
            conn.put("connector", e.getValue().toJSON());
            jcArray.put(conn);
        }
        jjob.put("connectors", jcArray);

        return jjob;
    }
}