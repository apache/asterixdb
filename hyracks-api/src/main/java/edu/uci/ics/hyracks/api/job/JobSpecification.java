/*
 * Copyright 2009-2010 by The Regents of the University of California
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
import java.util.List;
import java.util.Map;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import edu.uci.ics.hyracks.api.dataflow.ConnectorDescriptorId;
import edu.uci.ics.hyracks.api.dataflow.IConnectorDescriptor;
import edu.uci.ics.hyracks.api.dataflow.IOperatorDescriptor;
import edu.uci.ics.hyracks.api.dataflow.OperatorDescriptorId;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.util.Pair;

public class JobSpecification implements Serializable {
    private static final long serialVersionUID = 1L;

    private final List<OperatorDescriptorId> roots;

    private final Map<OperatorDescriptorId, IOperatorDescriptor> opMap;

    private final Map<ConnectorDescriptorId, IConnectorDescriptor> connMap;

    private final Map<OperatorDescriptorId, List<IConnectorDescriptor>> opInputMap;

    private final Map<OperatorDescriptorId, List<IConnectorDescriptor>> opOutputMap;

    private final Map<ConnectorDescriptorId, Pair<Pair<IOperatorDescriptor, Integer>, Pair<IOperatorDescriptor, Integer>>> connectorOpMap;

    private final Map<String, Serializable> properties;

    public JobSpecification() {
        roots = new ArrayList<OperatorDescriptorId>();
        opMap = new HashMap<OperatorDescriptorId, IOperatorDescriptor>();
        connMap = new HashMap<ConnectorDescriptorId, IConnectorDescriptor>();
        opInputMap = new HashMap<OperatorDescriptorId, List<IConnectorDescriptor>>();
        opOutputMap = new HashMap<OperatorDescriptorId, List<IConnectorDescriptor>>();
        connectorOpMap = new HashMap<ConnectorDescriptorId, Pair<Pair<IOperatorDescriptor, Integer>, Pair<IOperatorDescriptor, Integer>>>();
        properties = new HashMap<String, Serializable>();
    }

    public void addRoot(IOperatorDescriptor op) {
        roots.add(op.getOperatorId());
    }

    public void connect(IConnectorDescriptor conn, IOperatorDescriptor producerOp, int producerPort,
            IOperatorDescriptor consumerOp, int consumerPort) {
        insertIntoIndexedMap(opInputMap, consumerOp.getOperatorId(), consumerPort, conn);
        insertIntoIndexedMap(opOutputMap, producerOp.getOperatorId(), producerPort, conn);
        connectorOpMap.put(conn.getConnectorId(),
                new Pair<Pair<IOperatorDescriptor, Integer>, Pair<IOperatorDescriptor, Integer>>(
                        new Pair<IOperatorDescriptor, Integer>(producerOp, producerPort),
                        new Pair<IOperatorDescriptor, Integer>(consumerOp, consumerPort)));
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
        return connInfo.first.first.getOutputRecordDescriptors()[connInfo.first.second];
    }

    public IOperatorDescriptor getConsumer(IConnectorDescriptor conn) {
        Pair<Pair<IOperatorDescriptor, Integer>, Pair<IOperatorDescriptor, Integer>> connInfo = connectorOpMap.get(conn
                .getConnectorId());
        return connInfo.second.first;
    }

    public int getConsumerInputIndex(IConnectorDescriptor conn) {
        Pair<Pair<IOperatorDescriptor, Integer>, Pair<IOperatorDescriptor, Integer>> connInfo = connectorOpMap.get(conn
                .getConnectorId());
        return connInfo.second.second;
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
        return connInfo.first.first;
    }

    public int getProducerOutputIndex(IConnectorDescriptor conn) {
        Pair<Pair<IOperatorDescriptor, Integer>, Pair<IOperatorDescriptor, Integer>> connInfo = connectorOpMap.get(conn
                .getConnectorId());
        return connInfo.first.second;
    }

    public List<OperatorDescriptorId> getRoots() {
        return roots;
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
            IOperatorDescriptor op = e.getValue();
            buffer.append("   Partition Constraint: ").append(op.getPartitionConstraint()).append("\n");
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

        return buffer.toString();
    }

    public JSONObject toJSON() throws JSONException {
        JSONObject jjob = new JSONObject();

        jjob.put("type", "job");

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
            conn.put("type", "connector-info");
            if (connection != null) {
                conn.put("in-operator-id", connection.first.first.getOperatorId().toString());
                conn.put("in-operator-port", connection.first.second.intValue());
                conn.put("out-operator-id", connection.second.first.getOperatorId().toString());
                conn.put("out-operator-port", connection.second.second.intValue());
            }
            conn.put("connector", e.getValue().toJSON());
            jcArray.put(conn);
        }
        jjob.put("connectors", jcArray);

        return jjob;
    }
}