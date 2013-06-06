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
package edu.uci.ics.hyracks.api.topology;

import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class NetworkSwitch extends NetworkEndpoint {
    private static final long serialVersionUID = 1L;

    private final Port[] ports;

    private final Map<String, Integer> terminalNamePortIndexMap;

    public NetworkSwitch(String name, Map<String, String> properties, Port[] ports) {
        super(name, properties);
        this.ports = ports;
        terminalNamePortIndexMap = new HashMap<String, Integer>();
        for (int i = 0; i < ports.length; ++i) {
            Port port = ports[i];
            NetworkEndpoint endpoint = port.getEndpoint();
            Integer portIndex = Integer.valueOf(i);
            switch (endpoint.getType()) {
                case NETWORK_SWITCH: {
                    NetworkSwitch s = (NetworkSwitch) endpoint;
                    for (String t : s.terminalNamePortIndexMap.keySet()) {
                        terminalNamePortIndexMap.put(t, portIndex);
                    }
                    break;
                }

                case NETWORK_TERMINAL: {
                    NetworkTerminal t = (NetworkTerminal) endpoint;
                    terminalNamePortIndexMap.put(t.getName(), portIndex);
                    break;
                }
            }
        }
    }

    public Port[] getPorts() {
        return ports;
    }

    @Override
    public EndpointType getType() {
        return EndpointType.NETWORK_SWITCH;
    }

    boolean lookupNetworkTerminal(String terminalName, List<Integer> path) {
        if (terminalNamePortIndexMap.containsKey(terminalName)) {
            Integer portIndex = terminalNamePortIndexMap.get(terminalName);
            path.add(portIndex);
            NetworkEndpoint endpoint = ports[portIndex.intValue()].getEndpoint();
            if (endpoint.getType() == EndpointType.NETWORK_SWITCH) {
                ((NetworkSwitch) endpoint).lookupNetworkTerminal(terminalName, path);
            }
            return true;
        }
        return false;
    }

    void getPortList(List<Integer> path, int stepIndex, List<Port> portList) {
        if (stepIndex >= path.size()) {
            return;
        }
        int portIndex = path.get(stepIndex);
        Port port = ports[portIndex];
        portList.add(port);
        ++stepIndex;
        if (stepIndex >= path.size()) {
            return;
        }
        NetworkEndpoint endpoint = port.getEndpoint();
        if (endpoint.getType() != EndpointType.NETWORK_SWITCH) {
            throw new IllegalArgumentException("Path provided, " + path + ", longer than depth of topology tree");
        }
        ((NetworkSwitch) endpoint).getPortList(path, stepIndex, portList);
    }

    public static class Port implements Serializable {
        private static final long serialVersionUID = 1L;

        private final NetworkEndpoint endpoint;

        public Port(NetworkEndpoint endpoint) {
            this.endpoint = endpoint;
        }

        public NetworkEndpoint getEndpoint() {
            return endpoint;
        }
    }
}