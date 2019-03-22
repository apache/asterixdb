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
package org.apache.hyracks.api.topology;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Stack;

import org.apache.hyracks.api.topology.NetworkEndpoint.EndpointType;
import org.xml.sax.Attributes;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;
import org.xml.sax.XMLReader;
import org.xml.sax.helpers.DefaultHandler;
import org.xml.sax.helpers.XMLReaderFactory;

public class TopologyDefinitionParser {
    private final Stack<ElementStackEntry> stack;

    private boolean inPropertyElement;

    private TopologyDefinitionParser() {
        stack = new Stack<ElementStackEntry>();
        inPropertyElement = false;
    }

    public static ClusterTopology parse(InputSource in) throws IOException, SAXException {
        TopologyDefinitionParser parser = new TopologyDefinitionParser();
        return parser.parseInternal(in);
    }

    private ClusterTopology parseInternal(InputSource in) throws IOException, SAXException {
        XMLReader parser;
        parser = XMLReaderFactory.createXMLReader();
        SAXContentHandler handler = new SAXContentHandler();
        parser.setContentHandler(handler);
        parser.parse(in);
        if (stack.size() != 1) {
            throw new IllegalStateException("Malformed topology definition");
        }
        ElementStackEntry e = stack.pop();
        if (e.ports.size() != 1) {
            throw new IllegalArgumentException("Malformed topology definition");
        }
        NetworkEndpoint endpoint = e.ports.get(0).getEndpoint();
        if (endpoint.getType() != EndpointType.NETWORK_SWITCH) {
            throw new IllegalArgumentException("Top level content in cluster-topology must be network-switch");
        }
        return new ClusterTopology((NetworkSwitch) endpoint);
    }

    private class SAXContentHandler extends DefaultHandler {
        @Override
        public void endElement(String uri, String localName, String qName) throws SAXException {
            if ("network-switch".equals(localName) || "terminal".equals(localName)) {
                ElementStackEntry e = stack.pop();
                NetworkEndpoint endpoint = e.type == EndpointType.NETWORK_SWITCH
                        ? new NetworkSwitch(e.name, e.properties,
                                e.ports.toArray(new NetworkSwitch.Port[e.ports.size()]))
                        : new NetworkTerminal(e.name, e.properties);
                stack.peek().ports.add(new NetworkSwitch.Port(endpoint));
            } else if ("property".equals(localName)) {
                if (!inPropertyElement) {
                    throw new IllegalStateException("Improperly nested property element encountered");
                }
                inPropertyElement = false;
            }
        }

        @Override
        public void startElement(String uri, String localName, String qName, Attributes atts) throws SAXException {
            if ("cluster-topology".equals(localName)) {
                if (!stack.isEmpty()) {
                    throw new IllegalStateException("Encountered unexpected " + qName);
                }
                stack.push(new ElementStackEntry(null, ""));
            } else if ("network-switch".equals(localName) || "terminal".equals(localName)) {
                String name = atts.getValue("", "name");
                if (name == null) {
                    throw new IllegalStateException("Encountered " + localName + " element with no name attribute");
                }
                EndpointType type = "network-switch".equals(localName) ? EndpointType.NETWORK_SWITCH
                        : EndpointType.NETWORK_TERMINAL;
                ElementStackEntry e = new ElementStackEntry(type, name);
                stack.push(e);
            } else if ("property".equals(localName)) {
                if (inPropertyElement) {
                    throw new IllegalStateException("Improperly nested property element encountered");
                }
                String name = atts.getValue("", "name");
                if (name == null) {
                    throw new IllegalStateException("Encountered " + localName + " element with no name attribute");
                }
                String value = atts.getValue("", "value");
                if (value == null) {
                    throw new IllegalStateException("Encountered " + localName + " element with no value attribute");
                }
                stack.peek().properties.put(name, value);
                inPropertyElement = true;
            }
        }
    }

    private static class ElementStackEntry {
        private final EndpointType type;

        private final String name;

        private final Map<String, String> properties;

        private final List<NetworkSwitch.Port> ports;

        public ElementStackEntry(EndpointType type, String name) {
            this.type = type;
            this.name = name;
            this.properties = new HashMap<String, String>();
            ports = new ArrayList<NetworkSwitch.Port>();
        }
    }
}
