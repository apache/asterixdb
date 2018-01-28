/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 */
package org.apache.hyracks.algebricks.core.utils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class DotFormatBuilder {
    private final StringBuilder stringBuilder;
    private final Set<Node> nodes;
    private final List<Edge> edges;

    public DotFormatBuilder(StringValue graphName) {
        this.edges = new ArrayList<>();
        this.nodes = new HashSet<>();
        this.stringBuilder = new StringBuilder();
        this.stringBuilder.append("digraph ").append(graphName).append(" {\n").append("rankdir=BT;\n");
        this.stringBuilder.append("node [style=\"rounded,filled\",shape=box];\n");
    }

    public String getDotDocument() {
        // print edges first
        for (Edge edge : edges) {
            stringBuilder.append(edge);
        }
        // print nodes
        for (Node node : nodes) {
            stringBuilder.append(node);
        }
        stringBuilder.append("\n}");
        return stringBuilder.toString();
    }

    // point of entry method
    public Node createNode(StringValue nodeId, StringValue nodeLabel) {
        Node node = new Node(nodeId, nodeLabel);
        for (Node existingNode : nodes) {
            if (node.equals(existingNode)) {
                existingNode.setNodeLabel(nodeLabel);
                return existingNode;
            }
        }
        nodes.add(node);
        return node;
    }

    // point of entry method
    public Edge createEdge(final Node source, final Node destination) {
        // sanity checks if any variable is null?
        if (source == null || destination == null || !nodes.contains(source) || !nodes.contains(destination)) {
            return null;
        }

        // append to edges list
        Edge newEdge = new Edge(source, destination);
        edges.add(newEdge);

        return newEdge;
    }

    public class Node {
        private final StringValue nodeId;
        private HashMap<String, AttributeValue> attributes = new HashMap<>();

        // no instantiation
        private Node(StringValue nodeId, StringValue nodeLabel) {
            this.nodeId = nodeId;
            setNodeLabel(nodeLabel);
        }

        public StringValue getNodeId() {
            return nodeId;
        }

        public AttributeValue getNodeLabel() {
            return attributes.get(Attribute.LABEL);
        }

        public Node setNodeLabel(StringValue nodeLabel) {
            if (nodeLabel != null) {
                attributes.put(Attribute.LABEL, nodeLabel);
            }
            return this;
        }

        public Node setFillColor(Color color) {
            if (color != null) {
                attributes.put(Attribute.COLOR, color);
            }
            return this;
        }

        @Override
        public boolean equals(Object other) {
            if (!(other instanceof Node)) {
                return false;
            }
            Node otherNode = (Node) other;

            return nodeId.getValue().equals(otherNode.nodeId.getValue());
        }

        @Override
        public int hashCode() {
            return nodeId.getValue().hashCode();
        }

        @Override
        public String toString() {
            StringBuilder nodeString = new StringBuilder();
            nodeString.append(nodeId).append(" [");
            attributes.forEach((key, value) -> nodeString.append(key).append("=").append(value).append(","));
            // remove last ","
            if (nodeString.charAt(nodeString.length() - 1) == ',') {
                nodeString.deleteCharAt(nodeString.length() - 1);
            }
            nodeString.append("];\n");

            return nodeString.toString();
        }
    }

    public class Edge {
        private final Node source;
        private final Node destination;
        private final HashMap<String, AttributeValue> attributes = new HashMap<>();

        // no instantiation
        private Edge(Node source, Node destination) {
            this.source = source;
            this.destination = destination;
        }

        public Edge setLabel(StringValue edgeLabel) {
            if (edgeLabel != null) {
                attributes.put(Attribute.LABEL, edgeLabel);
            }
            return this;
        }

        public Edge setColor(Color color) {
            if (color != null) {
                attributes.put(Attribute.COLOR, color);
            }
            return this;
        }

        @Override
        public boolean equals(Object other) {
            if (!(other instanceof Edge)) {
                return false;
            }
            Edge otherEdge = (Edge) other;

            return source.equals(otherEdge.source) && destination.equals(otherEdge.destination);
        }

        @Override
        public int hashCode() {
            return source.hashCode() ^ destination.hashCode();
        }

        @Override
        public String toString() {
            StringBuilder edgeString = new StringBuilder();
            edgeString.append(source.getNodeId()).append("->").append(destination.getNodeId()).append(" [");
            attributes.forEach((key, value) -> edgeString.append(key).append("=").append(value).append(","));
            // remove last ","
            if (edgeString.charAt(edgeString.length() - 1) == ',') {
                edgeString.deleteCharAt(edgeString.length() - 1);
            }
            edgeString.append("];\n");

            return edgeString.toString();
        }
    }

    public abstract static class AttributeValue {
        private final String value;

        // no instantiation
        private AttributeValue(String value) {
            this.value = value;
        }

        public String getValue() {
            return value;
        }

        @Override
        public String toString() {
            return value;
        }
    }

    public static final class StringValue extends AttributeValue {
        // no instantiation
        private StringValue(String value) {
            super(value);
        }

        public static StringValue of(String value) {
            String newValue = value;
            if (value == null) {
                newValue = "";
            }
            newValue = newValue.replace("\n", "\\n");
            return new StringValue("\"" + newValue.replace("\"", "\'").trim() + "\"");
        }
    }

    public static final class Color extends AttributeValue {
        public static final Color RED = new Color("red");
        public static final Color SKYBLUE = new Color("skyblue");

        // no instantiation
        private Color(String color) {
            super(color);
        }
    }

    private class Attribute {
        private static final String COLOR = "color";
        private static final String LABEL = "label";

        // no instantiation
        private Attribute() {
        }
    }
}
