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
package org.apache.hyracks.util;

import java.io.IOException;
import java.io.Writer;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.DoubleStream;
import java.util.stream.IntStream;
import java.util.stream.LongStream;
import java.util.stream.Stream;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

public class JSONUtil {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private static final ObjectMapper SORTED_MAPPER = new ObjectMapper();

    private JSONUtil() {
    }

    static {
        SORTED_MAPPER.configure(SerializationFeature.ORDER_MAP_ENTRIES_BY_KEYS, true);
        SORTED_MAPPER.configure(MapperFeature.SORT_PROPERTIES_ALPHABETICALLY, true);
    }

    public static String convertNode(final JsonNode node) throws JsonProcessingException {
        return SORTED_MAPPER.writeValueAsString(SORTED_MAPPER.treeToValue(node, Object.class));
    }

    public static String convertNodeOrThrow(final JsonNode node) {
        try {
            return convertNode(node);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException(e);
        }
    }

    public static void writeNode(final Writer writer, final JsonNode node) throws IOException {
        SORTED_MAPPER.writeValue(writer, SORTED_MAPPER.treeToValue(node, Object.class));
    }

    public static String quoteAndEscape(String str) {
        return quoteAndEscape(new StringBuilder(), str).toString();
    }

    public static StringBuilder quoteAndEscape(StringBuilder sb, String str) {
        return escape(sb.append('"'), str).append('"');
    }

    public static String escape(String str) {
        return escape(new StringBuilder(), str).toString();
    }

    public static StringBuilder escape(StringBuilder sb, String str) {
        for (int i = 0; i < str.length(); ++i) {
            appendEsc(sb, str.charAt(i));
        }
        return sb;
    }

    private static StringBuilder appendEsc(StringBuilder sb, char c) {
        CharSequence cs = esc(c);
        return cs != null ? sb.append(cs) : sb.append(c);
    }

    public static CharSequence esc(char c) {
        switch (c) {
            case '"':
                return "\\\"";
            case '\\':
                return "\\\\";
            case '/':
                return "\\/";
            case '\b':
                return "\\b";
            case '\n':
                return "\\n";
            case '\f':
                return "\\f";
            case '\r':
                return "\\r";
            case '\t':
                return "\\t";
            default:
                return null;
        }
    }

    /**
     * Write map as a json string. if an object is a string and starts with a { or [
     * then it assumes that it is a json object or a json array and so it doesn't surround
     * it with "
     *
     * @param map
     *            a map representing the json object
     * @return
     *         a String representation of the json object
     */
    public static String fromMap(Map<String, Object> map) {
        StringBuilder aString = new StringBuilder();
        aString.append("{ ");
        boolean first = true;
        for (Entry<String, Object> entry : map.entrySet()) {
            if (!first) {
                aString.append(", ");
            }
            aString.append("\"");
            aString.append(entry.getKey());
            aString.append("\"");
            aString.append(" : ");
            Object value = entry.getValue();
            if (value instanceof String) {
                String strValue = (String) value;
                if (strValue.startsWith("{") || strValue.startsWith("[")) {
                    aString.append(value);
                } else {
                    aString.append("\"");
                    aString.append(value);
                    aString.append("\"");
                }
            } else {
                aString.append(value);
            }
            first = false;
        }
        aString.append(" }");
        return aString.toString();
    }

    public static void put(ObjectNode o, String name, int value) {
        o.put(name, value);
    }

    public static void put(ObjectNode o, String name, String value) {
        o.put(name, value);
    }

    public static void put(ObjectNode o, String name, long value) {
        o.put(name, value);
    }

    public static void put(ObjectNode o, String name, double value) {
        o.put(name, value);
    }

    public static void put(ObjectNode o, String name, long[] elements) {
        LongStream.of(elements).forEachOrdered(o.putArray(name)::add);
    }

    public static void put(ObjectNode o, String name, long[][] elements) {
        Stream.of(elements).forEachOrdered(o.putArray(name)::addPOJO);
    }

    public static void put(ObjectNode o, String name, int[] elements) {
        IntStream.of(elements).forEachOrdered(o.putArray(name)::add);
    }

    public static void put(ObjectNode o, String name, double[] elements) {
        DoubleStream.of(elements).forEachOrdered(o.putArray(name)::add);
    }

    public static void put(ObjectNode o, String name, Map<String, String> map) {
        map.forEach(o.putObject(name)::put);
    }

    public static void put(ObjectNode o, String name, String[] elements) {
        Stream.of(elements).forEachOrdered(o.putArray(name)::add);
    }

    public static void put(ObjectNode o, String name, List<String> elements) {
        elements.forEach(o.putArray(name)::add);
    }

    public static void putArrayOrScalar(ObjectNode o, String name, List<String> elements) {
        switch (elements.size()) {
            case 0:
                o.putNull(name);
                break;
            case 1:
                o.put(name, elements.get(0));
                break;
            default:
                ArrayNode arrayNode = o.putArray(name);
                for (String item : elements) {
                    arrayNode.add(item);
                }
                break;
        }
    }

    public static ObjectNode createObject() {
        return OBJECT_MAPPER.createObjectNode();
    }

    public static ArrayNode createArray() {
        return OBJECT_MAPPER.createArrayNode();
    }
}
