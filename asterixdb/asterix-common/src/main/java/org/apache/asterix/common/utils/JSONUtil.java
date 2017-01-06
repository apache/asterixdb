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
package org.apache.asterix.common.utils;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;

import java.io.IOException;
import java.util.Iterator;
import java.util.logging.Logger;

public class JSONUtil {

    private static final Logger LOGGER = Logger.getLogger(JSONUtil.class.getName());

    private static final String INDENT = "\t";

    private static final ObjectMapper SORTED_MAPPER = new ObjectMapper();

    private JSONUtil() {
    }

    static {
        SORTED_MAPPER.configure(SerializationFeature.ORDER_MAP_ENTRIES_BY_KEYS, true);
    }

    public static String convertNode(final JsonNode node) throws JsonProcessingException {
        final Object obj = SORTED_MAPPER.treeToValue(node, Object.class);
        final String json = SORTED_MAPPER.writerWithDefaultPrettyPrinter().writeValueAsString(obj);
        return json;
    }

    public static String indent(String str, int initialIndent) {
        ObjectMapper om = new ObjectMapper();
        try {
            return appendObj(new StringBuilder(), om.readTree(str), initialIndent).toString();
        } catch (IOException e) {
            LOGGER.finest(String.valueOf(e));
            LOGGER.finest("Could not indent JSON string, returning the input string: " + str);
            return str;
        }
    }

    private static StringBuilder appendOrd(StringBuilder sb, JsonNode o, int indent) {
        if (o.isObject()) {
            return appendObj(sb, o, indent);
        } else if (o.isArray()) {
            return appendAry(sb, o, indent);
        } else if (o.isTextual()) {
            return quoteAndEscape(sb, o.asText());
        } else if (o.isNull() || o.isIntegralNumber() || o.isBoolean()) {
            return sb.append(String.valueOf(o));
        }
        throw new UnsupportedOperationException(o.getClass().getSimpleName());
    }

    private static StringBuilder appendObj(StringBuilder builder, JsonNode jobj, int indent) {
        StringBuilder sb = builder.append("{\n");
        boolean first = true;
        for (Iterator<JsonNode> it = jobj.iterator(); it.hasNext();) {
            final String key = it.next().asText();
            if (first) {
                first = false;
            } else {
                sb = sb.append(",\n");
            }
            sb = indent(sb, indent + 1);
            sb = quote(sb, key);
            sb = sb.append(": ");
            if (jobj.get(key).isArray()) {
                sb = appendAry(sb, jobj.get(key), indent + 1);
            } else if (jobj.get(key).isObject()) {
                sb = appendObj(sb, jobj.get(key), indent + 1);
            } else {
                sb = appendOrd(sb, jobj.get(key), indent + 1);
            }
        }
        sb = sb.append("\n");
        return indent(sb, indent).append("}");
    }

    private static StringBuilder appendAry(StringBuilder builder, JsonNode jarr, int indent) {
        StringBuilder sb = builder.append("[\n");
        for (int i = 0; i < jarr.size(); ++i) {
            if (i > 0) {
                sb = sb.append(",\n");
            }
            sb = indent(sb, indent + 1);
            if (jarr.get(i).isArray()) {
                sb = appendAry(sb, jarr.get(i), indent + 1);
            } else if (jarr.get(i).isObject()) {
                sb = appendObj(sb, jarr.get(i), indent + 1);
            } else {
                sb = appendOrd(sb, jarr.get(i), indent + 1);
            }
        }
        sb = sb.append("\n");
        return indent(sb, indent).append("]");
    }

    private static StringBuilder quote(StringBuilder sb, String str) {
        return sb.append('"').append(str).append('"');
    }

    private static StringBuilder indent(StringBuilder sb, int i) {
        int indent = i;
        while (indent > 0) {
            sb.append(INDENT);
            --indent;
        }
        return sb;
    }

    public static String quoteAndEscape(String str) {
        return quoteAndEscape(new StringBuilder(), str).toString();
    }

    private static StringBuilder quoteAndEscape(StringBuilder sb, String str) {
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
}
