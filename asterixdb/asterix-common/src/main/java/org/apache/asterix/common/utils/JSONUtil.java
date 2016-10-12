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

import java.util.Iterator;
import java.util.logging.Logger;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

public class JSONUtil {

    private static final Logger LOGGER = Logger.getLogger(JSONUtil.class.getName());

    private static final String INDENT = "\t";

    private JSONUtil() {
    }

    public static String indent(String str, int initialIndent) {
        try {
            return append(new StringBuilder(), new JSONObject(str), initialIndent).toString();
        } catch (JSONException e) {
            LOGGER.finest("Could not indent JSON string, returning the input string: " + str);
            return str;
        }
    }

    private static StringBuilder append(StringBuilder sb, Object o, int indent) throws JSONException {
        if (o instanceof JSONObject) {
            return append(sb, (JSONObject) o, indent);
        } else if (o instanceof JSONArray) {
            return append(sb, (JSONArray) o, indent);
        } else if (o instanceof String) {
            return quoteAndEscape(sb, (String) o);
        } else if (JSONObject.NULL.equals(o) || o instanceof Number || o instanceof Boolean) {
            return sb.append(String.valueOf(o));
        }
        throw new UnsupportedOperationException(o.getClass().getSimpleName());
    }

    private static StringBuilder append(StringBuilder builder, JSONObject jobj, int indent) throws JSONException {
        StringBuilder sb = builder.append("{\n");
        boolean first = true;
        for (Iterator it = jobj.keys(); it.hasNext();) {
            final String key = (String) it.next();
            if (first) {
                first = false;
            } else {
                sb = sb.append(",\n");
            }
            sb = indent(sb, indent + 1);
            sb = quote(sb, key);
            sb = sb.append(": ");
            sb = append(sb, jobj.get(key), indent + 1);
        }
        sb = sb.append("\n");
        return indent(sb, indent).append("}");
    }

    private static StringBuilder append(StringBuilder builder, JSONArray jarr, int indent) throws JSONException {
        StringBuilder sb = builder.append("[\n");
        for (int i = 0; i < jarr.length(); ++i) {
            if (i > 0) {
                sb = sb.append(",\n");
            }
            sb = indent(sb, indent + 1);
            sb = append(sb, jarr.get(i), indent + 1);
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
