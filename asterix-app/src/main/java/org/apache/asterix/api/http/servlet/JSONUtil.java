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
package org.apache.asterix.api.http.servlet;

import java.util.Iterator;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

public class JSONUtil {

    static final String INDENT = "    ";

    public static String indent(String str) {
        try {
            return append(new StringBuilder(), new JSONObject(str), 0).toString();
        } catch (JSONException e) {
            throw new IllegalArgumentException(e);
        }
    }

    static StringBuilder append(StringBuilder sb, Object o, int indent) throws JSONException {
        if (o instanceof JSONObject) {
            return append(sb, (JSONObject) o, indent);
        } else if (o instanceof JSONArray) {
            return append(sb, (JSONArray) o, indent);
        } else if (o instanceof String) {
            return quote(sb, (String) o);
        } else if (o instanceof Number || o instanceof Boolean) {
            return sb.append(String.valueOf(o));
        }
        throw new UnsupportedOperationException(o.getClass().getSimpleName());
    }

    static StringBuilder append(StringBuilder sb, JSONObject jobj, int indent) throws JSONException {
        sb = sb.append("{\n");
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

    static StringBuilder append(StringBuilder sb, JSONArray jarr, int indent) throws JSONException {
        sb = sb.append("[\n");
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

    static StringBuilder quote(StringBuilder sb, String str) {
        return sb.append('"').append(str).append('"');
    }

    static StringBuilder indent(StringBuilder sb, int indent) {
        while (indent > 0) {
            sb.append(INDENT);
            --indent;
        }
        return sb;
    }

    public static String escape(String str) {
        StringBuilder result = new StringBuilder();
        for (int i = 0; i < str.length(); ++i) {
            appendEsc(result, str.charAt(i));
        }
        return result.toString();
    }

    public static StringBuilder appendEsc(StringBuilder sb, char c) {
        switch (c) {
            case '"':
                return sb.append("\\\"");
            case '\\':
                return sb.append("\\\\");
            case '/':
                return sb.append("\\/");
            case '\b':
                return sb.append("\\b");
            case '\n':
                return sb.append("\\n");
            case '\f':
                return sb.append("\\f");
            case '\r':
                return sb.append("\\r");
            case '\t':
                return sb.append("\\t");
            default:
                return sb.append(c);
        }
    }

    public static void main(String[] args) {
        String json = args.length > 0 ? args[0] : "{\"a\":[\"b\",\"c\\\nd\"],\"e\":42}";
        System.out.println(json);
        System.out.println(indent(json));
    }
}
