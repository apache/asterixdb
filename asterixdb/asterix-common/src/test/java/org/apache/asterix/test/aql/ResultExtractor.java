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
package org.apache.asterix.test.aql;

import java.io.InputStream;
import java.nio.charset.Charset;
import java.util.logging.Logger;

import org.apache.commons.io.IOUtils;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.json.JSONTokener;

/**
 * extracts results from the response of the QueryServiceServlet.
 *
 * As the response is not necessarily valid JSON, non-JSON content has to be extracted in some cases.
 * The current implementation creates a toomany copies of the data to be usable for larger results.
 */
public class ResultExtractor {

    private static final Logger LOGGER = Logger.getLogger(ResultExtractor.class.getName());

    public static InputStream extract(InputStream resultStream) throws Exception {
        final Charset utf8 = Charset.forName("UTF-8");
        String result = IOUtils.toString(resultStream, utf8);

        LOGGER.fine("+++++++\n" + result + "\n+++++++\n");

        JSONTokener tokener = new JSONTokener(result);
        tokener.nextTo('{');
        tokener.next('{');
        String name;
        String type = null;
        String status = null;
        String results = "";
        while ((name = getFieldName(tokener)) != null) {
            if ("requestID".equals(name) || "signature".equals(name)) {
                getStringField(tokener);
            } else if ("status".equals(name)) {
                status = getStringField(tokener);
            } else if ("type".equals(name)) {
                type = getStringField(tokener);
            } else if ("metrics".equals(name)) {
                JSONObject metrics = getObjectField(tokener);
                LOGGER.fine(name + ": " + metrics);
            } else if ("errors".equals(name)) {
                JSONArray errors = getArrayField(tokener);
                LOGGER.fine(name + ": " + errors);
                JSONObject err = errors.getJSONObject(0);
                throw new Exception(err.getString("msg"));
            } else if ("results".equals(name)) {
                results = getResults(tokener, type);
            } else {
                throw tokener.syntaxError(name + ": unanticipated field");
            }
        }
        while (tokener.more() && tokener.skipTo('}') != '}') {
            // skip along
        }
        tokener.next('}');
        if (! "success".equals(status)) {
            throw new Exception("Unexpected status: '" + status + "'");
        }
        return IOUtils.toInputStream(results, utf8);
    }

    public static String extractHandle(InputStream resultStream) throws Exception {
        final Charset utf8 = Charset.forName("UTF-8");
        String result = IOUtils.toString(resultStream, utf8);
        JSONObject parsed = new JSONObject(result);
        JSONArray handle = parsed.getJSONArray("handle");
        JSONObject res = new JSONObject();
        res.put("handle", handle);
        return res.toString();
    }

    private static String getFieldName(JSONTokener tokener) throws JSONException {
        char c = tokener.skipTo('"');
        if (c != '"') {
            return null;
        }
        tokener.next('"');
        return tokener.nextString('"');
    }

    private static String getStringField(JSONTokener tokener) throws JSONException {
        tokener.skipTo('"');
        tokener.next('"');
        return tokener.nextString('"');
    }

    private static JSONArray getArrayField(JSONTokener tokener) throws JSONException {
        tokener.skipTo(':');
        tokener.next(':');
        Object obj = tokener.nextValue();
        if (obj instanceof JSONArray) {
            return (JSONArray) obj;
        } else {
            throw tokener.syntaxError(String.valueOf(obj) + ": unexpected value");
        }
    }

    private static JSONObject getObjectField(JSONTokener tokener) throws JSONException {
        tokener.skipTo(':');
        tokener.next(':');
        Object obj = tokener.nextValue();
        if (obj instanceof JSONObject) {
            return (JSONObject) obj;
        } else {
            throw tokener.syntaxError(String.valueOf(obj) + ": unexpected value");
        }
    }

    private static String getResults(JSONTokener tokener, String type) throws JSONException {
        tokener.skipTo(':');
        tokener.next(':');
        StringBuilder result = new StringBuilder();
        if (type != null) {
            // a type was provided in the response and so the result is encoded as an array of escaped strings that
            // need to be concatenated
            Object obj = tokener.nextValue();
            if (!(obj instanceof JSONArray)) {
                throw tokener.syntaxError("array expected");
            }
            JSONArray array = (JSONArray) obj;
            for (int i = 0; i < array.length(); ++i) {
                result.append(array.getString(i));
            }
            return result.toString();
        } else {
            int level = 0;
            boolean inQuote = false;
            while (tokener.more()) {
                char c = tokener.next();
                switch (c) {
                    case '{':
                    case '[':
                        ++level;
                        result.append(c);
                        break;
                    case '}':
                    case ']':
                        --level;
                        result.append(c);
                        break;
                    case '"':
                        if (inQuote) {
                            --level;
                        } else {
                            ++level;
                        }
                        inQuote = !inQuote;
                        result.append(c);
                        break;
                    case ',':
                        if (level == 0) {
                            return result.toString().trim();
                        } else {
                            result.append(c);
                        }
                        break;
                    default:
                        result.append(c);
                        break;
                }
            }
        }
        return null;
    }
}
