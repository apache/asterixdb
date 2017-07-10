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
package org.apache.asterix.test.common;

import java.io.InputStream;
import java.nio.charset.Charset;
import java.util.Iterator;
import java.util.logging.Logger;

import org.apache.asterix.common.exceptions.AsterixException;
import org.apache.commons.io.IOUtils;

import com.fasterxml.jackson.core.PrettyPrinter;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.Iterators;

/**
 * extracts results from the response of the QueryServiceServlet.
 * As the response is not necessarily valid JSON, non-JSON content has to be extracted in some cases.
 * The current implementation creates a too many copies of the data to be usable for larger results.
 */
public class ResultExtractor {

    private static final Logger LOGGER = Logger.getLogger(ResultExtractor.class.getName());

    public static InputStream extract(InputStream resultStream) throws Exception {
        ObjectMapper om = new ObjectMapper();
        String resultStr = IOUtils.toString(resultStream, Charset.defaultCharset());
        PrettyPrinter singleLine = new SingleLinePrettyPrinter();
        ObjectNode result = om.readValue(resultStr, ObjectNode.class);

        LOGGER.fine("+++++++\n" + result + "\n+++++++\n");

        String type = "";
        String status = "";
        String results = "";
        String field = "";
        for (Iterator<String> sIter = result.fieldNames(); sIter.hasNext();) {
            field = sIter.next();
            switch (field) {
                case "requestID":
                    break;
                case "clientContextID":
                    break;
                case "signature":
                    break;
                case "status":
                    status = om.writeValueAsString(result.get(field));
                    break;
                case "type":
                    type = om.writeValueAsString(result.get(field));
                    break;
                case "metrics":
                    LOGGER.fine(om.writeValueAsString(result.get(field)));
                    break;
                case "errors":
                    JsonNode errors = result.get(field).get(0).get("msg");
                    if (!result.get("metrics").has("errorCount")) {
                        throw new AsterixException("Request reported error but not an errorCount");
                    };
                    throw new AsterixException(errors.asText());
                case "results":
                    if (result.get(field).size() <= 1) {
                        if (result.get(field).size() == 0) {
                            results = "";
                        } else if (result.get(field).isArray()) {
                            if (result.get(field).get(0).isTextual()) {
                                results = result.get(field).get(0).asText();
                            } else {
                                ObjectMapper omm = new ObjectMapper();
                                omm.setDefaultPrettyPrinter(singleLine);
                                omm.enable(SerializationFeature.INDENT_OUTPUT);
                                results = omm.writer(singleLine).writeValueAsString(result.get(field));
                            }
                        } else {
                            results = om.writeValueAsString(result.get(field));
                        }
                    } else {
                        StringBuilder sb = new StringBuilder();
                        JsonNode[] fields = Iterators.toArray(result.get(field).elements(), JsonNode.class);
                        if (fields.length > 1) {
                            for (JsonNode f : fields) {
                                if (f.isObject()) {
                                    sb.append(om.writeValueAsString(f));
                                } else {
                                    sb.append(f.asText());
                                }
                            }
                        }
                        results = sb.toString();
                    }
                    break;
                default:
                    throw new AsterixException("Unanticipated field \"" + field + "\"");
            }
        }

        return IOUtils.toInputStream(results);
    }

    public static String extractHandle(InputStream resultStream) throws Exception {
        final Charset utf8 = Charset.forName("UTF-8");
        ObjectMapper om = new ObjectMapper();
        String result = IOUtils.toString(resultStream, utf8);
        ObjectNode resultJson = om.readValue(result, ObjectNode.class);
        final JsonNode handle = resultJson.get("handle");
        if (handle != null) {
            return handle.asText();
        } else {
            JsonNode errors = resultJson.get("errors");
            if (errors != null) {
                JsonNode msg = errors.get(0).get("msg");
                throw new AsterixException(msg.asText());
            }
        }
        return null;
    }
}
