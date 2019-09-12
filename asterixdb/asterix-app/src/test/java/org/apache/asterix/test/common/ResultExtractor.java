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
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.asterix.common.exceptions.AsterixException;
import org.apache.commons.io.IOUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.fasterxml.jackson.core.util.DefaultPrettyPrinter;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.Iterators;

/**
 * extracts results from the response of the QueryServiceServlet.
 * As the response is not necessarily valid JSON, non-JSON content has to be extracted in some cases.
 * The current implementation creates a too many copies of the data to be usable for larger results.
 */
public class ResultExtractor {

    private enum ResultField {
        RESULTS("results"),
        REQUEST_ID("requestID"),
        METRICS("metrics"),
        PROFILE("profile"),
        CLIENT_CONTEXT_ID("clientContextID"),
        SIGNATURE("signature"),
        STATUS("status"),
        TYPE("type"),
        ERRORS("errors"),
        PLANS("plans"),
        WARNINGS("warnings");

        private static final Map<String, ResultField> fields = new HashMap<>();

        static {
            for (ResultField field : ResultField.values()) {
                fields.put(field.getFieldName(), field);
            }
        }

        private String fieldName;

        ResultField(String fieldName) {
            this.fieldName = fieldName;
        }

        public String getFieldName() {
            return fieldName;
        }

        public static ResultField ofFieldName(String fieldName) {
            return fields.get(fieldName);
        }
    }

    private static final Logger LOGGER = LogManager.getLogger();
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    public static ExtractedResult extract(InputStream resultStream, Charset resultCharset) throws Exception {
        return extract(resultStream, EnumSet.of(ResultField.RESULTS, ResultField.WARNINGS), resultCharset);
    }

    public static InputStream extractMetrics(InputStream resultStream, Charset resultCharset) throws Exception {
        return extract(resultStream, EnumSet.of(ResultField.METRICS), resultCharset).getResult();
    }

    public static InputStream extractProfile(InputStream resultStream, Charset resultCharset) throws Exception {
        return extract(resultStream, EnumSet.of(ResultField.PROFILE), resultCharset).getResult();
    }

    public static InputStream extractPlans(InputStream resultStream, Charset resultCharset) throws Exception {
        return extract(resultStream, EnumSet.of(ResultField.PLANS), resultCharset).getResult();
    }

    public static String extractHandle(InputStream resultStream, Charset responseCharset) throws Exception {
        String result = IOUtils.toString(resultStream, responseCharset);
        ObjectNode resultJson = OBJECT_MAPPER.readValue(result, ObjectNode.class);
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

    private static ExtractedResult extract(InputStream resultStream, EnumSet<ResultField> resultFields,
            Charset resultCharset) throws Exception {
        ExtractedResult extractedResult = new ExtractedResult();
        final String resultStr = IOUtils.toString(resultStream, resultCharset);
        final ObjectNode result = OBJECT_MAPPER.readValue(resultStr, ObjectNode.class);

        LOGGER.debug("+++++++\n" + result + "\n+++++++\n");
        // if we have errors field in the results, we will always return it
        checkForErrors(result);
        final StringBuilder resultBuilder = new StringBuilder();
        for (Iterator<String> fieldNameIter = result.fieldNames(); fieldNameIter.hasNext();) {
            final String fieldName = fieldNameIter.next();
            final ResultField fieldKind = ResultField.ofFieldName(fieldName.split("-")[0]);
            if (fieldKind == null) {
                throw new AsterixException("Unanticipated field \"" + fieldName + "\"");
            }
            if (!resultFields.contains(fieldKind)) {
                continue;
            }
            final JsonNode fieldValue = result.get(fieldName);
            switch (fieldKind) {
                case RESULTS:
                    if (fieldValue.size() <= 1) {
                        if (fieldValue.size() == 0) {
                            resultBuilder.append("");
                        } else if (fieldValue.isArray()) {
                            if (fieldValue.get(0).isTextual()) {
                                resultBuilder.append(fieldValue.get(0).asText());
                            } else {
                                ObjectMapper omm = new ObjectMapper();
                                omm.enable(SerializationFeature.INDENT_OUTPUT);
                                resultBuilder
                                        .append(omm.writer(new DefaultPrettyPrinter()).writeValueAsString(fieldValue));
                            }
                        } else {
                            resultBuilder.append(OBJECT_MAPPER.writeValueAsString(fieldValue));
                        }
                    } else {
                        JsonNode[] fields = Iterators.toArray(fieldValue.elements(), JsonNode.class);
                        if (fields.length > 1) {
                            for (JsonNode f : fields) {
                                if (f.isObject()) {

                                    resultBuilder.append(OBJECT_MAPPER.writeValueAsString(f));
                                } else {
                                    resultBuilder.append(f.asText());
                                }
                            }
                        }

                    }
                    break;
                case REQUEST_ID:
                case METRICS:
                case PROFILE:
                case CLIENT_CONTEXT_ID:
                case SIGNATURE:
                case STATUS:
                case TYPE:
                case PLANS:
                    resultBuilder.append(OBJECT_MAPPER.writeValueAsString(fieldValue));
                case WARNINGS:
                    extractWarnings(fieldValue, extractedResult);
                    break;
                default:
                    throw new IllegalStateException("Unexpected result field: " + fieldKind);
            }
        }
        extractedResult.setResult(IOUtils.toInputStream(resultBuilder, resultCharset));
        return extractedResult;
    }

    private static void checkForErrors(ObjectNode result) throws Exception {
        final JsonNode errorsField = result.get(ResultField.ERRORS.getFieldName());
        if (errorsField != null) {
            final JsonNode errors = errorsField.get(0).get("msg");
            if (!result.get(ResultField.METRICS.getFieldName()).has("errorCount")) {
                throw new Exception("Request reported error but not an errorCount");
            }
            throw new Exception(errors.asText());
        }
    }

    private static void extractWarnings(JsonNode warningsValue, ExtractedResult exeResult) {
        List<String> warnings = new ArrayList<>();
        if (warningsValue.isArray()) {
            final ArrayNode warningsArray = (ArrayNode) warningsValue;
            for (JsonNode warn : warningsArray) {
                warnings.add(warn.get("msg").asText());
            }
        }
        exeResult.setWarnings(warnings);
    }
}
