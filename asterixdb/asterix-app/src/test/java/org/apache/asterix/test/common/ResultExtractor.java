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

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.asterix.common.exceptions.AsterixException;
import org.apache.asterix.testframework.context.TestCaseContext.OutputFormat;
import org.apache.commons.io.IOUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.PrettyPrinter;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.Iterators;

/**
 * extracts results from the response of the QueryServiceServlet.
 * As the response is not necessarily valid JSON, non-JSON content has to be extracted in some cases.
 * The current implementation creates a too many copies of the data to be usable for larger results.
 */
public class ResultExtractor {

    private static class JsonPrettyPrinter implements PrettyPrinter {

        JsonPrettyPrinter() {
        }

        @Override
        public void writeRootValueSeparator(JsonGenerator gen) throws IOException {

        }

        @Override
        public void writeStartObject(JsonGenerator g) throws IOException {
            g.writeRaw("{ ");
        }

        @Override
        public void writeEndObject(JsonGenerator g, int nrOfEntries) throws IOException {
            g.writeRaw(" }");
        }

        @Override
        public void writeObjectFieldValueSeparator(JsonGenerator jg) throws IOException {
            jg.writeRaw(": ");
        }

        @Override
        public void writeObjectEntrySeparator(JsonGenerator g) throws IOException {
            g.writeRaw(", ");
        }

        @Override
        public void writeStartArray(JsonGenerator g) throws IOException {
            g.writeRaw("[ ");
        }

        @Override
        public void writeEndArray(JsonGenerator g, int nrOfValues) throws IOException {
            g.writeRaw(" ]");
        }

        @Override
        public void writeArrayValueSeparator(JsonGenerator g) throws IOException {
            g.writeRaw(", ");
        }

        @Override
        public void beforeArrayValues(JsonGenerator gen) throws IOException {
        }

        @Override
        public void beforeObjectEntries(JsonGenerator gen) throws IOException {
        }
    }

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
    private static final ObjectWriter WRITER = OBJECT_MAPPER.writer();
    private static final ObjectWriter PP_WRITER = OBJECT_MAPPER.writer(new JsonPrettyPrinter());
    private static final ObjectReader OBJECT_READER = OBJECT_MAPPER.readerFor(ObjectNode.class);

    public static ExtractedResult extract(InputStream resultStream, Charset resultCharset, OutputFormat outputFormat)
            throws Exception {
        return extract(resultStream, EnumSet.of(ResultField.RESULTS, ResultField.WARNINGS), resultCharset, outputFormat,
                null);
    }

    public static ExtractedResult extract(InputStream resultStream, Charset resultCharset) throws Exception {
        return extract(resultStream, EnumSet.of(ResultField.RESULTS, ResultField.WARNINGS), resultCharset);
    }

    public static InputStream extractMetrics(InputStream resultStream, Charset resultCharset) throws Exception {
        return extract(resultStream, EnumSet.of(ResultField.METRICS), resultCharset).getResult();
    }

    public static InputStream extractProfile(InputStream resultStream, Charset resultCharset) throws Exception {
        return extract(resultStream, EnumSet.of(ResultField.PROFILE), resultCharset).getResult();
    }

    public static InputStream extractPlans(InputStream resultStream, Charset resultCharset, String[] plans)
            throws Exception {
        return extract(resultStream, EnumSet.of(ResultField.PLANS), resultCharset, OutputFormat.ADM, plans).getResult();
    }

    public static InputStream extractStatus(InputStream resultStream, Charset resultCharset) throws Exception {
        return extract(resultStream, EnumSet.of(ResultField.STATUS), resultCharset).getResult();
    }

    public static String extractHandle(InputStream resultStream, Charset responseCharset) throws Exception {
        String result = IOUtils.toString(resultStream, responseCharset);
        ObjectNode resultJson = OBJECT_READER.readValue(result);
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
        return extract(resultStream, resultFields, resultCharset, OutputFormat.ADM, null);
    }

    private static ExtractedResult extract(InputStream resultStream, EnumSet<ResultField> resultFields,
            Charset resultCharset, OutputFormat fmt, String[] plans) throws Exception {
        ExtractedResult extractedResult = new ExtractedResult();
        final String resultStr = IOUtils.toString(resultStream, resultCharset);

        LOGGER.debug("+++++++\n" + resultStr + "\n+++++++\n");

        final ObjectNode result = OBJECT_READER.readValue(resultStr);
        final boolean isJsonFormat = isJsonFormat(fmt);

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
                            JsonNode oneElement = fieldValue.get(0);
                            if (oneElement.isTextual()) {
                                resultBuilder.append(isJsonFormat ? prettyPrint(oneElement) : oneElement.asText());
                            } else {
                                resultBuilder.append(prettyPrint(oneElement));
                            }
                        } else {
                            resultBuilder.append(prettyPrint(fieldValue));
                        }
                    } else {
                        JsonNode[] fields = Iterators.toArray(fieldValue.elements(), JsonNode.class);
                        if (isJsonFormat) {
                            for (JsonNode f : fields) {
                                resultBuilder.append(prettyPrint(f)).append('\n');
                            }
                        } else {
                            for (JsonNode f : fields) {
                                if (f.isValueNode()) {
                                    resultBuilder.append(f.asText());
                                } else {
                                    resultBuilder.append(prettyPrint(f)).append('\n');
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
                    if (plans == null) {
                        resultBuilder.append(WRITER.writeValueAsString(fieldValue));
                    } else {
                        for (int i = 0, size = plans.length; i < size; i++) {
                            JsonNode plan = fieldValue.get(plans[i]);
                            if (plan != null) {
                                resultBuilder.append(plan.asText());
                            }
                        }
                    }

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

    public static String prettyPrint(JsonNode node) throws JsonProcessingException {
        return PP_WRITER.writeValueAsString(node);
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

    private static boolean isJsonFormat(OutputFormat format) {
        return format == OutputFormat.CLEAN_JSON || format == OutputFormat.LOSSLESS_JSON;
    }
}
