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

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Reader;
import java.io.StringReader;
import java.io.StringWriter;
import java.util.BitSet;
import java.util.Collection;
import java.util.Collections;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

import org.apache.asterix.api.http.server.QueryServiceRequestParameters;
import org.apache.asterix.app.translator.RequestParameters;
import org.apache.asterix.om.base.IAObject;
import org.apache.asterix.testframework.xml.ParameterTypeEnum;
import org.apache.asterix.testframework.xml.TestCase;
import org.apache.asterix.translator.SessionConfig;
import org.apache.commons.compress.utils.IOUtils;
import org.apache.commons.io.FileUtils;
import org.apache.hyracks.util.file.FileUtil;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.TextNode;

public final class TestHelper {

    private static final Logger LOGGER = LogManager.getLogger();
    private static final String TEST_DIR_BASE_PATH = System.getProperty("user.dir") + File.separator + "target";
    private static final String[] TEST_DIRS = new String[] { "txnLogDir", "IODevice", "spill_area", "config" };
    private static final String PATTERN_VAR_ID_PREFIX = "\\$\\$";
    private static final Pattern PATTERN_VAR_ID = Pattern.compile(PATTERN_VAR_ID_PREFIX + "(\\d+)");
    private static final ObjectMapper SORTED_MAPPER = new ObjectMapper();
    private static final ObjectWriter PRETTY_SORTED_WRITER;

    static {
        SORTED_MAPPER.configure(SerializationFeature.ORDER_MAP_ENTRIES_BY_KEYS, true);
        SORTED_MAPPER.configure(MapperFeature.SORT_PROPERTIES_ALPHABETICALLY, true);
        PRETTY_SORTED_WRITER = SORTED_MAPPER.writerWithDefaultPrettyPrinter();
    }

    public static Reader asPrettyJson(final Reader rawJson) throws IOException {
        try {
            StringWriter sw = new StringWriter();
            PRETTY_SORTED_WRITER.writeValue(sw, SORTED_MAPPER.readTree(rawJson));
            return new StringReader(sw.toString());
        } finally {
            IOUtils.closeQuietly(rawJson);
        }
    }

    public static boolean isInPrefixList(List<String> prefixList, String s) {
        for (String s2 : prefixList) {
            if (s.startsWith(s2)) {
                return true;
            }
        }
        return false;
    }

    public static void deleteExistingInstanceFiles() {
        for (String dirName : TEST_DIRS) {
            File f = new File(FileUtil.joinPath(TEST_DIR_BASE_PATH, dirName));
            if (FileUtils.deleteQuietly(f)) {
                System.out.println("Dir " + f.getName() + " deleted");
            }
        }
    }

    public static void unzip(String sourceFile, String outputDir) throws IOException {
        if (System.getProperty("os.name").toLowerCase().startsWith("win")) {
            try (ZipFile zipFile = new ZipFile(sourceFile)) {
                Enumeration<? extends ZipEntry> entries = zipFile.entries();
                while (entries.hasMoreElements()) {
                    ZipEntry entry = entries.nextElement();
                    File entryDestination = new File(outputDir, entry.getName());
                    if (!entry.isDirectory()) {
                        entryDestination.getParentFile().mkdirs();
                        try (InputStream in = zipFile.getInputStream(entry);
                                OutputStream out = new FileOutputStream(entryDestination)) {
                            IOUtils.copy(in, out);
                        }
                    }
                }
            }
        } else {
            Process process = new ProcessBuilder("unzip", "-d", outputDir, sourceFile).start();
            try {
                process.waitFor();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IOException(e);
            }
        }
    }

    public static Map<String, IAObject> readStatementParameters(String statement) throws IOException {
        List<TestCase.CompilationUnit.Parameter> parameterList = TestExecutor.extractParameters(statement);
        if (parameterList.isEmpty()) {
            return Collections.emptyMap();
        }
        Map<String, JsonNode> stmtParams = new HashMap<>();
        ObjectMapper om = createObjectMapper();
        for (TestCase.CompilationUnit.Parameter param : parameterList) {
            String paramName = param.getName();
            JsonNode paramJsonValue;
            ParameterTypeEnum paramType = param.getType();
            if (paramType == null) {
                paramType = ParameterTypeEnum.STRING;
            }
            String paramValue = param.getValue();
            switch (paramType) {
                case STRING:
                    paramJsonValue = TextNode.valueOf(paramValue);
                    break;
                case JSON:
                    paramJsonValue = om.readTree(paramValue);
                    break;
                default:
                    throw new IllegalArgumentException(String.valueOf(paramType));

            }
            String name = QueryServiceRequestParameters.extractStatementParameterName(paramName);
            if (name != null) {
                stmtParams.put(name, paramJsonValue);
            } else if (QueryServiceRequestParameters.Parameter.ARGS.str().equals(paramName)) {
                if (paramJsonValue.isArray()) {
                    for (int i = 0, ln = paramJsonValue.size(); i < ln; i++) {
                        stmtParams.put(String.valueOf(i + 1), paramJsonValue.get(i));
                    }
                }
            }
        }

        return RequestParameters.deserializeParameterValues(
                RequestParameters.serializeParameterValues(stmtParams, SessionConfig.OutputFormat.CLEAN_JSON));
    }

    public static boolean equalJson(JsonNode expectedJson, JsonNode actualJson, boolean compareUnorderedArray,
            boolean ignoreExtraFields, boolean withinUnorderedComparison, String context) {
        if (expectedJson == actualJson) {
            return true;
        }
        // exactly one is null
        if (expectedJson == null || actualJson == null) {
            return false;
        }
        if ((expectedJson.isMissingNode() && !actualJson.isMissingNode())
                || (!expectedJson.isMissingNode() && actualJson.isMissingNode())) {
            if (!withinUnorderedComparison) {
                LOGGER.info("missing node mismatch: expected={} actual={} context={}", expectedJson, actualJson,
                        context);
            }
            return false;
        }
        // both are not null
        if (isRegexField(expectedJson)) {
            String expectedRegex = expectedJson.asText();
            String actualAsString = stringify(actualJson);
            expectedRegex = expectedRegex.substring(2, expectedRegex.length() - 1);
            final boolean matches = actualAsString.matches(expectedRegex);
            if (!matches && !withinUnorderedComparison) {
                LOGGER.info("regex mismatch: expected={} actual={} context={}", expectedRegex, actualAsString, context);
            }
            return matches;
        } else if (expectedJson.getNodeType() != actualJson.getNodeType()) {
            if (!withinUnorderedComparison) {
                LOGGER.info("node type mismatch: expected={}({}) actual={}({})", stringify(expectedJson),
                        expectedJson.getNodeType(), stringify(actualJson), actualJson.getNodeType());
            }
            return false;
        } else if (expectedJson.isArray() && actualJson.isArray()) {
            ArrayNode expectedArray = (ArrayNode) expectedJson;
            ArrayNode actualArray = (ArrayNode) actualJson;
            if (expectedArray.size() != actualArray.size()) {
                if (!withinUnorderedComparison) {
                    LOGGER.info("array size mismatch: expected={} actual={}", stringify(expectedArray),
                            stringify(actualArray));
                }
                return false;
            }
            return compareUnorderedArray ? compareUnordered(expectedArray, actualArray, ignoreExtraFields)
                    : compareOrdered(expectedArray, actualArray, ignoreExtraFields);
        } else if (expectedJson.isObject() && actualJson.isObject()) {
            // assumes no duplicates in field names
            ObjectNode expectedObject = (ObjectNode) expectedJson;
            ObjectNode actualObject = (ObjectNode) actualJson;
            if (!ignoreExtraFields && expectedObject.size() != actualObject.size()
                    || (ignoreExtraFields && expectedObject.size() > actualObject.size())) {
                if (!withinUnorderedComparison) {
                    LOGGER.info("object size mismatch: expected={} actual={} context={}", stringify(expectedObject),
                            stringify(actualObject), context);
                }
                return false;
            }
            Iterator<Map.Entry<String, JsonNode>> expectedFields = expectedObject.fields();
            Map.Entry<String, JsonNode> expectedField;
            JsonNode actualFieldValue;
            while (expectedFields.hasNext()) {
                expectedField = expectedFields.next();
                actualFieldValue = actualObject.get(expectedField.getKey());
                if (actualFieldValue == null) {
                    if (!withinUnorderedComparison) {
                        LOGGER.info("actual field value null: expected name={} expected value={}",
                                expectedField.getKey(), expectedField.getValue().asText());
                    }
                    return false;
                }
                if (!equalJson(expectedField.getValue(), actualFieldValue, compareUnorderedArray, ignoreExtraFields,
                        withinUnorderedComparison, expectedField.getKey())) {
                    return false;
                }
            }
            return true;
        }
        // value node
        String expectedAsString = stringify(expectedJson);
        String actualAsString = stringify(actualJson);
        if (!expectedAsString.equals(actualAsString)) {
            if (!withinUnorderedComparison) {
                LOGGER.info("value node mismatch: expected={} actual={} context={}", expectedAsString, actualAsString,
                        context);
            }
            return false;
        }
        return true;
    }

    private static String stringify(JsonNode jsonNode) {
        return jsonNode == null ? null : (jsonNode.isValueNode() ? jsonNode.asText() : jsonNode.toString());
    }

    private static boolean compareUnordered(ArrayNode expectedArray, ArrayNode actualArray, boolean ignoreExtraFields) {
        BitSet alreadyMatched = new BitSet(actualArray.size());
        for (int i = 0; i < expectedArray.size(); i++) {
            boolean found = false;
            JsonNode expectedElement = expectedArray.get(i);
            for (int k = 0; k < actualArray.size(); k++) {
                if (!alreadyMatched.get(k) && equalJson(expectedElement, actualArray.get(k), true, ignoreExtraFields,
                        true, stringify(actualArray))) {
                    alreadyMatched.set(k);
                    found = true;
                    break;
                }
            }
            if (!found) {
                LOGGER.info("unordered array comparison failed; expected={} actual={}", expectedArray, actualArray);
                return false;
            }
        }
        return true;
    }

    private static boolean compareOrdered(ArrayNode expectedArray, ArrayNode actualArray, boolean ignoreExtraFields) {
        for (int i = 0, size = expectedArray.size(); i < size; i++) {
            if (!equalJson(expectedArray.get(i), actualArray.get(i), false, ignoreExtraFields, false,
                    stringify(actualArray))) {
                return false;
            }
        }
        return true;
    }

    private static boolean isRegexField(JsonNode expectedJson) {
        if (expectedJson.isTextual()) {
            String regexValue = expectedJson.asText();
            return regexValue.startsWith("R{");
        }
        return false;
    }

    private static ObjectMapper createObjectMapper() {
        ObjectMapper objectMapper = new ObjectMapper();
        objectMapper.enable(SerializationFeature.INDENT_OUTPUT);
        objectMapper.configure(MapperFeature.SORT_PROPERTIES_ALPHABETICALLY, true);
        objectMapper.configure(SerializationFeature.ORDER_MAP_ENTRIES_BY_KEYS, true);
        return objectMapper;
    }

    public static void comparePlans(List<String> linesExpected, List<String> linesActual, File queryFile)
            throws Exception {
        int varBaseExpected = findBaseVarId(linesExpected);
        int varBaseActual = findBaseVarId(linesActual);

        Iterator<String> readerExpected = linesExpected.iterator();
        Iterator<String> readerActual = linesActual.iterator();
        String lineExpected, lineActual;
        int num = 1;
        while (readerExpected.hasNext()) {
            lineExpected = readerExpected.next();
            if (!readerActual.hasNext()) {
                throw TestExecutor.createLineNotFoundException(queryFile, lineExpected, num);
            }
            lineActual = readerActual.next();

            if (!planLineEquals(lineExpected, varBaseExpected, lineActual, varBaseActual)) {
                throw TestExecutor.createLineChangedException(queryFile, lineExpected, lineActual, num);
            }
            ++num;
        }
        if (readerActual.hasNext()) {
            throw new Exception(
                    "Result for " + queryFile + " changed at line " + num + ":\n< \n> " + readerActual.next());
        }
    }

    private static boolean planLineEquals(String lineExpected, int varIdBaseExpected, String lineActual,
            int varIdBaseActual) {
        String lineExpectedNorm = normalizePlanLine(lineExpected, varIdBaseExpected);
        String lineActualNorm = normalizePlanLine(lineActual, varIdBaseActual);
        return lineExpectedNorm.equals(lineActualNorm);
    }

    // rewrite variable ids in given plan line: $$varId -> $$(varId-varIdBase)
    private static String normalizePlanLine(String line, int varIdBase) {
        if (varIdBase == Integer.MAX_VALUE) {
            // plan did not contain any variables -> no rewriting necessary
            return line;
        }
        Matcher m = PATTERN_VAR_ID.matcher(line);
        StringBuilder sb = new StringBuilder(line.length());
        while (m.find()) {
            int varId = Integer.parseInt(m.group(1));
            int newVarId = varId - varIdBase;
            m.appendReplacement(sb, PATTERN_VAR_ID_PREFIX + newVarId);
        }
        m.appendTail(sb);
        return sb.toString();
    }

    private static int findBaseVarId(Collection<String> plan) {
        int varIdBase = Integer.MAX_VALUE;
        Matcher m = PATTERN_VAR_ID.matcher("");
        for (String line : plan) {
            m.reset(line);
            while (m.find()) {
                int varId = Integer.parseInt(m.group(1));
                varIdBase = Math.min(varIdBase, varId);
            }
        }
        return varIdBase;
    }
}
