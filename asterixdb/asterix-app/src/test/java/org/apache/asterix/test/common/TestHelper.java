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
import java.util.BitSet;
import java.util.Collections;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

import org.apache.asterix.api.http.server.QueryServiceServlet;
import org.apache.asterix.app.translator.RequestParameters;
import org.apache.asterix.om.base.IAObject;
import org.apache.asterix.testframework.xml.ParameterTypeEnum;
import org.apache.asterix.testframework.xml.TestCase;
import org.apache.commons.compress.utils.IOUtils;
import org.apache.commons.io.FileUtils;
import org.apache.hyracks.util.file.FileUtil;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.TextNode;

public final class TestHelper {

    private static final String TEST_DIR_BASE_PATH = System.getProperty("user.dir") + File.separator + "target";
    private static final String[] TEST_DIRS = new String[] { "txnLogDir", "IODevice", "spill_area", "config" };

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
            String name = QueryServiceServlet.extractStatementParameterName(paramName);
            if (name != null) {
                stmtParams.put(name, paramJsonValue);
            } else if (QueryServiceServlet.Parameter.ARGS.str().equals(paramName)) {
                if (paramJsonValue.isArray()) {
                    for (int i = 0, ln = paramJsonValue.size(); i < ln; i++) {
                        stmtParams.put(String.valueOf(i + 1), paramJsonValue.get(i));
                    }
                }
            }
        }

        return RequestParameters.deserializeParameterValues(RequestParameters.serializeParameterValues(stmtParams));
    }

    public static boolean equalJson(JsonNode expectedJson, JsonNode actualJson) {
        if (expectedJson == actualJson) {
            return true;
        }
        // exactly one is null
        if (expectedJson == null || actualJson == null) {
            return false;
        }
        // both are not null
        if (!isRegexField(expectedJson) && expectedJson.getNodeType() != actualJson.getNodeType()) {
            return false;
        } else if (expectedJson.isArray() && actualJson.isArray()) {
            ArrayNode expectedArray = (ArrayNode) expectedJson;
            ArrayNode actualArray = (ArrayNode) actualJson;
            if (expectedArray.size() != actualArray.size()) {
                return false;
            }
            boolean found;
            BitSet alreadyMatched = new BitSet(actualArray.size());
            for (int i = 0; i < expectedArray.size(); i++) {
                found = false;
                for (int k = 0; k < actualArray.size(); k++) {
                    if (!alreadyMatched.get(k) && equalJson(expectedArray.get(i), actualArray.get(k))) {
                        alreadyMatched.set(k);
                        found = true;
                        break;
                    }
                }
                if (!found) {
                    return false;
                }
            }
            return true;
        } else if (expectedJson.isObject() && actualJson.isObject()) {
            // assumes no duplicates in field names
            ObjectNode expectedObject = (ObjectNode) expectedJson;
            ObjectNode actualObject = (ObjectNode) actualJson;
            if (expectedObject.size() != actualObject.size()) {
                return false;
            }
            Iterator<Map.Entry<String, JsonNode>> expectedFields = expectedObject.fields();
            Map.Entry<String, JsonNode> expectedField;
            JsonNode actualFieldValue;
            while (expectedFields.hasNext()) {
                expectedField = expectedFields.next();
                actualFieldValue = actualObject.get(expectedField.getKey());
                if (actualFieldValue == null || !equalJson(expectedField.getValue(), actualFieldValue)) {
                    return false;
                }
            }
            return true;
        }
        // value node
        String expectedAsString = expectedJson.asText();
        String actualAsString = actualJson.asText();
        if (expectedAsString.startsWith("R{")) {
            expectedAsString = expectedAsString.substring(2, expectedAsString.length() - 1);
            return actualAsString.matches(expectedAsString);
        }
        return expectedAsString.equals(actualAsString);
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

    public static void main(String[] args) throws Exception {
        ObjectMapper om = createObjectMapper();
        String patternFile = args[0];
        String instanceFile = args[1];
        if (equalJson(om.readTree(new File(patternFile)), om.readTree(new File(instanceFile)))) {
            System.out.println(instanceFile + " matches " + patternFile);
        } else {
            System.out.println(instanceFile + " does not match " + patternFile);
            System.exit(1);
        }
    }
}
