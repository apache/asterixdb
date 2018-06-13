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
import java.util.Collections;
import java.util.Enumeration;
import java.util.HashMap;
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

    private static ObjectMapper createObjectMapper() {
        ObjectMapper objectMapper = new ObjectMapper();
        objectMapper.enable(SerializationFeature.INDENT_OUTPUT);
        objectMapper.configure(MapperFeature.SORT_PROPERTIES_ALPHABETICALLY, true);
        objectMapper.configure(SerializationFeature.ORDER_MAP_ENTRIES_BY_KEYS, true);
        return objectMapper;
    }
}