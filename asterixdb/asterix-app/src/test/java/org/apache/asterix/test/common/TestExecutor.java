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

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.PrintStream;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.Inet4Address;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Predicate;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.asterix.common.config.GlobalConfig;
import org.apache.asterix.common.utils.Servlets;
import org.apache.asterix.test.server.ITestServer;
import org.apache.asterix.test.server.TestServerProvider;
import org.apache.asterix.testframework.context.TestCaseContext;
import org.apache.asterix.testframework.context.TestCaseContext.OutputFormat;
import org.apache.asterix.testframework.context.TestFileContext;
import org.apache.asterix.testframework.xml.ComparisonEnum;
import org.apache.asterix.testframework.xml.TestCase.CompilationUnit;
import org.apache.asterix.testframework.xml.TestGroup;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.io.output.ByteArrayOutputStream;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.mutable.MutableInt;
import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.client.methods.RequestBuilder;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.client.StandardHttpRequestRetryHandler;
import org.apache.http.protocol.HttpContext;
import org.apache.http.util.EntityUtils;
import org.apache.hyracks.util.StorageUtil;
import org.junit.Assert;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

public class TestExecutor {

    /*
     * Static variables
     */
    protected static final Logger LOGGER = Logger.getLogger(TestExecutor.class.getName());
    // see
    // https://stackoverflow.com/questions/417142/what-is-the-maximum-length-of-a-url-in-different-browsers/417184
    private static final long MAX_URL_LENGTH = 2000l;
    private static final Pattern JAVA_BLOCK_COMMENT_PATTERN =
            Pattern.compile("/\\*.*\\*/", Pattern.MULTILINE | Pattern.DOTALL);
    private static final Pattern JAVA_LINE_COMMENT_PATTERN = Pattern.compile("//.*$", Pattern.MULTILINE);
    private static final Pattern SHELL_LINE_COMMENT_PATTERN = Pattern.compile("#.*$", Pattern.MULTILINE);
    private static final Pattern REGEX_LINES_PATTERN = Pattern.compile("^(-)?/(.*)/([im]*)$");
    private static final Pattern POLL_TIMEOUT_PATTERN =
            Pattern.compile("polltimeoutsecs=(\\d+)(\\D|$)", Pattern.MULTILINE);
    private static final Pattern POLL_DELAY_PATTERN = Pattern.compile("polldelaysecs=(\\d+)(\\D|$)", Pattern.MULTILINE);
    private static final Pattern HANDLE_VARIABLE_PATTERN = Pattern.compile("handlevariable=(\\w+)");
    private static final Pattern VARIABLE_REF_PATTERN = Pattern.compile("\\$(\\w+)");

    public static final int TRUNCATE_THRESHOLD = 16384;

    public static final String DELIVERY_ASYNC = "async";
    public static final String DELIVERY_DEFERRED = "deferred";
    public static final String DELIVERY_IMMEDIATE = "immediate";

    private static Method managixExecuteMethod = null;
    private static final HashMap<Integer, ITestServer> runningTestServers = new HashMap<>();

    /*
     * Instance members
     */
    protected final List<InetSocketAddress> endpoints;
    protected int endpointSelector;
    protected ITestLibrarian librarian;

    public TestExecutor() {
        this(Inet4Address.getLoopbackAddress().getHostAddress(), 19002);
    }

    public TestExecutor(String host, int port) {
        this(InetSocketAddress.createUnresolved(host, port));
    }

    public TestExecutor(InetSocketAddress endpoint) {
        this(Collections.singletonList(endpoint));
    }

    public TestExecutor(List<InetSocketAddress> endpoints) {
        this.endpoints = endpoints;
    }

    public void setLibrarian(ITestLibrarian librarian) {
        this.librarian = librarian;
    }

    /**
     * Probably does not work well with symlinks.
     */
    public boolean deleteRec(File path) {
        if (path.isDirectory()) {
            for (File f : path.listFiles()) {
                if (!deleteRec(f)) {
                    return false;
                }
            }
        }
        return path.delete();
    }

    public void runScriptAndCompareWithResult(File scriptFile, PrintWriter print, File expectedFile, File actualFile,
            ComparisonEnum compare) throws Exception {
        System.err.println("Expected results file: " + expectedFile.toString());
        BufferedReader readerExpected =
                new BufferedReader(new InputStreamReader(new FileInputStream(expectedFile), "UTF-8"));
        BufferedReader readerActual =
                new BufferedReader(new InputStreamReader(new FileInputStream(actualFile), "UTF-8"));
        boolean regex = false;
        try {
            if (ComparisonEnum.BINARY.equals(compare)) {
                if (!IOUtils.contentEquals(new FileInputStream(actualFile), new FileInputStream(expectedFile))) {
                    throw new Exception("Result for " + scriptFile + ": actual file did not match expected result");
                }
                return;
            } else if (actualFile.toString().endsWith(".regex")) {
                runScriptAndCompareWithResultRegex(scriptFile, expectedFile, actualFile);
                return;
            } else if (actualFile.toString().endsWith(".regexadm")) {
                runScriptAndCompareWithResultRegexAdm(scriptFile, expectedFile, actualFile);
                return;
            }
            String lineExpected, lineActual;
            int num = 1;
            while ((lineExpected = readerExpected.readLine()) != null) {
                lineActual = readerActual.readLine();
                // Assert.assertEquals(lineExpected, lineActual);
                if (lineActual == null) {
                    if (lineExpected.isEmpty()) {
                        continue;
                    }
                    throw createLineChangedException(scriptFile, lineExpected, "<EOF> after " + num + " lines", num);
                }

                // Comparing result equality but ignore "Time"-prefixed fields. (for metadata tests.)
                String[] lineSplitsExpected = lineExpected.split("Time");
                String[] lineSplitsActual = lineActual.split("Time");
                if (lineSplitsExpected.length != lineSplitsActual.length) {
                    throw createLineChangedException(scriptFile, lineExpected, lineActual, num);
                }
                if (!equalStrings(lineSplitsExpected[0], lineSplitsActual[0], regex)) {
                    throw createLineChangedException(scriptFile, lineExpected, lineActual, num);
                }

                for (int i = 1; i < lineSplitsExpected.length; i++) {
                    String[] splitsByCommaExpected = lineSplitsExpected[i].split(",");
                    String[] splitsByCommaActual = lineSplitsActual[i].split(",");
                    if (splitsByCommaExpected.length != splitsByCommaActual.length) {
                        throw createLineChangedException(scriptFile, lineExpected, lineActual, num);
                    }
                    for (int j = 1; j < splitsByCommaExpected.length; j++) {
                        if (splitsByCommaExpected[j].indexOf("DatasetId") >= 0) {
                            // Ignore the field "DatasetId", which is different for different runs.
                            // (for metadata tests)
                            continue;
                        }
                        if (!equalStrings(splitsByCommaExpected[j], splitsByCommaActual[j], regex)) {
                            throw createLineChangedException(scriptFile, lineExpected, lineActual, num);
                        }
                    }
                }

                ++num;
            }
            lineActual = readerActual.readLine();
            if (lineActual != null) {
                throw createLineChangedException(scriptFile, "expected <EOF>", lineActual, num);
            }
        } catch (Exception e) {
            System.err.println("Actual results file: " + actualFile.toString());
            throw e;
        } finally {
            readerExpected.close();
            readerActual.close();
        }

    }

    private ComparisonException createLineChangedException(File scriptFile, String lineExpected, String lineActual,
            int num) {
        return new ComparisonException("Result for " + scriptFile + " changed at line " + num + ":\n< "
                + truncateIfLong(lineExpected) + "\n> " + truncateIfLong(lineActual));
    }

    private String truncateIfLong(String string) {
        if (string.length() < TRUNCATE_THRESHOLD) {
            return string;
        }
        final StringBuilder truncatedString = new StringBuilder(string);
        truncatedString.setLength(TRUNCATE_THRESHOLD);
        truncatedString.append("\n<truncated ")
                .append(StorageUtil.toHumanReadableSize(string.length() - TRUNCATE_THRESHOLD)).append("...>");
        return truncatedString.toString();
    }

    private boolean equalStrings(String expected, String actual, boolean regexMatch) {
        String[] rowsExpected = expected.split("\n");
        String[] rowsActual = actual.split("\n");

        for (int i = 0; i < rowsExpected.length; i++) {
            String expectedRow = rowsExpected[i];
            String actualRow = rowsActual[i];

            if (regexMatch) {
                if (actualRow.matches(expectedRow)) {
                    continue;
                }
            } else if (actualRow.equals(expectedRow)) {
                continue;
            }

            String[] expectedFields = expectedRow.split(" ");
            String[] actualFields = actualRow.split(" ");

            boolean bagEncountered = false;
            Set<String> expectedBagElements = new HashSet<>();
            Set<String> actualBagElements = new HashSet<>();

            for (int j = 0; j < expectedFields.length; j++) {
                if (j >= actualFields.length) {
                    return false;
                } else if (expectedFields[j].equals(actualFields[j])) {
                    bagEncountered = expectedFields[j].equals("{{");
                    if (expectedFields[j].startsWith("}}")) {
                        if (regexMatch) {
                            if (expectedBagElements.size() != actualBagElements.size()) {
                                return false;
                            }
                            int[] expectedHits = new int[expectedBagElements.size()];
                            int[] actualHits = new int[actualBagElements.size()];
                            int k = 0;
                            for (String expectedElement : expectedBagElements) {
                                int l = 0;
                                for (String actualElement : actualBagElements) {
                                    if (actualElement.matches(expectedElement)) {
                                        expectedHits[k]++;
                                        actualHits[l]++;
                                    }
                                    l++;
                                }
                                k++;
                            }
                            for (int m = 0; m < expectedHits.length; m++) {
                                if (expectedHits[m] == 0 || actualHits[m] == 0) {
                                    return false;
                                }
                            }
                        } else if (!expectedBagElements.equals(actualBagElements)) {
                            return false;
                        }
                        bagEncountered = false;
                        expectedBagElements.clear();
                        actualBagElements.clear();
                    }
                } else if (expectedFields[j].indexOf('.') < 0) {
                    if (bagEncountered) {
                        expectedBagElements.add(expectedFields[j].replaceAll(",$", ""));
                        actualBagElements.add(actualFields[j].replaceAll(",$", ""));
                        continue;
                    }
                    return false;
                } else {
                    // If the fields are floating-point numbers, test them
                    // for equality safely
                    expectedFields[j] = expectedFields[j].split(",")[0];
                    actualFields[j] = actualFields[j].split(",")[0];
                    try {
                        Double double1 = Double.parseDouble(expectedFields[j]);
                        Double double2 = Double.parseDouble(actualFields[j]);
                        float float1 = (float) double1.doubleValue();
                        float float2 = (float) double2.doubleValue();

                        if (Math.abs(float1 - float2) == 0) {
                            continue;
                        } else {
                            return false;
                        }
                    } catch (NumberFormatException ignored) {
                        // Guess they weren't numbers - must simply not be equal
                        return false;
                    }
                }
            }
        }
        return true;
    }

    public void runScriptAndCompareWithResultRegex(File scriptFile, File expectedFile, File actualFile)
            throws Exception {
        String lineExpected, lineActual;
        try (BufferedReader readerExpected =
                new BufferedReader(new InputStreamReader(new FileInputStream(expectedFile), "UTF-8"));
                BufferedReader readerActual =
                        new BufferedReader(new InputStreamReader(new FileInputStream(actualFile), "UTF-8"))) {
            StringBuilder actual = new StringBuilder();
            while ((lineActual = readerActual.readLine()) != null) {
                actual.append(lineActual).append('\n');
            }
            while ((lineExpected = readerExpected.readLine()) != null) {
                if ("".equals(lineExpected.trim())) {
                    continue;
                }
                Matcher m = REGEX_LINES_PATTERN.matcher(lineExpected);
                if (!m.matches()) {
                    throw new IllegalArgumentException(
                            "Each line of regex file must conform to: [-]/regex/[flags]: " + expectedFile);
                }
                String negateStr = m.group(1);
                String expression = m.group(2);
                String flagStr = m.group(3);
                boolean negate = "-".equals(negateStr);
                int flags = Pattern.MULTILINE;
                if (flagStr.contains("m")) {
                    flags |= Pattern.DOTALL;
                }
                if (flagStr.contains("i")) {
                    flags |= Pattern.CASE_INSENSITIVE;
                }
                Pattern linePattern = Pattern.compile(expression, flags);
                boolean match = linePattern.matcher(actual).find();
                if (match && !negate || negate && !match) {
                    continue;
                }
                throw new Exception("Result for " + scriptFile + ": expected pattern '" + expression
                        + "' not found in result: " + actual);
            }
        }
    }

    public void runScriptAndCompareWithResultRegexAdm(File scriptFile, File expectedFile, File actualFile)
            throws Exception {
        StringWriter actual = new StringWriter();
        StringWriter expected = new StringWriter();
        IOUtils.copy(new FileInputStream(actualFile), actual, StandardCharsets.UTF_8);
        IOUtils.copy(new FileInputStream(expectedFile), expected, StandardCharsets.UTF_8);
        Pattern pattern = Pattern.compile(expected.toString(), Pattern.DOTALL | Pattern.MULTILINE);
        if (!pattern.matcher(actual.toString()).matches()) {
            // figure out where the problem first occurs...
            StringBuilder builder = new StringBuilder();
            String[] lines = expected.toString().split("\\n");
            int endOfMatch = 0;
            final StringBuffer actualBuffer = actual.getBuffer();
            for (int i = 0; i < lines.length; i++) {
                builder.append(lines[i]).append('\n');
                Pattern partPatten = Pattern.compile(builder.toString(), Pattern.DOTALL | Pattern.MULTILINE);
                final Matcher matcher = partPatten.matcher(actualBuffer);
                if (!matcher.lookingAt()) {
                    final int eol = actualBuffer.indexOf("\n", endOfMatch);
                    String actualLine = actualBuffer.substring(endOfMatch, eol == -1 ? actualBuffer.length() : eol);
                    throw createLineChangedException(scriptFile, lines[i], actualLine, i + 1);
                }
                endOfMatch = matcher.end();
            }
            throw new Exception("Result for " + scriptFile + ": actual file did not match expected result");
        }
    }

    // For tests where you simply want the byte-for-byte output.
    private static void writeOutputToFile(File actualFile, InputStream resultStream) throws Exception {
        final File parentDir = actualFile.getParentFile();
        if (!parentDir.isDirectory()) {
            if (parentDir.exists()) {
                LOGGER.warning("Actual file parent \"" + parentDir + "\" exists but is not a directory");
            } else if (!parentDir.mkdirs()) {
                LOGGER.warning("Unable to create actual file parent dir: " + parentDir);
            }
        }
        try (FileOutputStream out = new FileOutputStream(actualFile)) {
            IOUtils.copy(resultStream, out);
        }
    }

    protected HttpResponse executeAndCheckHttpRequest(HttpUriRequest method) throws Exception {
        return checkResponse(executeHttpRequest(method), code -> code == HttpStatus.SC_OK);
    }

    protected HttpResponse executeAndCheckHttpRequest(HttpUriRequest method, Predicate<Integer> responseCodeValidator)
            throws Exception {
        return checkResponse(executeHttpRequest(method), responseCodeValidator);
    }

    protected HttpResponse executeHttpRequest(HttpUriRequest method) throws Exception {
        HttpClient client = HttpClients.custom().setRetryHandler(StandardHttpRequestRetryHandler.INSTANCE).build();
        try {
            return client.execute(method, getHttpContext());
        } catch (Exception e) {
            GlobalConfig.ASTERIX_LOGGER.log(Level.SEVERE, e.getMessage(), e);
            e.printStackTrace();
            throw e;
        }
    }

    protected HttpContext getHttpContext() {
        return null;
    }

    protected HttpResponse checkResponse(HttpResponse httpResponse, Predicate<Integer> responseCodeValidator)
            throws Exception {
        if (!responseCodeValidator.test(httpResponse.getStatusLine().getStatusCode())) {
            String errorBody = EntityUtils.toString(httpResponse.getEntity());
            String[] errors;
            try {
                // First try to parse the response for a JSON error response.
                ObjectMapper om = new ObjectMapper();
                JsonNode result = om.readTree(errorBody);
                errors = new String[] { result.get("error-code").get(1).asText(), result.get("summary").asText(),
                        result.get("stacktrace").asText() };
            } catch (Exception e) {
                // whoops, not JSON (e.g. 404) - just include the body
                GlobalConfig.ASTERIX_LOGGER.log(Level.SEVERE, errorBody);
                Exception failure = new Exception("HTTP operation failed:" + "\nSTATUS LINE: "
                        + httpResponse.getStatusLine() + "\nERROR_BODY: " + errorBody);
                failure.addSuppressed(e);
                throw failure;
            }
            throw new ParsedException("HTTP operation failed: " + errors[0] + "\nSTATUS LINE: "
                    + httpResponse.getStatusLine() + "\nSUMMARY: " + errors[2].split("\n")[0], errors[2]);
        }
        return httpResponse;
    }

    static class ParsedException extends Exception {

        private final String savedStack;

        ParsedException(String message, String stackTrace) {
            super(message);
            savedStack = stackTrace;
        }

        @Override
        public String toString() {
            return getMessage();
        }

        @Override
        public void printStackTrace(PrintStream s) {
            super.printStackTrace(s);
            s.println("Caused by: " + savedStack);
        }

        @Override
        public void printStackTrace(PrintWriter s) {
            super.printStackTrace(s);
            s.println("Caused by: " + savedStack);
        }
    }

    public InputStream executeQuery(String str, OutputFormat fmt, URI uri, List<CompilationUnit.Parameter> params)
            throws Exception {
        HttpUriRequest method = constructHttpMethod(str, uri, "query", false, params);
        // Set accepted output response type
        method.setHeader("Accept", fmt.mimeType());
        HttpResponse response = executeAndCheckHttpRequest(method);
        return response.getEntity().getContent();
    }

    public InputStream executeQueryService(String str, URI uri, OutputFormat fmt) throws Exception {
        return executeQueryService(str, fmt, uri, new ArrayList<>(), false);
    }

    public InputStream executeQueryService(String str, OutputFormat fmt, URI uri,
            List<CompilationUnit.Parameter> params, boolean jsonEncoded) throws Exception {
        return executeQueryService(str, fmt, uri, params, jsonEncoded, null, false);
    }

    public InputStream executeQueryService(String str, OutputFormat fmt, URI uri,
            List<CompilationUnit.Parameter> params, boolean jsonEncoded, Predicate<Integer> responseCodeValidator)
            throws Exception {
        return executeQueryService(str, fmt, uri, params, jsonEncoded, responseCodeValidator, false);
    }

    protected InputStream executeQueryService(String str, OutputFormat fmt, URI uri,
            List<CompilationUnit.Parameter> params, boolean jsonEncoded, Predicate<Integer> responseCodeValidator,
            boolean cancellable) throws Exception {
        final List<CompilationUnit.Parameter> newParams = upsertParam(params, "format", fmt.mimeType());
        HttpUriRequest method = jsonEncoded ? constructPostMethodJson(str, uri, "statement", newParams)
                : constructPostMethodUrl(str, uri, "statement", newParams);
        // Set accepted output response type
        method.setHeader("Accept", OutputFormat.CLEAN_JSON.mimeType());
        HttpResponse response = executeHttpRequest(method);
        if (responseCodeValidator != null) {
            checkResponse(response, responseCodeValidator);
        }
        return response.getEntity().getContent();
    }

    protected List<CompilationUnit.Parameter> upsertParam(List<CompilationUnit.Parameter> params, String name,
            String value) {
        boolean replaced = false;
        List<CompilationUnit.Parameter> result = new ArrayList<>();
        for (CompilationUnit.Parameter param : params) {
            CompilationUnit.Parameter newParam = new CompilationUnit.Parameter();
            newParam.setName(param.getName());
            if (name.equals(param.getName())) {
                newParam.setValue(value);
                replaced = true;
            } else {
                newParam.setValue(param.getValue());
            }
            result.add(newParam);
        }
        if (!replaced) {
            CompilationUnit.Parameter newParam = new CompilationUnit.Parameter();
            newParam.setName(name);
            newParam.setValue(value);
            result.add(newParam);
        }
        return result;
    }

    private HttpUriRequest constructHttpMethod(String statement, URI uri, String stmtParam, boolean postStmtAsParam,
            List<CompilationUnit.Parameter> otherParams) throws URISyntaxException {
        if (statement.length() + uri.toString().length() < MAX_URL_LENGTH) {
            // Use GET for small-ish queries
            return constructGetMethod(uri, upsertParam(otherParams, stmtParam, statement));
        } else {
            // Use POST for bigger ones to avoid 413 FULL_HEAD
            String stmtParamName = (postStmtAsParam ? stmtParam : null);
            return constructPostMethodUrl(statement, uri, stmtParamName, otherParams);
        }
    }

    private HttpUriRequest constructGetMethod(URI endpoint, List<CompilationUnit.Parameter> params) {
        RequestBuilder builder = RequestBuilder.get(endpoint);
        for (CompilationUnit.Parameter param : params) {
            builder.addParameter(param.getName(), param.getValue());
        }
        builder.setCharset(StandardCharsets.UTF_8);
        return builder.build();
    }

    private HttpUriRequest constructGetMethod(URI endpoint, OutputFormat fmt, List<CompilationUnit.Parameter> params) {
        HttpUriRequest method = constructGetMethod(endpoint, params);
        // Set accepted output response type
        method.setHeader("Accept", fmt.mimeType());
        return method;
    }

    private HttpUriRequest constructPostMethod(URI uri, List<CompilationUnit.Parameter> params) {
        RequestBuilder builder = RequestBuilder.post(uri);
        for (CompilationUnit.Parameter param : params) {
            builder.addParameter(param.getName(), param.getValue());
        }
        builder.setCharset(StandardCharsets.UTF_8);
        return builder.build();
    }

    private HttpUriRequest constructPostMethod(URI uri, OutputFormat fmt, List<CompilationUnit.Parameter> params) {
        HttpUriRequest method = constructPostMethod(uri, params);
        // Set accepted output response type
        method.setHeader("Accept", fmt.mimeType());
        return method;
    }

    protected HttpUriRequest constructPostMethodUrl(String statement, URI uri, String stmtParam,
            List<CompilationUnit.Parameter> otherParams) {
        RequestBuilder builder = RequestBuilder.post(uri);
        if (stmtParam != null) {
            for (CompilationUnit.Parameter param : upsertParam(otherParams, stmtParam, statement)) {
                builder.addParameter(param.getName(), param.getValue());
            }
            builder.addParameter(stmtParam, statement);
        } else {
            // this seems pretty bad - we should probably fix the API and not the client
            builder.setEntity(new StringEntity(statement, StandardCharsets.UTF_8));
        }
        builder.setCharset(StandardCharsets.UTF_8);
        return builder.build();
    }

    protected HttpUriRequest constructPostMethodJson(String statement, URI uri, String stmtParam,
            List<CompilationUnit.Parameter> otherParams) {
        if (stmtParam == null) {
            throw new NullPointerException("Statement parameter required.");
        }
        RequestBuilder builder = RequestBuilder.post(uri);
        ObjectMapper om = new ObjectMapper();
        ObjectNode content = om.createObjectNode();
        for (CompilationUnit.Parameter param : upsertParam(otherParams, stmtParam, statement)) {
            content.put(param.getName(), param.getValue());
        }
        try {
            builder.setEntity(new StringEntity(om.writeValueAsString(content), ContentType.APPLICATION_JSON));
        } catch (JsonProcessingException e) {
            e.printStackTrace();
        }
        builder.setCharset(StandardCharsets.UTF_8);
        return builder.build();
    }

    public InputStream executeJSONGet(OutputFormat fmt, URI uri) throws Exception {
        return executeJSONGet(fmt, uri, code -> code == HttpStatus.SC_OK);
    }

    public InputStream executeJSONGet(OutputFormat fmt, URI uri, Predicate<Integer> responseCodeValidator)
            throws Exception {
        HttpUriRequest request = constructGetMethod(uri, fmt, new ArrayList<>());
        HttpResponse response = executeAndCheckHttpRequest(request, responseCodeValidator);
        return response.getEntity().getContent();
    }

    public InputStream executeJSONPost(OutputFormat fmt, URI uri) throws Exception {
        return executeJSONPost(fmt, uri, code -> code == HttpStatus.SC_OK);
    }

    public InputStream executeJSONPost(OutputFormat fmt, URI uri, Predicate<Integer> responseCodeValidator)
            throws Exception {
        HttpUriRequest request = constructPostMethod(uri, fmt, new ArrayList<>());
        HttpResponse response = executeAndCheckHttpRequest(request, responseCodeValidator);
        return response.getEntity().getContent();
    }

    // To execute Update statements
    // Insert and Delete statements are executed here
    public void executeUpdate(String str, URI uri) throws Exception {
        // Create a method instance.
        HttpUriRequest request =
                RequestBuilder.post(uri).setEntity(new StringEntity(str, StandardCharsets.UTF_8)).build();

        // Execute the method.
        executeAndCheckHttpRequest(request);
    }

    // Executes AQL in either async or async-defer mode.
    public InputStream executeAnyAQLAsync(String statement, boolean defer, OutputFormat fmt, URI uri,
            Map<String, Object> variableCtx) throws Exception {
        // Create a method instance.
        HttpUriRequest request =
                RequestBuilder.post(uri).addParameter("mode", defer ? "asynchronous-deferred" : "asynchronous")
                        .setEntity(new StringEntity(statement, StandardCharsets.UTF_8))
                        .setHeader("Accept", fmt.mimeType()).build();

        String handleVar = getHandleVariable(statement);

        HttpResponse response = executeAndCheckHttpRequest(request);
        InputStream resultStream = response.getEntity().getContent();
        String resultStr = IOUtils.toString(resultStream, "UTF-8");
        ObjectNode resultJson = new ObjectMapper().readValue(resultStr, ObjectNode.class);
        final JsonNode jsonHandle = resultJson.get("handle");
        final String strHandle = jsonHandle.asText();

        if (handleVar != null) {
            variableCtx.put(handleVar, strHandle);
            return resultStream;
        }
        return null;
    }

    // To execute DDL and Update statements
    // create type statement
    // create dataset statement
    // create index statement
    // create dataverse statement
    // create function statement
    public void executeDDL(String str, URI uri) throws Exception {
        // Create a method instance.
        HttpUriRequest request =
                RequestBuilder.post(uri).setEntity(new StringEntity(str, StandardCharsets.UTF_8)).build();

        // Execute the method.
        executeAndCheckHttpRequest(request);
    }

    // Method that reads a DDL/Update/Query File
    // and returns the contents as a string
    // This string is later passed to REST API for execution.
    public String readTestFile(File testFile) throws Exception {
        BufferedReader reader =
                new BufferedReader(new InputStreamReader(new FileInputStream(testFile), StandardCharsets.UTF_8));
        String line;
        StringBuilder stringBuilder = new StringBuilder();
        String ls = System.getProperty("line.separator");
        while ((line = reader.readLine()) != null) {
            stringBuilder.append(line);
            stringBuilder.append(ls);
        }
        reader.close();
        return stringBuilder.toString();
    }

    public static void executeManagixCommand(String command) throws ClassNotFoundException, NoSuchMethodException,
            SecurityException, IllegalAccessException, IllegalArgumentException, InvocationTargetException {
        if (managixExecuteMethod == null) {
            Class<?> clazz = Class.forName("org.apache.asterix.installer.test.AsterixInstallerIntegrationUtil");
            managixExecuteMethod = clazz.getMethod("executeCommand", String.class);
        }
        managixExecuteMethod.invoke(null, command);
    }

    public static String executeScript(ProcessBuilder pb, String scriptPath) throws Exception {
        LOGGER.info("Executing script: " + scriptPath);
        pb.command(scriptPath);
        Process p = pb.start();
        return getProcessOutput(p);
    }

    private static String executeVagrantScript(ProcessBuilder pb, String node, String scriptName) throws Exception {
        pb.command("vagrant", "ssh", node, "--", pb.environment().get("SCRIPT_HOME") + scriptName);
        Process p = pb.start();
        p.waitFor();
        InputStream input = p.getInputStream();
        return IOUtils.toString(input, StandardCharsets.UTF_8.name());
    }

    private static String executeVagrantManagix(ProcessBuilder pb, String command) throws Exception {
        pb.command("vagrant", "ssh", "cc", "--", pb.environment().get("MANAGIX_HOME") + command);
        Process p = pb.start();
        p.waitFor();
        InputStream input = p.getInputStream();
        return IOUtils.toString(input, StandardCharsets.UTF_8.name());
    }

    private static String getScriptPath(String queryPath, String scriptBasePath, String scriptFileName) {
        String targetWord = "queries" + File.separator;
        int targetWordSize = targetWord.lastIndexOf(File.separator);
        int beginIndex = queryPath.lastIndexOf(targetWord) + targetWordSize;
        int endIndex = queryPath.lastIndexOf(File.separator);
        String prefix = queryPath.substring(beginIndex, endIndex);
        return scriptBasePath + prefix + File.separator + scriptFileName;
    }

    private static String getProcessOutput(Process p) throws Exception {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        Future<Integer> future =
                Executors.newSingleThreadExecutor().submit(() -> IOUtils.copy(p.getInputStream(), new OutputStream() {
                    @Override
                    public void write(int b) throws IOException {
                        baos.write(b);
                        System.out.write(b);
                    }

                    @Override
                    public void flush() throws IOException {
                        baos.flush();
                        System.out.flush();
                    }

                    @Override
                    public void close() throws IOException {
                        baos.close();
                        System.out.close();
                    }
                }));
        p.waitFor();
        future.get();
        ByteArrayInputStream bisIn = new ByteArrayInputStream(baos.toByteArray());
        StringWriter writerIn = new StringWriter();
        IOUtils.copy(bisIn, writerIn, StandardCharsets.UTF_8);
        StringWriter writerErr = new StringWriter();
        IOUtils.copy(p.getErrorStream(), writerErr, StandardCharsets.UTF_8);

        StringBuffer stdOut = writerIn.getBuffer();
        if (writerErr.getBuffer().length() > 0) {
            StringBuilder sbErr = new StringBuilder();
            sbErr.append("script execution failed - error message:\n" + "-------------------------------------------\n"
                    + "stdout: ").append(stdOut).append("\nstderr: ").append(writerErr.getBuffer())
                    .append("-------------------------------------------");
            LOGGER.info(sbErr.toString());
            throw new Exception(sbErr.toString());
        }
        return stdOut.toString();
    }

    public void executeTest(String actualPath, TestCaseContext testCaseCtx, ProcessBuilder pb,
            boolean isDmlRecoveryTest) throws Exception {
        executeTest(actualPath, testCaseCtx, pb, isDmlRecoveryTest, null);
    }

    public void executeTestFile(TestCaseContext testCaseCtx, TestFileContext ctx, Map<String, Object> variableCtx,
            String statement, boolean isDmlRecoveryTest, ProcessBuilder pb, CompilationUnit cUnit,
            MutableInt queryCount, List<TestFileContext> expectedResultFileCtxs, File testFile, String actualPath)
            throws Exception {
        File qbcFile;
        boolean failed = false;
        File expectedResultFile;
        switch (ctx.getType()) {
            case "ddl":
                if (ctx.getFile().getName().endsWith("aql")) {
                    executeDDL(statement, getEndpoint(Servlets.AQL_DDL));
                } else {
                    executeSqlppUpdateOrDdl(statement, OutputFormat.CLEAN_JSON);
                }
                break;
            case "update":
                // isDmlRecoveryTest: set IP address
                if (isDmlRecoveryTest && statement.contains("nc1://")) {
                    statement = statement.replaceAll("nc1://", "127.0.0.1://../../../../../../asterix-app/");
                }
                if (ctx.getFile().getName().endsWith("aql")) {
                    executeUpdate(statement, getEndpoint(Servlets.AQL_UPDATE));
                } else {
                    executeSqlppUpdateOrDdl(statement, OutputFormat.forCompilationUnit(cUnit));
                }
                break;
            case "pollget":
            case "pollquery":
                poll(testCaseCtx, ctx, variableCtx, statement, isDmlRecoveryTest, pb, cUnit, queryCount,
                        expectedResultFileCtxs, testFile, actualPath);
                break;
            case "query":
            case "async":
            case "deferred":
                // isDmlRecoveryTest: insert Crash and Recovery
                if (isDmlRecoveryTest) {
                    executeScript(pb, pb.environment().get("SCRIPT_HOME") + File.separator + "dml_recovery"
                            + File.separator + "kill_cc_and_nc.sh");
                    executeScript(pb, pb.environment().get("SCRIPT_HOME") + File.separator + "dml_recovery"
                            + File.separator + "stop_and_start.sh");
                }
                expectedResultFile = (queryCount.intValue() >= expectedResultFileCtxs.size()) ? null
                        : expectedResultFileCtxs.get(queryCount.intValue()).getFile();
                File actualResultFile = expectedResultFile == null ? null
                        : testCaseCtx.getActualResultFile(cUnit, expectedResultFile, new File(actualPath));
                executeQuery(OutputFormat.forCompilationUnit(cUnit), statement, variableCtx, ctx.getType(), testFile,
                        expectedResultFile, actualResultFile, queryCount, expectedResultFileCtxs.size(),
                        cUnit.getParameter(), ComparisonEnum.TEXT);
                break;
            case "mgx":
                executeManagixCommand(stripLineComments(statement).trim());
                Thread.sleep(8000);
                break;
            case "txnqbc": // qbc represents query before crash
                InputStream resultStream = executeQuery(statement, OutputFormat.forCompilationUnit(cUnit),
                        getEndpoint(Servlets.AQL_QUERY), cUnit.getParameter());
                qbcFile = getTestCaseQueryBeforeCrashFile(actualPath, testCaseCtx, cUnit);
                writeOutputToFile(qbcFile, resultStream);
                break;
            case "txnqar": // qar represents query after recovery
                resultStream = executeQuery(statement, OutputFormat.forCompilationUnit(cUnit),
                        getEndpoint(Servlets.AQL_QUERY), cUnit.getParameter());
                File qarFile = new File(actualPath + File.separator
                        + testCaseCtx.getTestCase().getFilePath().replace(File.separator, "_") + "_" + cUnit.getName()
                        + "_qar.adm");
                writeOutputToFile(qarFile, resultStream);
                qbcFile = getTestCaseQueryBeforeCrashFile(actualPath, testCaseCtx, cUnit);
                runScriptAndCompareWithResult(testFile, new PrintWriter(System.err), qbcFile, qarFile,
                        ComparisonEnum.TEXT);
                break;
            case "txneu": // eu represents erroneous update
                try {
                    executeUpdate(statement, getEndpoint(Servlets.AQL_UPDATE));
                } catch (Exception e) {
                    // An exception is expected.
                    failed = true;
                    System.err.println("testFile " + testFile.toString() + " raised an exception: " + e);
                }
                if (!failed) {
                    throw new Exception("Test \"" + testFile + "\" FAILED!\n  An exception" + "is expected.");
                }
                System.err.println("...but that was expected.");
                break;
            case "script":
                try {
                    String output = executeScript(pb, getScriptPath(testFile.getAbsolutePath(),
                            pb.environment().get("SCRIPT_HOME"), stripLineComments(statement).trim()));
                    if (output.contains("ERROR")) {
                        throw new Exception(output);
                    }
                } catch (Exception e) {
                    throw new Exception("Test \"" + testFile + "\" FAILED!\n", e);
                }
                break;
            case "sleep":
                String[] lines = stripLineComments(statement).trim().split("\n");
                Thread.sleep(Long.parseLong(lines[lines.length - 1].trim()));
                break;
            case "errddl": // a ddlquery that expects error
                try {
                    executeDDL(statement, getEndpoint(Servlets.AQL_DDL));
                } catch (Exception e) {
                    // expected error happens
                    failed = true;
                    System.err.println("testFile " + testFile.toString() + " raised an exception: " + e);
                }
                if (!failed) {
                    throw new Exception("Test \"" + testFile + "\" FAILED!\n  An exception is expected.");
                }
                System.err.println("...but that was expected.");
                break;
            case "vscript": // a script that will be executed on a vagrant virtual node
                try {
                    String[] command = stripLineComments(statement).trim().split(" ");
                    if (command.length != 2) {
                        throw new Exception("invalid vagrant script format");
                    }
                    String nodeId = command[0];
                    String scriptName = command[1];
                    String output = executeVagrantScript(pb, nodeId, scriptName);
                    if (output.contains("ERROR")) {
                        throw new Exception(output);
                    }
                } catch (Exception e) {
                    throw new Exception("Test \"" + testFile + "\" FAILED!\n", e);
                }
                break;
            case "vmgx": // a managix command that will be executed on vagrant cc node
                String output = executeVagrantManagix(pb, stripLineComments(statement).trim());
                if (output.contains("ERROR")) {
                    throw new Exception(output);
                }
                break;
            case "get":
            case "post":
                expectedResultFile = (queryCount.intValue() >= expectedResultFileCtxs.size()) ? null
                        : expectedResultFileCtxs.get(queryCount.intValue()).getFile();
                actualResultFile = expectedResultFile == null ? null
                        : testCaseCtx.getActualResultFile(cUnit, expectedResultFile, new File(actualPath));
                executeHttpRequest(OutputFormat.forCompilationUnit(cUnit), statement, variableCtx, ctx.getType(),
                        testFile, expectedResultFile, actualResultFile, queryCount, expectedResultFileCtxs.size(),
                        ctx.extension(), cUnit.getOutputDir().getCompare());
                break;
            case "server": // (start <test server name> <port>
                               // [<arg1>][<arg2>][<arg3>]...|stop (<port>|all))
                try {
                    lines = statement.trim().split("\n");
                    String[] command = lines[lines.length - 1].trim().split(" ");
                    if (command.length < 2) {
                        throw new Exception("invalid server command format. expected format ="
                                + " (start <test server name> <port> [<arg1>][<arg2>][<arg3>]"
                                + "...|stop (<port>|all))");
                    }
                    String action = command[0];
                    if (action.equals("start")) {
                        if (command.length < 3) {
                            throw new Exception("invalid server start command. expected format ="
                                    + " (start <test server name> <port> [<arg1>][<arg2>][<arg3>]...");
                        }
                        String name = command[1];
                        Integer port = new Integer(command[2]);
                        if (runningTestServers.containsKey(port)) {
                            throw new Exception("server with port " + port + " is already running");
                        }
                        ITestServer server = TestServerProvider.createTestServer(name, port);
                        server.configure(Arrays.copyOfRange(command, 3, command.length));
                        server.start();
                        runningTestServers.put(port, server);
                    } else if (action.equals("stop")) {
                        String target = command[1];
                        if (target.equals("all")) {
                            for (ITestServer server : runningTestServers.values()) {
                                server.stop();
                            }
                            runningTestServers.clear();
                        } else {
                            Integer port = new Integer(command[1]);
                            ITestServer server = runningTestServers.get(port);
                            if (server == null) {
                                throw new Exception("no server is listening to port " + port);
                            }
                            server.stop();
                            runningTestServers.remove(port);
                        }
                    } else {
                        throw new Exception("unknown server action");
                    }
                } catch (Exception e) {
                    throw new Exception("Test \"" + testFile + "\" FAILED!\n", e);
                }
                break;
            case "lib": // expected format <dataverse-name> <library-name>
                            // <library-directory>
                        // TODO: make this case work well with entity names containing spaces by
                        // looking for \"
                lines = statement.split("\n");
                String lastLine = lines[lines.length - 1];
                String[] command = lastLine.trim().split(" ");
                if (command.length < 3) {
                    throw new Exception("invalid library format");
                }
                String dataverse = command[1];
                String library = command[2];
                switch (command[0]) {
                    case "install":
                        if (command.length != 4) {
                            throw new Exception("invalid library format");
                        }
                        String libPath = command[3];
                        librarian.install(dataverse, library, libPath);
                        break;
                    case "uninstall":
                        if (command.length != 3) {
                            throw new Exception("invalid library format");
                        }
                        librarian.uninstall(dataverse, library);
                        break;
                    default:
                        throw new Exception("invalid library format");
                }
                break;
            case "node":
                command = stripJavaComments(statement).trim().split(" ");
                String commandType = command[0];
                String nodeId = command[1];
                if (commandType.equals("kill")) {
                    killNC(nodeId, cUnit);
                }
                break;
            default:
                throw new IllegalArgumentException("No statements of type " + ctx.getType());
        }
    }

    protected void executeHttpRequest(OutputFormat fmt, String statement, Map<String, Object> variableCtx,
            String reqType, File testFile, File expectedResultFile, File actualResultFile, MutableInt queryCount,
            int numResultFiles, String extension, ComparisonEnum compare) throws Exception {
        String handleVar = getHandleVariable(statement);
        final String trimmedPathAndQuery = stripLineComments(stripJavaComments(statement)).trim();
        final String variablesReplaced = replaceVarRef(trimmedPathAndQuery, variableCtx);
        InputStream resultStream;
        if ("http".equals(extension)) {
            resultStream = executeHttp(reqType, variablesReplaced, fmt);
        } else if ("uri".equals(extension)) {
            resultStream = executeURI(reqType, URI.create(variablesReplaced), fmt);
        } else {
            throw new IllegalArgumentException("Unexpected format for method " + reqType + ": " + extension);
        }
        if (handleVar != null) {
            String handle = ResultExtractor.extractHandle(resultStream);
            if (handle != null) {
                variableCtx.put(handleVar, handle);
            } else {
                throw new Exception("no handle for test " + testFile.toString());
            }
        } else {
            if (expectedResultFile == null) {
                Assert.fail("no result file for " + testFile.toString() + "; queryCount: " + queryCount
                        + ", filectxs.size: " + numResultFiles);
            }
            writeOutputToFile(actualResultFile, resultStream);
            runScriptAndCompareWithResult(testFile, new PrintWriter(System.err), expectedResultFile, actualResultFile,
                    compare);
        }
        queryCount.increment();
    }

    public void executeQuery(OutputFormat fmt, String statement, Map<String, Object> variableCtx, String reqType,
            File testFile, File expectedResultFile, File actualResultFile, MutableInt queryCount, int numResultFiles,
            List<CompilationUnit.Parameter> params, ComparisonEnum compare) throws Exception {
        InputStream resultStream = null;
        if (testFile.getName().endsWith("aql")) {
            if (reqType.equalsIgnoreCase("query")) {
                resultStream = executeQuery(statement, fmt, getEndpoint(Servlets.AQL_QUERY), params);
            } else {
                final URI endpoint = getEndpoint(Servlets.AQL);
                if (reqType.equalsIgnoreCase("async")) {
                    resultStream = executeAnyAQLAsync(statement, false, fmt, endpoint, variableCtx);
                } else if (reqType.equalsIgnoreCase("deferred")) {
                    resultStream = executeAnyAQLAsync(statement, true, fmt, endpoint, variableCtx);
                }
                Assert.assertNotNull("no handle for " + reqType + " test " + testFile.toString(), resultStream);
            }
        } else {
            String delivery = DELIVERY_IMMEDIATE;
            if (reqType.equalsIgnoreCase("async")) {
                delivery = DELIVERY_ASYNC;
            } else if (reqType.equalsIgnoreCase("deferred")) {
                delivery = DELIVERY_DEFERRED;
            }
            final URI uri = getEndpoint(Servlets.QUERY_SERVICE);
            if (DELIVERY_IMMEDIATE.equals(delivery)) {
                resultStream = executeQueryService(statement, fmt, uri, params, true, null, true);
                resultStream = ResultExtractor.extract(resultStream);
            } else {
                String handleVar = getHandleVariable(statement);
                resultStream = executeQueryService(statement, fmt, uri, upsertParam(params, "mode", delivery), true);
                String handle = ResultExtractor.extractHandle(resultStream);
                Assert.assertNotNull("no handle for " + reqType + " test " + testFile.toString(), handleVar);
                variableCtx.put(handleVar, handle);
            }
        }
        writeOutputToFile(actualResultFile, resultStream);
        if (expectedResultFile == null) {
            Assert.fail("no result file for " + testFile.toString() + "; queryCount: " + queryCount
                    + ", filectxs.size: " + numResultFiles);
        }

        runScriptAndCompareWithResult(testFile, new PrintWriter(System.err), expectedResultFile, actualResultFile,
                compare);
        queryCount.increment();

        // Deletes the matched result file.
        actualResultFile.getParentFile().delete();
    }

    private void poll(TestCaseContext testCaseCtx, TestFileContext ctx, Map<String, Object> variableCtx,
            String statement, boolean isDmlRecoveryTest, ProcessBuilder pb, CompilationUnit cUnit,
            MutableInt queryCount, List<TestFileContext> expectedResultFileCtxs, File testFile, String actualPath)
            throws Exception {
        // polltimeoutsecs=nnn, polldelaysecs=nnn
        int timeoutSecs = getTimeoutSecs(statement);
        int retryDelaySecs = getRetryDelaySecs(statement);
        long startTime = System.currentTimeMillis();
        long limitTime = startTime + TimeUnit.SECONDS.toMillis(timeoutSecs);
        ctx.setType(ctx.getType().substring("poll".length()));
        boolean expectedException = false;
        Exception finalException;
        LOGGER.fine("polling for up to " + timeoutSecs + " seconds w/ " + retryDelaySecs + " second(s) delay");
        int responsesReceived = 0;
        final ExecutorService executorService = Executors.newSingleThreadExecutor();
        while (true) {
            try {
                Future<Void> execution = executorService.submit(() -> {
                    executeTestFile(testCaseCtx, ctx, variableCtx, statement, isDmlRecoveryTest, pb, cUnit, queryCount,
                            expectedResultFileCtxs, testFile, actualPath);
                    return null;
                });
                execution.get(limitTime - System.currentTimeMillis(), TimeUnit.MILLISECONDS);
                responsesReceived++;
                finalException = null;
                break;
            } catch (TimeoutException e) {
                if (responsesReceived == 0) {
                    throw new Exception(
                            "Poll limit (" + timeoutSecs + "s) exceeded without obtaining *any* result from server");
                } else {
                    throw new Exception("Poll limit (" + timeoutSecs + "s) exceeded without obtaining expected result");

                }
            } catch (Exception e) {
                if (isExpected(e, cUnit)) {
                    expectedException = true;
                    finalException = e;
                    break;
                }
                if ((System.currentTimeMillis() > limitTime)) {
                    finalException = e;
                    break;
                }
                LOGGER.fine("sleeping " + retryDelaySecs + " second(s) before polling again");
                TimeUnit.SECONDS.sleep(retryDelaySecs);
            }
        }
        if (expectedException) {
            throw finalException;
        } else if (finalException != null) {
            throw new Exception("Poll limit (" + timeoutSecs + "s) exceeded without obtaining expected result",
                    finalException);
        }
    }

    public InputStream executeSqlppUpdateOrDdl(String statement, OutputFormat outputFormat) throws Exception {
        InputStream resultStream = executeQueryService(statement, getEndpoint(Servlets.QUERY_SERVICE), outputFormat);
        return ResultExtractor.extract(resultStream);
    }

    protected static boolean isExpected(Exception e, CompilationUnit cUnit) {
        final List<String> expErrors = cUnit.getExpectedError();
        for (String exp : expErrors) {
            if (e.toString().contains(exp)) {
                return true;
            }
        }
        return false;
    }

    public static int getTimeoutSecs(String statement) {
        final Matcher timeoutMatcher = POLL_TIMEOUT_PATTERN.matcher(statement);
        if (timeoutMatcher.find()) {
            return Integer.parseInt(timeoutMatcher.group(1));
        } else {
            throw new IllegalArgumentException("ERROR: polltimeoutsecs=nnn must be present in poll file");
        }
    }

    public static int getRetryDelaySecs(String statement) {
        final Matcher retryDelayMatcher = POLL_DELAY_PATTERN.matcher(statement);
        return retryDelayMatcher.find() ? Integer.parseInt(retryDelayMatcher.group(1)) : 1;
    }

    protected static String getHandleVariable(String statement) {
        final Matcher handleVariableMatcher = HANDLE_VARIABLE_PATTERN.matcher(statement);
        return handleVariableMatcher.find() ? handleVariableMatcher.group(1) : null;
    }

    protected static String replaceVarRef(String statement, Map<String, Object> variableCtx) {
        String tmpStmt = statement;
        Matcher variableReferenceMatcher = VARIABLE_REF_PATTERN.matcher(tmpStmt);
        while (variableReferenceMatcher.find()) {
            String var = variableReferenceMatcher.group(1);
            Object value = variableCtx.get(var);
            Assert.assertNotNull("No value for variable reference $" + var, value);
            tmpStmt = tmpStmt.replace("$" + var, String.valueOf(value));
            variableReferenceMatcher = VARIABLE_REF_PATTERN.matcher(tmpStmt);
        }
        return tmpStmt;
    }

    protected InputStream executeHttp(String ctxType, String endpoint, OutputFormat fmt) throws Exception {
        String[] split = endpoint.split("\\?");
        URI uri = createEndpointURI(split[0], split.length > 1 ? split[1] : null);
        return executeURI(ctxType, uri, fmt);
    }

    private InputStream executeURI(String ctxType, URI uri, OutputFormat fmt) throws Exception {
        switch (ctxType) {
            case "get":
                return executeJSONGet(fmt, uri);
            case "post":
                return executeJSONPost(fmt, uri);
            default:
                throw new AssertionError("Not implemented: " + ctxType);
        }
    }

    private void killNC(String nodeId, CompilationUnit cUnit) throws Exception {
        //get node process id
        OutputFormat fmt = OutputFormat.forCompilationUnit(cUnit);
        String endpoint = "/admin/cluster/node/" + nodeId + "/config";
        InputStream executeJSONGet = executeJSONGet(fmt, createEndpointURI(endpoint, null));
        StringWriter actual = new StringWriter();
        IOUtils.copy(executeJSONGet, actual, StandardCharsets.UTF_8);
        String config = actual.toString();
        int nodePid = new ObjectMapper().readValue(config, ObjectNode.class).get("pid").asInt();
        if (nodePid <= 1) {
            throw new IllegalArgumentException("Could not retrieve node pid from admin API");
        }
        ProcessBuilder pb = new ProcessBuilder("kill", "-9", Integer.toString(nodePid));
        pb.start().waitFor();
        // Delete NC's transaction logs to re-initialize it as a new NC.
        deleteNCTxnLogs(nodeId, cUnit);
    }

    private void deleteNCTxnLogs(String nodeId, CompilationUnit cUnit) throws Exception {
        OutputFormat fmt = OutputFormat.forCompilationUnit(cUnit);
        String endpoint = "/admin/cluster/node/" + nodeId + "/config";
        InputStream executeJSONGet = executeJSONGet(fmt, createEndpointURI(endpoint, null));
        StringWriter actual = new StringWriter();
        IOUtils.copy(executeJSONGet, actual, StandardCharsets.UTF_8);
        String config = actual.toString();
        ObjectMapper om = new ObjectMapper();
        String logDir = om.readTree(config).findPath("txn.log.dir").asText();
        FileUtils.deleteQuietly(new File(logDir));
    }

    public void executeTest(String actualPath, TestCaseContext testCaseCtx, ProcessBuilder pb,
            boolean isDmlRecoveryTest, TestGroup failedGroup) throws Exception {
        MutableInt queryCount = new MutableInt(0);
        int numOfErrors = 0;
        int numOfFiles = 0;
        List<CompilationUnit> cUnits = testCaseCtx.getTestCase().getCompilationUnit();
        for (CompilationUnit cUnit : cUnits) {
            List<String> expectedErrors = cUnit.getExpectedError();
            LOGGER.info(
                    "Starting [TEST]: " + testCaseCtx.getTestCase().getFilePath() + "/" + cUnit.getName() + " ... ");
            Map<String, Object> variableCtx = new HashMap<>();
            List<TestFileContext> testFileCtxs = testCaseCtx.getTestFiles(cUnit);
            List<TestFileContext> expectedResultFileCtxs = testCaseCtx.getExpectedResultFiles(cUnit);
            for (TestFileContext ctx : testFileCtxs) {
                numOfFiles++;
                final File testFile = ctx.getFile();
                final String statement = readTestFile(testFile);
                try {
                    executeTestFile(testCaseCtx, ctx, variableCtx, statement, isDmlRecoveryTest, pb, cUnit, queryCount,
                            expectedResultFileCtxs, testFile, actualPath);
                } catch (Exception e) {
                    System.err.println("testFile " + testFile.toString() + " raised an exception: " + e);
                    numOfErrors++;
                    if (isUnExpected(e, expectedErrors, numOfErrors, queryCount)) {
                        e.printStackTrace();
                        System.err.println("...Unexpected!");
                        if (failedGroup != null) {
                            failedGroup.getTestCase().add(testCaseCtx.getTestCase());
                        }
                        throw new Exception("Test \"" + testFile + "\" FAILED!");
                    }
                } finally {
                    if (numOfFiles == testFileCtxs.size()) {
                        if (numOfErrors < cUnit.getExpectedError().size()) {
                            System.err.println("...Unexpected!");
                            Exception e = new Exception(
                                    "Test \"" + cUnit.getName() + "\" FAILED!\nExpected error was not thrown...");
                            System.err.println(e);
                            throw e;
                        }
                        LOGGER.info("[TEST]: " + testCaseCtx.getTestCase().getFilePath() + "/" + cUnit.getName()
                                + " PASSED ");
                    }
                }
            }
        }
    }

    protected boolean isUnExpected(Exception e, List<String> expectedErrors, int numOfErrors, MutableInt queryCount) {
        String expectedError = null;
        if (expectedErrors.size() < numOfErrors) {
            return true;
        } else {
            // Get the expected exception
            expectedError = expectedErrors.get(numOfErrors - 1);
            if (e.toString().contains(expectedError)) {
                System.err.println("...but that was expected.");
                return false;
            } else {
                System.err
                        .println("Expected to find the following in error text:\n+++++\n" + expectedError + "\n+++++");
                return true;
            }
        }
    }

    private static File getTestCaseQueryBeforeCrashFile(String actualPath, TestCaseContext testCaseCtx,
            CompilationUnit cUnit) {
        return new File(
                actualPath + File.separator + testCaseCtx.getTestCase().getFilePath().replace(File.separator, "_") + "_"
                        + cUnit.getName() + "_qbc.adm");
    }

    protected URI createEndpointURI(String path, String query) throws URISyntaxException {
        int endpointIdx = Math.abs(endpointSelector++ % endpoints.size());
        InetSocketAddress endpoint = endpoints.get(endpointIdx);
        URI uri = new URI("http", null, endpoint.getHostString(), endpoint.getPort(), path, query, null);
        LOGGER.fine("Created endpoint URI: " + uri);
        return uri;
    }

    public URI getEndpoint(String servlet) throws URISyntaxException {
        return createEndpointURI(getPath(servlet).replaceAll("/\\*$", ""), null);
    }

    public static String stripJavaComments(String text) {
        return JAVA_BLOCK_COMMENT_PATTERN.matcher(text).replaceAll("");
    }

    public static String stripLineComments(String text) {
        final String s = SHELL_LINE_COMMENT_PATTERN.matcher(text).replaceAll("");
        return JAVA_LINE_COMMENT_PATTERN.matcher(s).replaceAll("");
    }

    public void cleanup(String testCase, List<String> badtestcases) throws Exception {
        try {
            ArrayList<String> toBeDropped = new ArrayList<>();
            InputStream resultStream = executeQueryService("select dv.DataverseName from Metadata.`Dataverse` as dv;",
                    getEndpoint(Servlets.QUERY_SERVICE), OutputFormat.CLEAN_JSON);
            String out = IOUtils.toString(resultStream);
            ObjectMapper om = new ObjectMapper();
            om.setConfig(om.getDeserializationConfig().with(DeserializationFeature.ACCEPT_EMPTY_STRING_AS_NULL_OBJECT));
            JsonNode result;
            try {
                result = om.readValue(out, ObjectNode.class).get("results");
            } catch (JsonMappingException e) {
                result = om.createArrayNode();
            }
            for (int i = 0; i < result.size(); i++) {
                JsonNode json = result.get(i);
                if (json != null) {
                    String dvName = json.get("DataverseName").asText();
                    if (!dvName.equals("Metadata") && !dvName.equals("Default")) {
                        toBeDropped.add(dvName);
                    }
                }
            }
            if (!toBeDropped.isEmpty()) {
                badtestcases.add(testCase);
                LOGGER.warning(
                        "Last test left some garbage. Dropping dataverses: " + StringUtils.join(toBeDropped, ','));
                StringBuilder dropStatement = new StringBuilder();
                for (String dv : toBeDropped) {
                    dropStatement.append("drop dataverse ");
                    dropStatement.append(dv);
                    dropStatement.append(";\n");
                }
                resultStream = executeQueryService(dropStatement.toString(), getEndpoint(Servlets.QUERY_SERVICE),
                        OutputFormat.CLEAN_JSON);
                ResultExtractor.extract(resultStream);
            }
        } catch (Throwable th) {
            th.printStackTrace();
            throw th;
        }
    }

    //This method is here to enable extension
    protected String getPath(String servlet) {
        return servlet;
    }

    public void waitForClusterActive(int timeoutSecs, TimeUnit timeUnit) throws Exception {
        waitForClusterState("ACTIVE", timeoutSecs, timeUnit);
    }

    public void waitForClusterState(String desiredState, int timeout, TimeUnit timeUnit) throws Exception {
        LOGGER.info("Waiting for cluster state " + desiredState + "...");
        Thread t = new Thread(() -> {
            while (true) {
                try {
                    final HttpClient client = HttpClients.createDefault();
                    final HttpGet get = new HttpGet(getEndpoint(Servlets.CLUSTER_STATE));
                    final HttpResponse httpResponse = client.execute(get, getHttpContext());
                    final int statusCode = httpResponse.getStatusLine().getStatusCode();
                    final String response = EntityUtils.toString(httpResponse.getEntity());
                    if (statusCode != HttpStatus.SC_OK) {
                        throw new Exception("HTTP error " + statusCode + ":\n" + response);
                    }
                    ObjectMapper om = new ObjectMapper();
                    ObjectNode result = (ObjectNode) om.readTree(response);
                    if (result.get("state").asText().matches(desiredState)) {
                        break;
                    }
                } catch (Exception e) {
                    // ignore, try again
                }
                try {
                    Thread.sleep(500);
                } catch (InterruptedException e) {
                    break;
                }
            }
        });
        t.start();
        timeUnit.timedJoin(t, timeout);
        if (t.isAlive()) {
            throw new Exception("Cluster did not become " + desiredState + " within " + timeout + " "
                    + timeUnit.name().toLowerCase());
        }
        LOGGER.info("Cluster state now " + desiredState);
    }

}
