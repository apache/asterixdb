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

import static java.nio.charset.StandardCharsets.UTF_8;

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
import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.CharBuffer;
import java.nio.charset.Charset;
import java.text.MessageFormat;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Predicate;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.asterix.api.http.server.QueryServiceServlet;
import org.apache.asterix.app.external.IExternalUDFLibrarian;
import org.apache.asterix.common.api.Duration;
import org.apache.asterix.common.config.GlobalConfig;
import org.apache.asterix.common.utils.Servlets;
import org.apache.asterix.lang.sqlpp.util.SqlppStatementUtil;
import org.apache.asterix.runtime.evaluators.common.NumberUtils;
import org.apache.asterix.test.server.ITestServer;
import org.apache.asterix.test.server.TestServerProvider;
import org.apache.asterix.testframework.context.TestCaseContext;
import org.apache.asterix.testframework.context.TestCaseContext.OutputFormat;
import org.apache.asterix.testframework.context.TestFileContext;
import org.apache.asterix.testframework.xml.ComparisonEnum;
import org.apache.asterix.testframework.xml.ParameterTypeEnum;
import org.apache.asterix.testframework.xml.TestCase.CompilationUnit;
import org.apache.asterix.testframework.xml.TestCase.CompilationUnit.Parameter;
import org.apache.asterix.testframework.xml.TestGroup;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;
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
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.client.StandardHttpRequestRetryHandler;
import org.apache.http.protocol.HttpContext;
import org.apache.http.util.EntityUtils;
import org.apache.hyracks.http.server.utils.HttpUtil;
import org.apache.hyracks.util.StorageUtil;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Assert;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.util.RawValue;

public class TestExecutor {

    /*
     * Static variables
     */
    protected static final Logger LOGGER = LogManager.getLogger();
    private static final String AQL = "aql";
    private static final String SQLPP = "sqlpp";
    private static final String DEFAULT_PLAN_FORMAT = "string";
    // see
    // https://stackoverflow.com/questions/417142/what-is-the-maximum-length-of-a-url-in-different-browsers/417184
    private static final long MAX_URL_LENGTH = 2000l;
    private static final Pattern JAVA_BLOCK_COMMENT_PATTERN =
            Pattern.compile("/\\*.*\\*/", Pattern.MULTILINE | Pattern.DOTALL);
    private static final Pattern JAVA_SHELL_SQL_LINE_COMMENT_PATTERN =
            Pattern.compile("^(//|#|--).*$", Pattern.MULTILINE);
    private static final Pattern REGEX_LINES_PATTERN = Pattern.compile("^(-)?/(.*)/([im]*)$");
    private static final Pattern POLL_TIMEOUT_PATTERN =
            Pattern.compile("polltimeoutsecs=(\\d+)(\\D|$)", Pattern.MULTILINE);
    private static final Pattern POLL_DELAY_PATTERN = Pattern.compile("polldelaysecs=(\\d+)(\\D|$)", Pattern.MULTILINE);
    private static final Pattern HANDLE_VARIABLE_PATTERN = Pattern.compile("handlevariable=(\\w+)");
    private static final Pattern VARIABLE_REF_PATTERN = Pattern.compile("\\$(\\w+)");
    private static final Pattern HTTP_PARAM_PATTERN =
            Pattern.compile("param (?<name>[\\w-$]+)(?::(?<type>\\w+))?=(?<value>.*)", Pattern.MULTILINE);
    private static final Pattern HTTP_BODY_PATTERN = Pattern.compile("body=(.*)", Pattern.MULTILINE);
    private static final Pattern HTTP_STATUSCODE_PATTERN = Pattern.compile("statuscode (.*)", Pattern.MULTILINE);
    private static final Pattern MAX_RESULT_READS_PATTERN =
            Pattern.compile("maxresultreads=(\\d+)(\\D|$)", Pattern.MULTILINE);
    private static final Pattern HTTP_REQUEST_TYPE = Pattern.compile("requesttype=(.*)", Pattern.MULTILINE);
    private static final Pattern EXTRACT_RESULT_TYPE = Pattern.compile("extractresult=(.*)", Pattern.MULTILINE);
    private static final String NC_ENDPOINT_PREFIX = "nc:";
    public static final int TRUNCATE_THRESHOLD = 16384;
    public static final Set<String> NON_CANCELLABLE =
            Collections.unmodifiableSet(new HashSet<>(Arrays.asList("store", "validate")));
    private static final int MAX_NON_UTF_8_STATEMENT_SIZE = 64 * 1024;
    private static final ContentType TEXT_PLAIN_UTF8 = ContentType.create(HttpUtil.ContentType.APPLICATION_JSON, UTF_8);

    private final IPollTask plainExecutor = (testCaseCtx, ctx, variableCtx, statement, isDmlRecoveryTest, pb, cUnit,
            queryCount, expectedResultFileCtxs, testFile, actualPath, actualWarnCount) -> executeTestFile(testCaseCtx,
                    ctx, variableCtx, statement, isDmlRecoveryTest, pb, cUnit, queryCount, expectedResultFileCtxs,
                    testFile, actualPath, actualWarnCount);

    public static final String DELIVERY_ASYNC = "async";
    public static final String DELIVERY_DEFERRED = "deferred";
    public static final String DELIVERY_IMMEDIATE = "immediate";
    public static final String DIAGNOSE = "diagnose";
    private static final String METRICS_QUERY_TYPE = "metrics";
    private static final String PROFILE_QUERY_TYPE = "profile";
    private static final String PLANS_QUERY_TYPE = "plans";

    private static final HashMap<Integer, ITestServer> runningTestServers = new HashMap<>();
    private static Map<String, InetSocketAddress> ncEndPoints;
    private static List<InetSocketAddress> ncEndPointsList = new ArrayList<>();
    private static Map<String, InetSocketAddress> replicationAddress;

    private final List<Charset> allCharsets;
    private final Queue<Charset> charsetsRemaining = new ArrayDeque<>();

    /*
     * Instance members
     */
    protected final List<InetSocketAddress> endpoints;
    protected int endpointSelector;
    protected IExternalUDFLibrarian librarian;
    private Map<File, TestLoop> testLoops = new HashMap<>();
    private double timeoutMultiplier = 1;

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
        this.allCharsets = Stream
                .of("UTF-8", "UTF-16", "UTF-16BE", "UTF-16LE", "UTF-32", "UTF-32BE", "UTF-32LE", "x-UTF-32BE-BOM",
                        "x-UTF-32LE-BOM", "x-UTF-16LE-BOM")
                .filter(Charset::isSupported).map(Charset::forName).collect(Collectors.toList());
    }

    public void setLibrarian(IExternalUDFLibrarian librarian) {
        this.librarian = librarian;
    }

    public void setNcEndPoints(Map<String, InetSocketAddress> ncEndPoints) {
        this.ncEndPoints = ncEndPoints;
        ncEndPointsList.addAll(ncEndPoints.values());
    }

    public void setNcReplicationAddress(Map<String, InetSocketAddress> replicationAddress) {
        this.replicationAddress = replicationAddress;
    }

    public void setTimeoutMultiplier(double timeoutMultiplier) {
        this.timeoutMultiplier = timeoutMultiplier;
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

    public void runScriptAndCompareWithResult(File scriptFile, File expectedFile, File actualFile,
            ComparisonEnum compare, Charset actualEncoding) throws Exception {
        LOGGER.info("Expected results file: {} ", expectedFile);
        boolean regex = false;
        try (BufferedReader readerExpected =
                new BufferedReader(new InputStreamReader(new FileInputStream(expectedFile), UTF_8));
                BufferedReader readerActual =
                        new BufferedReader(new InputStreamReader(new FileInputStream(actualFile), actualEncoding))) {
            if (ComparisonEnum.BINARY.equals(compare)) {
                if (!IOUtils.contentEquals(new FileInputStream(actualFile), new FileInputStream(expectedFile))) {
                    throw new Exception("Result for " + scriptFile + ": actual file did not match expected result");
                }
                return;
            } else if (actualFile.toString().endsWith(".regex")) {
                runScriptAndCompareWithResultRegex(scriptFile, readerExpected, readerActual);
                return;
            } else if (actualFile.toString().endsWith(".regexadm")) {
                runScriptAndCompareWithResultRegexAdm(scriptFile, readerExpected, readerActual);
                return;
            } else if (actualFile.toString().endsWith(".regexjson")) {
                ObjectMapper OM = new ObjectMapper();
                JsonNode expectedJson = OM.readTree(readerExpected);
                JsonNode actualJson = OM.readTree(readerActual);
                if (expectedJson == null || actualJson == null) {
                    throw new NullPointerException("Error parsing expected or actual result file for " + scriptFile);
                }
                if (!TestHelper.equalJson(expectedJson, actualJson)) {
                    throw new ComparisonException("Result for " + scriptFile + " didn't match the expected JSON"
                            + "\nexpected result:\n" + expectedJson + "\nactual result:\n" + actualJson);
                }
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
                    throw createLineChangedException(scriptFile, lineExpected, "<EOF>", num);
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
                throw createLineChangedException(scriptFile, "<EOF>", lineActual, num);
            }
        } catch (Exception e) {
            LOGGER.info("Actual results file: {} encoding: {}", actualFile, actualEncoding);
            throw e;
        }

    }

    private ComparisonException createLineChangedException(File scriptFile, String lineExpected, String lineActual,
            int num) {
        return new ComparisonException("Result for " + scriptFile + " changed at line " + num + ":\nexpected < "
                + truncateIfLong(lineExpected) + "\nactual   > " + truncateIfLong(lineActual));
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
                    // Get the String values
                    expectedFields[j] = expectedFields[j].split(",")[0];
                    actualFields[j] = actualFields[j].split(",")[0];

                    // Ensure type compatibility before value comparison
                    if (NumberUtils.isSameTypeNumericStrings(expectedFields[j], actualFields[j])) {
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

                    return false; // Not equal
                }
            }
        }
        return true;
    }

    public void runScriptAndCompareWithResultRegex(File scriptFile, BufferedReader readerExpected,
            BufferedReader readerActual) throws Exception {
        String lineExpected, lineActual;
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
                throw new IllegalArgumentException("Each line of regex file must conform to: [-]/regex/[flags]");
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

    public void runScriptAndCompareWithResultRegexAdm(File scriptFile, BufferedReader expectedFile,
            BufferedReader actualFile) throws Exception {
        StringWriter actual = new StringWriter();
        StringWriter expected = new StringWriter();
        IOUtils.copy(actualFile, actual);
        IOUtils.copy(expectedFile, expected);
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
                LOGGER.warn("Actual file parent \"" + parentDir + "\" exists but is not a directory");
            } else if (!parentDir.mkdirs()) {
                LOGGER.warn("Unable to create actual file parent dir: " + parentDir);
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
        // https://issues.apache.org/jira/browse/ASTERIXDB-2315
        ExecutorService executor = Executors.newSingleThreadExecutor();
        CloseableHttpClient client =
                HttpClients.custom().setRetryHandler(StandardHttpRequestRetryHandler.INSTANCE).build();
        Future<HttpResponse> future = executor.submit(() -> {
            try {
                return client.execute(method, getHttpContext());
            } catch (Exception e) {
                GlobalConfig.ASTERIX_LOGGER.log(Level.ERROR, "Failure executing {}", method, e);
                throw e;
            }
        });
        try {
            return future.get();
        } catch (Exception e) {
            client.close();
            throw e;
        } finally {
            executor.shutdownNow();
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
                GlobalConfig.ASTERIX_LOGGER.log(Level.ERROR, errorBody);
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

    public InputStream executeQueryService(String str, URI uri, OutputFormat fmt) throws Exception {
        return executeQueryService(str, fmt, uri, new ArrayList<>(), false, UTF_8);
    }

    public InputStream executeQueryService(String str, URI uri, OutputFormat fmt, Charset resultCharset)
            throws Exception {
        return executeQueryService(str, fmt, uri, new ArrayList<>(), false, resultCharset);
    }

    public InputStream executeQueryService(String str, OutputFormat fmt, URI uri, List<Parameter> params,
            boolean jsonEncoded, Charset responseCharset) throws Exception {
        return executeQueryService(str, fmt, uri, constructQueryParameters(str, fmt, params), jsonEncoded,
                responseCharset, null, false);
    }

    public InputStream executeQueryService(String str, OutputFormat fmt, URI uri, List<Parameter> params,
            boolean jsonEncoded, Predicate<Integer> responseCodeValidator) throws Exception {
        return executeQueryService(str, fmt, uri, constructQueryParameters(str, fmt, params), jsonEncoded, UTF_8,
                responseCodeValidator, false);
    }

    public List<Parameter> constructQueryParameters(String str, OutputFormat fmt, List<Parameter> params) {
        List<Parameter> newParams = upsertParam(params, "format", ParameterTypeEnum.STRING, fmt.mimeType());

        newParams = upsertParam(newParams, QueryServiceServlet.Parameter.PLAN_FORMAT.str(), ParameterTypeEnum.STRING,
                DEFAULT_PLAN_FORMAT);
        final Optional<String> maxReadsOptional = extractMaxResultReads(str);
        if (maxReadsOptional.isPresent()) {
            newParams = upsertParam(newParams, QueryServiceServlet.Parameter.MAX_RESULT_READS.str(),
                    ParameterTypeEnum.STRING, maxReadsOptional.get());
        }
        return newParams;
    }

    public InputStream executeQueryService(String str, OutputFormat fmt, URI uri, List<Parameter> params,
            boolean jsonEncoded, Charset responseCharset, Predicate<Integer> responseCodeValidator, boolean cancellable)
            throws Exception {

        final List<Parameter> additionalParams = extractParameters(str);
        for (Parameter param : additionalParams) {
            params = upsertParam(params, param.getName(), param.getType(), param.getValue());
        }

        HttpUriRequest method = jsonEncoded ? constructPostMethodJson(str, uri, "statement", params)
                : constructPostMethodUrl(str, uri, "statement", params);
        // Set accepted output response type
        method.setHeader("Origin", uri.getScheme() + uri.getAuthority());
        method.setHeader("Accept", OutputFormat.CLEAN_JSON.mimeType());
        method.setHeader("Accept-Charset", responseCharset.name());
        if (!responseCharset.equals(UTF_8)) {
            LOGGER.info("using Accept-Charset: {}", responseCharset.name());
        }

        HttpResponse response = executeHttpRequest(method);
        if (responseCodeValidator != null) {
            checkResponse(response, responseCodeValidator);
        }
        return response.getEntity().getContent();
    }

    public InputStream executeQueryService(String str, TestFileContext ctx, OutputFormat fmt, URI uri,
            List<Parameter> params, boolean jsonEncoded, Charset responseCharset,
            Predicate<Integer> responseCodeValidator, boolean cancellable) throws Exception {
        return executeQueryService(str, fmt, uri, constructQueryParameters(str, fmt, params), jsonEncoded,
                responseCharset, responseCodeValidator, cancellable);
    }

    public synchronized void setAvailableCharsets(Charset... charsets) {
        allCharsets.clear();
        allCharsets.addAll(Arrays.asList(charsets));
        charsetsRemaining.clear();
    }

    private synchronized Charset nextCharset() {
        while (true) {
            Charset nextCharset = charsetsRemaining.poll();
            if (nextCharset != null) {
                return nextCharset;
            }
            Collections.shuffle(allCharsets);
            charsetsRemaining.addAll(allCharsets);
        }
    }

    // duplicated from hyracks-test-support as transitive dependencies on test-jars are not handled correctly
    private static boolean canEncodeDecode(Charset charset, String input) {
        try {
            if (input.equals(new String(input.getBytes(charset), charset))) {
                // workaround for https://bugs.openjdk.java.net/browse/JDK-6392670 and similar
                if (input.equals(charset.decode(charset.encode(CharBuffer.wrap(input))).toString())) {
                    return true;
                }
            }
        } catch (Exception e) {
            LOGGER.debug("cannot encode / decode {} with {} due to exception", input, charset.displayName(), e);
        }
        return false;
    }

    protected List<Parameter> upsertParam(List<Parameter> params, String name, ParameterTypeEnum type, String value) {
        boolean replaced = false;
        List<Parameter> result = new ArrayList<>();
        for (Parameter param : params) {
            Parameter newParam = new Parameter();
            newParam.setName(param.getName());
            if (name.equals(param.getName())) {
                newParam.setType(type);
                newParam.setValue(value);
                replaced = true;
            } else {
                newParam.setType(param.getType());
                newParam.setValue(param.getValue());
            }
            result.add(newParam);
        }
        if (!replaced) {
            Parameter newParam = new Parameter();
            newParam.setName(name);
            newParam.setType(type);
            newParam.setValue(value);
            result.add(newParam);
        }
        return result;
    }

    private HttpUriRequest constructHttpMethod(String statement, URI uri, String stmtParam, boolean postStmtAsParam,
            List<Parameter> otherParams) {
        String stmtParamName = (postStmtAsParam ? stmtParam : null);
        return constructPostMethodUrl(statement, uri, stmtParamName, otherParams);
    }

    private HttpUriRequest constructGetMethod(URI endpoint, List<Parameter> params) {
        RequestBuilder builder = RequestBuilder.get(endpoint);
        for (Parameter param : params) {
            builder.addParameter(param.getName(), param.getValue());
        }
        return builder.build();
    }

    private HttpUriRequest buildRequest(String method, URI uri, List<Parameter> params, Optional<String> body,
            ContentType contentType) {
        RequestBuilder builder = RequestBuilder.create(method);
        builder.setUri(uri);
        for (Parameter param : params) {
            builder.addParameter(param.getName(), param.getValue());
        }
        builder.setCharset(UTF_8);
        body.ifPresent(s -> builder.setEntity(new StringEntity(s, contentType)));
        return builder.build();
    }

    private HttpUriRequest buildRequest(String method, URI uri, OutputFormat fmt, List<Parameter> params,
            Optional<String> body, ContentType contentType) {
        HttpUriRequest request = buildRequest(method, uri, params, body, contentType);
        // Set accepted output response type
        request.setHeader("Accept", fmt.mimeType());
        return request;
    }

    private HttpUriRequest constructGetMethod(URI endpoint, OutputFormat fmt, List<Parameter> params) {
        HttpUriRequest method = constructGetMethod(endpoint, params);
        // Set accepted output response type
        method.setHeader("Accept", fmt.mimeType());
        return method;
    }

    private HttpUriRequest constructPostMethod(URI uri, List<Parameter> params) {
        RequestBuilder builder = RequestBuilder.post(uri);
        for (Parameter param : params) {
            builder.addParameter(param.getName(), param.getValue());
        }
        builder.setCharset(UTF_8);
        return builder.build();
    }

    protected void setCharset(RequestBuilder builder, int stmtLength) {
        builder.setCharset(stmtLength > MAX_NON_UTF_8_STATEMENT_SIZE ? UTF_8 : nextCharset());
    }

    protected HttpUriRequest constructPostMethodUrl(String statement, URI uri, String stmtParam,
            List<Parameter> otherParams) {
        Objects.requireNonNull(stmtParam, "statement parameter required");
        RequestBuilder builder = RequestBuilder.post(uri);
        for (Parameter param : upsertParam(otherParams, stmtParam, ParameterTypeEnum.STRING, statement)) {
            builder.addParameter(param.getName(), param.getValue());
        }
        setCharset(builder, statement.length());
        return builder.build();
    }

    protected HttpUriRequest constructPostMethodJson(String statement, URI uri, String stmtParam,
            List<Parameter> otherParams) {
        Objects.requireNonNull(stmtParam, "statement parameter required");
        RequestBuilder builder = RequestBuilder.post(uri);
        ObjectMapper om = new ObjectMapper();
        ObjectNode content = om.createObjectNode();
        for (Parameter param : upsertParam(otherParams, stmtParam, ParameterTypeEnum.STRING, statement)) {
            String paramName = param.getName();
            ParameterTypeEnum paramType = param.getType();
            if (paramType == null) {
                paramType = ParameterTypeEnum.STRING;
            }
            String paramValue = param.getValue();
            switch (paramType) {
                case STRING:
                    content.put(paramName, paramValue);
                    break;
                case JSON:
                    content.putRawValue(paramName, new RawValue(paramValue));
                    break;
                default:
                    throw new IllegalStateException(paramType.toString());
            }
        }
        try {
            builder.setEntity(new StringEntity(om.writeValueAsString(content),
                    ContentType.create(ContentType.APPLICATION_JSON.getMimeType(),
                            statement.length() > MAX_NON_UTF_8_STATEMENT_SIZE ? UTF_8 : nextCharset())));
        } catch (JsonProcessingException e) {
            e.printStackTrace();
        }
        return builder.build();
    }

    public InputStream executeJSONGet(OutputFormat fmt, URI uri) throws Exception {
        return executeJSON(fmt, "GET", uri, Collections.emptyList());
    }

    public InputStream executeJSONGet(OutputFormat fmt, URI uri, List<Parameter> params,
            Predicate<Integer> responseCodeValidator) throws Exception {
        return executeJSON(fmt, "GET", uri, params, responseCodeValidator, Optional.empty(), TEXT_PLAIN_UTF8);
    }

    public InputStream executeJSON(OutputFormat fmt, String method, URI uri, List<Parameter> params) throws Exception {
        return executeJSON(fmt, method, uri, params, code -> code == HttpStatus.SC_OK, Optional.empty(),
                TEXT_PLAIN_UTF8);
    }

    public InputStream executeJSON(OutputFormat fmt, String method, URI uri, Predicate<Integer> responseCodeValidator)
            throws Exception {
        return executeJSON(fmt, method, uri, Collections.emptyList(), responseCodeValidator, Optional.empty(),
                TEXT_PLAIN_UTF8);
    }

    private InputStream executeJSON(OutputFormat fmt, String method, URI uri, List<Parameter> params,
            Predicate<Integer> responseCodeValidator, Optional<String> body, ContentType contentType) throws Exception {
        HttpUriRequest request = buildRequest(method, uri, fmt, params, body, contentType);
        HttpResponse response = executeAndCheckHttpRequest(request, responseCodeValidator);
        return response.getEntity().getContent();
    }

    // Method that reads a DDL/Update/Query File
    // and returns the contents as a string
    // This string is later passed to REST API for execution.
    public String readTestFile(File testFile) throws Exception {
        BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(testFile), UTF_8));
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

    public static String executeScript(ProcessBuilder pb, String scriptPath) throws Exception {
        LOGGER.info("Executing script: " + scriptPath);
        pb.command(scriptPath);
        Process p = pb.start();
        return getProcessOutput(p);
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
        IOUtils.copy(bisIn, writerIn, UTF_8);
        StringWriter writerErr = new StringWriter();
        IOUtils.copy(p.getErrorStream(), writerErr, UTF_8);

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
            MutableInt queryCount, List<TestFileContext> expectedResultFileCtxs, File testFile, String actualPath,
            MutableInt actualWarnCount) throws Exception {
        InputStream resultStream;
        File qbcFile;
        boolean failed = false;
        File expectedResultFile;
        switch (ctx.getType()) {
            case "ddl":
                if (ctx.getFile().getName().endsWith("aql")) {
                    executeAqlUpdateOrDdl(statement, OutputFormat.CLEAN_JSON);
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
                    executeAqlUpdateOrDdl(statement, OutputFormat.forCompilationUnit(cUnit));
                } else {
                    executeSqlppUpdateOrDdl(statement, OutputFormat.forCompilationUnit(cUnit));
                }
                break;
            case "pollget":
            case "pollquery":
                poll(testCaseCtx, ctx, variableCtx, statement, isDmlRecoveryTest, pb, cUnit, queryCount,
                        expectedResultFileCtxs, testFile, actualPath, ctx.getType().substring("poll".length()),
                        actualWarnCount, plainExecutor);
                break;
            case "polldynamic":
                polldynamic(testCaseCtx, ctx, variableCtx, statement, isDmlRecoveryTest, pb, cUnit, queryCount,
                        expectedResultFileCtxs, testFile, actualPath, actualWarnCount);
                break;
            case "query":
            case "async":
            case "parse":
            case "deferred":
            case "metrics":
            case "profile":
            case "plans":
                // isDmlRecoveryTest: insert Crash and Recovery
                if (isDmlRecoveryTest) {
                    executeScript(pb, pb.environment().get("SCRIPT_HOME") + File.separator + "dml_recovery"
                            + File.separator + "kill_cc_and_nc.sh");
                    executeScript(pb, pb.environment().get("SCRIPT_HOME") + File.separator + "dml_recovery"
                            + File.separator + "stop_and_start.sh");
                }
                expectedResultFile =
                        (queryCount.intValue() < 0 || queryCount.intValue() >= expectedResultFileCtxs.size()) ? null
                                : expectedResultFileCtxs.get(queryCount.intValue()).getFile();
                File actualResultFile = expectedResultFile == null ? null
                        : testCaseCtx.getActualResultFile(cUnit, expectedResultFile, new File(actualPath));
                ExtractedResult extractedResult = executeQuery(OutputFormat.forCompilationUnit(cUnit), statement,
                        variableCtx, ctx, expectedResultFile, actualResultFile, queryCount,
                        expectedResultFileCtxs.size(), cUnit.getParameter(), ComparisonEnum.TEXT);

                if (testCaseCtx.getTestCase().isCheckWarnings()) {
                    boolean expectedSourceLoc = testCaseCtx.isSourceLocationExpected(cUnit);
                    validateWarnings(extractedResult.getWarnings(), cUnit.getExpectedWarn(), actualWarnCount,
                            expectedSourceLoc);
                }
                break;
            case "store":
                // This is a query that returns the expected output of a subsequent query
                String key = getKey(statement);
                if (variableCtx.containsKey(key)) {
                    throw new IllegalStateException("There exist already a stored result with the key: " + key);
                }
                actualResultFile = new File(actualPath, testCaseCtx.getTestCase().getFilePath() + File.separatorChar
                        + cUnit.getName() + '.' + ctx.getSeqNum() + ".adm");
                executeQuery(OutputFormat.forCompilationUnit(cUnit), statement, variableCtx, ctx, null,
                        actualResultFile, queryCount, expectedResultFileCtxs.size(), cUnit.getParameter(),
                        ComparisonEnum.TEXT);
                variableCtx.put(key, actualResultFile);
                break;
            case "validate":
                // This is a query that validates the output against a previously executed query
                validate(actualPath, testCaseCtx, cUnit, statement, variableCtx, testFile, ctx, queryCount,
                        expectedResultFileCtxs);
                break;
            case "txnqbc": // qbc represents query before crash
                resultStream = query(cUnit, testFile.getName(), statement, UTF_8);
                qbcFile = getTestCaseQueryBeforeCrashFile(actualPath, testCaseCtx, cUnit);
                writeOutputToFile(qbcFile, resultStream);
                break;
            case "txnqar": // qar represents query after recovery
                resultStream = query(cUnit, testFile.getName(), statement, UTF_8);
                File qarFile = new File(actualPath + File.separator
                        + testCaseCtx.getTestCase().getFilePath().replace(File.separator, "_") + "_" + cUnit.getName()
                        + "_qar.adm");
                writeOutputToFile(qarFile, resultStream);
                qbcFile = getTestCaseQueryBeforeCrashFile(actualPath, testCaseCtx, cUnit);
                runScriptAndCompareWithResult(testFile, qbcFile, qarFile, ComparisonEnum.TEXT, UTF_8);
                break;
            case "txneu": // eu represents erroneous update
                try {
                    executeAqlUpdateOrDdl(statement, OutputFormat.forCompilationUnit(cUnit));
                } catch (Exception e) {
                    // An exception is expected.
                    failed = true;
                    LOGGER.info("testFile {} raised an (expected) exception", testFile, e.toString());
                }
                if (!failed) {
                    throw new Exception("Test \"" + testFile + "\" FAILED; an exception was expected");
                }
                break;
            case "script":
                try {
                    String output = executeScript(pb, getScriptPath(testFile.getAbsolutePath(),
                            pb.environment().get("SCRIPT_HOME"), stripLineComments(statement).trim()));
                    if (output.contains("ERROR")) {
                        throw new Exception(output);
                    }
                } catch (Exception e) {
                    throw new Exception("Test \"" + testFile + "\" FAILED!", e);
                }
                break;
            case "sleep":
                String[] lines = stripLineComments(statement).trim().split("\n");
                Thread.sleep(Long.parseLong(lines[lines.length - 1].trim()));
                break;
            case "errddl": // a ddlquery that expects error
                try {
                    executeAqlUpdateOrDdl(statement, OutputFormat.forCompilationUnit(cUnit));
                } catch (Exception e) {
                    // expected error happens
                    failed = true;
                    LOGGER.info("testFile {} raised an (expected) exception", testFile, e.toString());
                }
                if (!failed) {
                    throw new Exception("Test \"" + testFile + "\" FAILED; an exception was expected");
                }
                break;
            case "get":
            case "post":
            case "put":
            case "delete":
                expectedResultFile =
                        (queryCount.intValue() < 0 || queryCount.intValue() >= expectedResultFileCtxs.size()) ? null
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
                if (command.length < 2) {
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
                switch (commandType) {
                    case "kill":
                        killNC(nodeId, cUnit);
                        break;
                    case "start":
                        startNC(nodeId);
                        break;
                }
                break;
            case "loop":
                TestLoop testLoop = testLoops.get(testFile);
                if (testLoop == null) {
                    lines = stripAllComments(statement).trim().split("\n");
                    String target = null;
                    int count = -1;
                    long durationSecs = -1;
                    for (String line : lines) {
                        command = line.trim().split(" ");
                        switch (command[0]) {
                            case "target":
                                if (target != null) {
                                    throw new IllegalStateException("duplicate target");
                                }
                                target = command[1];
                                break;
                            case "count":
                                if (count != -1) {
                                    throw new IllegalStateException("duplicate count");
                                }
                                count = Integer.parseInt(command[1]);
                                break;
                            case "duration":
                                if (durationSecs != -1) {
                                    throw new IllegalStateException("duplicate duration");
                                }
                                long duration = Duration.parseDurationStringToNanos(command[1]);
                                durationSecs = TimeUnit.NANOSECONDS.toSeconds(duration);
                                if (durationSecs < 1) {
                                    throw new IllegalArgumentException("duration cannot be shorter than 1s");
                                } else if (TimeUnit.SECONDS.toDays(durationSecs) > 1) {
                                    throw new IllegalArgumentException("duration cannot be exceed 1d");
                                }
                                break;
                            default:
                                throw new IllegalArgumentException("unknown directive: " + command[0]);
                        }
                    }
                    if (target == null || (count == -1 && durationSecs == -1) || (count != -1 && durationSecs != -1)) {
                        throw new IllegalStateException("Must specify 'target' and exactly one of 'count', 'duration'");
                    }
                    if (count != -1) {
                        testLoop = TestLoop.createLoop(target, count);
                    } else {
                        testLoop = TestLoop.createLoop(target, durationSecs, TimeUnit.SECONDS);
                    }
                    testLoops.put(testFile, testLoop);
                }
                testLoop.executeLoop();
                // we only reach here if the loop is over
                testLoops.remove(testFile);
                break;
            case "sto":
                command = stripJavaComments(statement).trim().split(" ");
                executeStorageCommand(command);
                break;
            case "port":
                command = stripJavaComments(statement).trim().split(" ");
                handlePortCommand(command);
                break;
            default:
                throw new IllegalArgumentException("No statements of type " + ctx.getType());
        }
    }

    private void validate(String actualPath, TestCaseContext testCaseCtx, CompilationUnit cUnit, String statement,
            Map<String, Object> variableCtx, File testFile, TestFileContext ctx, MutableInt queryCount,
            List<TestFileContext> expectedResultFileCtxs) throws Exception {
        String key = getKey(statement);
        File expectedResultFile = (File) variableCtx.remove(key);
        if (expectedResultFile == null) {
            throw new IllegalStateException("There is no stored result with the key: " + key);
        }
        File actualResultFile = new File(actualPath, testCaseCtx.getTestCase().getFilePath() + File.separatorChar
                + cUnit.getName() + '.' + ctx.getSeqNum() + ".adm");
        executeQuery(OutputFormat.forCompilationUnit(cUnit), statement, variableCtx,
                new TestFileContext(testFile, "validate"), expectedResultFile, actualResultFile, queryCount,
                expectedResultFileCtxs.size(), cUnit.getParameter(), ComparisonEnum.TEXT);
    }

    protected void executeHttpRequest(OutputFormat fmt, String statement, Map<String, Object> variableCtx,
            String reqType, File testFile, File expectedResultFile, File actualResultFile, MutableInt queryCount,
            int numResultFiles, String extension, ComparisonEnum compare) throws Exception {
        String handleVar = getHandleVariable(statement);
        final String trimmedPathAndQuery = stripAllComments(statement).trim();
        final String variablesReplaced = replaceVarRef(trimmedPathAndQuery, variableCtx);
        final List<Parameter> params = extractParameters(statement);
        final Optional<String> body = extractBody(statement);
        final Predicate<Integer> statusCodePredicate = extractStatusCodePredicate(statement);
        final boolean extracResult = isExtracResult(statement);
        final String mimeReqType = extractHttpRequestType(statement);
        ContentType contentType = mimeReqType != null ? ContentType.create(mimeReqType, UTF_8) : TEXT_PLAIN_UTF8;
        InputStream resultStream;
        if ("http".equals(extension)) {
            resultStream = executeHttp(reqType, variablesReplaced, fmt, params, statusCodePredicate, body, contentType);
        } else if ("uri".equals(extension)) {
            resultStream = executeURI(reqType, URI.create(variablesReplaced), fmt, params, statusCodePredicate, body,
                    contentType);
            if (extracResult) {
                resultStream = ResultExtractor.extract(resultStream, UTF_8).getResult();
            }
        } else {
            throw new IllegalArgumentException("Unexpected format for method " + reqType + ": " + extension);
        }
        if (handleVar != null) {
            String handle = ResultExtractor.extractHandle(resultStream, UTF_8);
            if (handle != null) {
                variableCtx.put(handleVar, handle);
            } else {
                throw new Exception("no handle for test " + testFile.toString());
            }
        } else {
            if (expectedResultFile == null) {
                if (testFile.getName().startsWith(DIAGNOSE)) {
                    LOGGER.info("Diagnostic output: {}", IOUtils.toString(resultStream, UTF_8));
                } else {
                    LOGGER.info("Unexpected output: {}", IOUtils.toString(resultStream, UTF_8));
                    Assert.fail("no result file for " + testFile.toString() + "; queryCount: " + queryCount
                            + ", filectxs.size: " + numResultFiles);
                }
            } else {
                writeOutputToFile(actualResultFile, resultStream);
                runScriptAndCompareWithResult(testFile, expectedResultFile, actualResultFile, compare, UTF_8);
            }
        }
        queryCount.increment();
    }

    protected Charset getResponseCharset(File expectedResultFile) {
        return expectedResultFile == null ? UTF_8 : nextCharset();
    }

    public ExtractedResult executeQuery(OutputFormat fmt, String statement, Map<String, Object> variableCtx,
            TestFileContext ctx, File expectedResultFile, File actualResultFile, MutableInt queryCount,
            int numResultFiles, List<Parameter> params, ComparisonEnum compare, URI uri) throws Exception {

        String delivery = DELIVERY_IMMEDIATE;
        String reqType = ctx.getType();
        File testFile = ctx.getFile();
        if (reqType.equalsIgnoreCase("async")) {
            delivery = DELIVERY_ASYNC;
        } else if (reqType.equalsIgnoreCase("deferred")) {
            delivery = DELIVERY_DEFERRED;
        }

        boolean isJsonEncoded = isJsonEncoded(extractHttpRequestType(statement));

        Charset responseCharset = getResponseCharset(expectedResultFile);
        InputStream resultStream;
        ExtractedResult extractedResult = null;
        if (DELIVERY_IMMEDIATE.equals(delivery)) {
            resultStream = executeQueryService(statement, ctx, fmt, uri, params, isJsonEncoded, responseCharset, null,
                    isCancellable(reqType));
            switch (reqType) {
                case METRICS_QUERY_TYPE:
                    resultStream = ResultExtractor.extractMetrics(resultStream, responseCharset);
                    break;
                case PROFILE_QUERY_TYPE:
                    resultStream = ResultExtractor.extractProfile(resultStream, responseCharset);
                    break;
                case PLANS_QUERY_TYPE:
                    resultStream = ResultExtractor.extractPlans(resultStream, responseCharset);
                    break;
                default:
                    extractedResult = ResultExtractor.extract(resultStream, responseCharset);
                    resultStream = extractedResult.getResult();
                    break;
            }
        } else {
            String handleVar = getHandleVariable(statement);
            resultStream = executeQueryService(statement, fmt, uri,
                    upsertParam(params, "mode", ParameterTypeEnum.STRING, delivery), isJsonEncoded, responseCharset);
            String handle = ResultExtractor.extractHandle(resultStream, responseCharset);
            Assert.assertNotNull("no handle for " + reqType + " test " + testFile.toString(), handleVar);
            variableCtx.put(handleVar, toQueryServiceHandle(handle));
        }
        if (actualResultFile == null) {
            if (testFile.getName().startsWith(DIAGNOSE)) {
                LOGGER.info("Diagnostic output: {}", IOUtils.toString(resultStream, responseCharset));
            } else {
                Assert.fail("no result file for " + testFile.toString() + "; queryCount: " + queryCount
                        + ", filectxs.size: " + numResultFiles);
            }
        } else {
            writeOutputToFile(actualResultFile, resultStream);
            if (expectedResultFile == null) {
                if (reqType.equals("store")) {
                    return extractedResult;
                }
                Assert.fail("no result file for " + testFile.toString() + "; queryCount: " + queryCount
                        + ", filectxs.size: " + numResultFiles);
            }
        }
        runScriptAndCompareWithResult(testFile, expectedResultFile, actualResultFile, compare, responseCharset);
        if (!reqType.equals("validate")) {
            queryCount.increment();
        }
        // Deletes the matched result file.
        actualResultFile.getParentFile().delete();
        return extractedResult;
    }

    public ExtractedResult executeQuery(OutputFormat fmt, String statement, Map<String, Object> variableCtx,
            TestFileContext ctx, File expectedResultFile, File actualResultFile, MutableInt queryCount,
            int numResultFiles, List<Parameter> params, ComparisonEnum compare) throws Exception {
        URI uri = getEndpoint(ctx.getFile().getName().endsWith("aql") ? Servlets.QUERY_AQL : Servlets.QUERY_SERVICE,
                FilenameUtils.getExtension(ctx.getFile().getName()));
        return executeQuery(fmt, statement, variableCtx, ctx, expectedResultFile, actualResultFile, queryCount,
                numResultFiles, params, compare, uri);

    }

    private void polldynamic(TestCaseContext testCaseCtx, TestFileContext ctx, Map<String, Object> variableCtx,
            String statement, boolean isDmlRecoveryTest, ProcessBuilder pb, CompilationUnit cUnit,
            MutableInt queryCount, List<TestFileContext> expectedResultFileCtxs, File testFile, String actualPath,
            MutableInt actualWarnCount) throws Exception {
        IExpectedResultPoller poller = getExpectedResultPoller(statement);
        final String key = getKey(statement);
        poll(testCaseCtx, ctx, variableCtx, statement, isDmlRecoveryTest, pb, cUnit, queryCount, expectedResultFileCtxs,
                testFile, actualPath, "validate", actualWarnCount, new IPollTask() {
                    @Override
                    public void execute(TestCaseContext testCaseCtx, TestFileContext ctx,
                            Map<String, Object> variableCtx, String statement, boolean isDmlRecoveryTest,
                            ProcessBuilder pb, CompilationUnit cUnit, MutableInt queryCount,
                            List<TestFileContext> expectedResultFileCtxs, File testFile, String actualPath,
                            MutableInt actualWarnCount) throws Exception {
                        File actualResultFile = new File(actualPath, testCaseCtx.getTestCase().getFilePath()
                                + File.separatorChar + cUnit.getName() + '.' + ctx.getSeqNum() + ".polled.adm");
                        if (actualResultFile.exists() && !actualResultFile.delete()) {
                            throw new Exception(
                                    "Failed to delete an existing result file: " + actualResultFile.getAbsolutePath());
                        }
                        writeOutputToFile(actualResultFile, new ByteArrayInputStream(poller.poll().getBytes(UTF_8)));
                        variableCtx.put(key, actualResultFile);
                        validate(actualPath, testCaseCtx, cUnit, statement, variableCtx, testFile, ctx, queryCount,
                                expectedResultFileCtxs);
                    }
                });
    }

    protected IExpectedResultPoller getExpectedResultPoller(String statement) {
        String key = "poller=";
        String value = null;
        String[] lines = statement.split("\n");
        for (String line : lines) {
            if (line.contains(key)) {
                value = line.substring(line.indexOf(key) + key.length()).trim();
            }
        }
        if (value == null) {
            throw new IllegalArgumentException("ERROR: poller=<...> must be present in poll-dynamic file");
        }
        String staticPoller = "static:";
        if (value.startsWith(staticPoller)) {
            String polled = value.substring(staticPoller.length());
            return () -> polled;
        }
        throw new IllegalArgumentException("ERROR: unknown poller: " + value);
    }

    private void poll(TestCaseContext testCaseCtx, TestFileContext ctx, Map<String, Object> variableCtx,
            String statement, boolean isDmlRecoveryTest, ProcessBuilder pb, CompilationUnit cUnit,
            MutableInt queryCount, List<TestFileContext> expectedResultFileCtxs, File testFile, String actualPath,
            String newType, MutableInt actualWarnCount, IPollTask pollTask) throws Exception {
        // polltimeoutsecs=nnn, polldelaysecs=nnn
        int timeoutSecs = getTimeoutSecs(statement);
        int retryDelaySecs = getRetryDelaySecs(statement);
        long startTime = System.currentTimeMillis();
        long limitTime = startTime + TimeUnit.SECONDS.toMillis(timeoutSecs);
        Semaphore endSemaphore = new Semaphore(1);
        final ExecutorService executorService = Executors.newSingleThreadExecutor();
        String originalType = ctx.getType();
        ctx.setType(newType);
        try {
            boolean expectedException = false;
            Exception finalException = null;
            LOGGER.debug("polling for up to " + timeoutSecs + " seconds w/ " + retryDelaySecs + " second(s) delay");
            int responsesReceived = 0;
            while (true) {
                try {
                    endSemaphore.acquire();
                    Semaphore startSemaphore = new Semaphore(0);
                    Future<Void> execution = executorService.submit(() -> {
                        try {
                            startSemaphore.release();
                            pollTask.execute(testCaseCtx, ctx, variableCtx, statement, isDmlRecoveryTest, pb, cUnit,
                                    queryCount, expectedResultFileCtxs, testFile, actualPath, actualWarnCount);
                        } finally {
                            endSemaphore.release();
                        }
                        return null;
                    });
                    startSemaphore.acquire();
                    execution.get(limitTime - System.currentTimeMillis(), TimeUnit.MILLISECONDS);
                    responsesReceived++;
                    finalException = null;
                    break;
                } catch (TimeoutException e) {
                    if (responsesReceived == 0) {
                        throw new Exception("Poll limit (" + timeoutSecs
                                + "s) exceeded without obtaining *any* result from server");
                    } else if (finalException != null) {
                        throw new Exception(
                                "Poll limit (" + timeoutSecs
                                        + "s) exceeded without obtaining expected result; last exception:",
                                finalException);
                    } else {
                        throw new Exception(
                                "Poll limit (" + timeoutSecs + "s) exceeded without obtaining expected result");

                    }
                } catch (ExecutionException ee) {
                    Exception e;
                    if (ee.getCause() instanceof Exception) {
                        e = (Exception) ee.getCause();
                    } else {
                        e = ee;
                    }
                    if (e instanceof ComparisonException) {
                        LOGGER.info("Comparison failure on poll: {}", e::getMessage);
                    } else if (LOGGER.isDebugEnabled()) {
                        LOGGER.debug("Received exception on poll", e);
                    } else {
                        LOGGER.info("Received exception on poll: {}", e::toString);
                    }
                    responsesReceived++;
                    if (isExpected(e, cUnit)) {
                        expectedException = true;
                        finalException = e;
                        break;
                    }
                    if ((System.currentTimeMillis() > limitTime)) {
                        finalException = e;
                        break;
                    }
                    LOGGER.debug("sleeping " + retryDelaySecs + " second(s) before polling again");
                    TimeUnit.SECONDS.sleep(retryDelaySecs);
                }
            }
            if (expectedException) {
                throw finalException;
            } else if (finalException != null) {
                throw new Exception("Poll limit (" + timeoutSecs + "s) exceeded without obtaining expected result",
                        finalException);
            }
        } finally {
            executorService.shutdownNow();
            // ensure no leftover task is running. This avoids re-polling due to
            // resetting the ctx type to poll while the last attempt is being made
            endSemaphore.acquire();
            ctx.setType(originalType);
        }
    }

    public InputStream executeSqlppUpdateOrDdl(String statement, OutputFormat outputFormat) throws Exception {
        return executeUpdateOrDdl(statement, outputFormat, getQueryServiceUri(SQLPP));
    }

    private InputStream executeAqlUpdateOrDdl(String statement, OutputFormat outputFormat) throws Exception {
        return executeUpdateOrDdl(statement, outputFormat, getQueryServiceUri(AQL));
    }

    private InputStream executeUpdateOrDdl(String statement, OutputFormat outputFormat, URI serviceUri)
            throws Exception {
        InputStream resultStream = executeQueryService(statement, serviceUri, outputFormat, UTF_8);
        return ResultExtractor.extract(resultStream, UTF_8).getResult();
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

    public int getTimeoutSecs(String statement) {
        final Matcher timeoutMatcher = POLL_TIMEOUT_PATTERN.matcher(statement);
        if (timeoutMatcher.find()) {
            return (int) (Integer.parseInt(timeoutMatcher.group(1)) * timeoutMultiplier);
        } else {
            throw new IllegalArgumentException("ERROR: polltimeoutsecs=nnn must be present in poll file");
        }
    }

    public static String getKey(String statement) {
        String key = "key=";
        String[] lines = statement.split("\n");
        for (String line : lines) {
            if (line.contains(key)) {
                String value = line.substring(line.indexOf(key) + key.length()).trim();
                if (value.length() == 0) {
                    break;
                }
                return value;
            }
        }
        throw new IllegalArgumentException("ERROR: key=<KEY> must be present in store/validate file");
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

    protected static Optional<String> extractMaxResultReads(String statement) {
        final Matcher m = MAX_RESULT_READS_PATTERN.matcher(statement);
        while (m.find()) {
            return Optional.of(m.group(1));
        }
        return Optional.empty();
    }

    protected static Optional<String> extractBody(String statement) {
        final Matcher m = HTTP_BODY_PATTERN.matcher(statement);
        while (m.find()) {
            return Optional.of(m.group(1));
        }
        return Optional.empty();
    }

    public static List<Parameter> extractParameters(String statement) {
        List<Parameter> params = new ArrayList<>();
        final Matcher m = HTTP_PARAM_PATTERN.matcher(statement);
        while (m.find()) {
            final Parameter param = new Parameter();
            String name = m.group("name");
            param.setName(name);
            String value = m.group("value");
            param.setValue(value);
            String type = m.group("type");
            if (type != null) {
                try {
                    param.setType(ParameterTypeEnum.fromValue(type.toLowerCase()));
                } catch (IllegalArgumentException e) {
                    throw new IllegalArgumentException(
                            String.format("Invalid type '%s' specified for parameter '%s'", type, name));
                }
            }
            params.add(param);
        }
        return params;
    }

    private static String extractHttpRequestType(String statement) {
        Matcher m = HTTP_REQUEST_TYPE.matcher(statement);
        return m.find() ? m.group(1) : null;
    }

    private static boolean isExtracResult(String statement) {
        Matcher m = EXTRACT_RESULT_TYPE.matcher(statement);
        return m.find() ? Boolean.valueOf(m.group(1)) : false;
    }

    private static boolean isJsonEncoded(String httpRequestType) throws Exception {
        if (httpRequestType == null || httpRequestType.isEmpty()) {
            return true;
        }
        switch (httpRequestType.trim()) {
            case HttpUtil.ContentType.JSON:
            case HttpUtil.ContentType.APPLICATION_JSON:
                return true;
            case HttpUtil.ContentType.APPLICATION_X_WWW_FORM_URLENCODED:
                return false;
            default:
                throw new Exception("Invalid value for http request type: " + httpRequestType);
        }
    }

    protected static Predicate<Integer> extractStatusCodePredicate(String statement) {
        List<Integer> codes = new ArrayList<>();
        final Matcher m = HTTP_STATUSCODE_PATTERN.matcher(statement);
        while (m.find()) {
            codes.add(Integer.parseInt(m.group(1)));
        }
        if (codes.isEmpty()) {
            return code -> code == HttpStatus.SC_OK;
        } else {
            return codes::contains;
        }
    }

    protected InputStream executeHttp(String ctxType, String endpoint, OutputFormat fmt, List<Parameter> params,
            Predicate<Integer> statusCodePredicate, Optional<String> body, ContentType contentType) throws Exception {
        String[] split = endpoint.split("\\?");
        URI uri = createEndpointURI(split[0], split.length > 1 ? split[1] : null);
        return executeURI(ctxType, uri, fmt, params, statusCodePredicate, body, contentType);
    }

    private InputStream executeURI(String ctxType, URI uri, OutputFormat fmt, List<Parameter> params) throws Exception {
        return executeJSON(fmt, ctxType.toUpperCase(), uri, params);
    }

    private InputStream executeURI(String ctxType, URI uri, OutputFormat fmt, List<Parameter> params,
            Predicate<Integer> responseCodeValidator, Optional<String> body, ContentType contentType) throws Exception {
        return executeJSON(fmt, ctxType.toUpperCase(), uri, params, responseCodeValidator, body, contentType);
    }

    public void killNC(String nodeId, CompilationUnit cUnit) throws Exception {
        //get node process id
        OutputFormat fmt = OutputFormat.CLEAN_JSON;
        String endpoint = "/admin/cluster/node/" + nodeId + "/config";
        InputStream executeJSONGet = executeJSONGet(fmt, createEndpointURI(endpoint, null));
        StringWriter actual = new StringWriter();
        IOUtils.copy(executeJSONGet, actual, UTF_8);
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

    public void startNC(String nodeId) throws Exception {
        //get node process id
        OutputFormat fmt = OutputFormat.CLEAN_JSON;
        String endpoint = "/rest/startnode";
        List<Parameter> params = new ArrayList<>();
        Parameter node = new Parameter();
        node.setName("node");
        node.setType(ParameterTypeEnum.STRING);
        node.setValue(nodeId);
        params.add(node);
        InputStream executeJSON = executeJSON(fmt, "POST", URI.create("http://localhost:16001" + endpoint), params);
    }

    private void deleteNCTxnLogs(String nodeId, CompilationUnit cUnit) throws Exception {
        OutputFormat fmt = OutputFormat.forCompilationUnit(cUnit);
        String endpoint = "/admin/cluster/node/" + nodeId + "/config";
        InputStream executeJSONGet = executeJSONGet(fmt, createEndpointURI(endpoint, null));
        StringWriter actual = new StringWriter();
        IOUtils.copy(executeJSONGet, actual, UTF_8);
        String config = actual.toString();
        ObjectMapper om = new ObjectMapper();
        String logDir = om.readTree(config).findPath("txn.log.dir").asText();
        FileUtils.deleteQuietly(new File(logDir));
    }

    public void executeTest(String actualPath, TestCaseContext testCaseCtx, ProcessBuilder pb,
            boolean isDmlRecoveryTest, TestGroup failedGroup) throws Exception {
        executeTest(actualPath, testCaseCtx, pb, isDmlRecoveryTest, failedGroup, null);
    }

    public void executeTest(String actualPath, TestCaseContext testCaseCtx, ProcessBuilder pb,
            boolean isDmlRecoveryTest, TestGroup failedGroup, TestGroup passedGroup) throws Exception {
        MutableInt queryCount = new MutableInt(0);
        int numOfErrors = 0;
        int numOfFiles = 0;
        List<CompilationUnit> cUnits = testCaseCtx.getTestCase().getCompilationUnit();
        for (CompilationUnit cUnit : cUnits) {
            List<String> expectedErrors = cUnit.getExpectedError();
            int expectedWarnCount = cUnit.getExpectedWarn().size();
            MutableInt actualWarnCount = new MutableInt(0);
            LOGGER.info(
                    "Starting [TEST]: " + testCaseCtx.getTestCase().getFilePath() + "/" + cUnit.getName() + " ... ");
            Map<String, Object> variableCtx = new HashMap<>();
            List<TestFileContext> testFileCtxs = testCaseCtx.getTestFiles(cUnit);
            if (testFileCtxs.isEmpty()) {
                Assert.fail("No test files found for test: " + testCaseCtx.getTestCase().getFilePath() + "/"
                        + cUnit.getName());
            }
            List<TestFileContext> expectedResultFileCtxs = testCaseCtx.getExpectedResultFiles(cUnit);
            int[] savedQueryCounts = new int[numOfFiles + testFileCtxs.size()];
            for (ListIterator<TestFileContext> iter = testFileCtxs.listIterator(); iter.hasNext();) {
                TestFileContext ctx = iter.next();
                savedQueryCounts[numOfFiles] = queryCount.getValue();
                numOfFiles++;
                final File testFile = ctx.getFile();
                final String statement = readTestFile(testFile);
                try {
                    if (!testFile.getName().startsWith(DIAGNOSE)) {
                        executeTestFile(testCaseCtx, ctx, variableCtx, statement, isDmlRecoveryTest, pb, cUnit,
                                queryCount, expectedResultFileCtxs, testFile, actualPath, actualWarnCount);
                    }
                } catch (TestLoop loop) {
                    // rewind the iterator until we find our target
                    while (!ctx.getFile().getName().equals(loop.getTarget())) {
                        if (!iter.hasPrevious()) {
                            throw new IllegalStateException("unable to find loop target '" + loop.getTarget() + "'!");
                        }
                        ctx = iter.previous();
                        numOfFiles--;
                        queryCount.setValue(savedQueryCounts[numOfFiles]);
                    }
                } catch (Exception e) {
                    numOfErrors++;
                    boolean unexpected = isUnExpected(e, expectedErrors, numOfErrors, queryCount,
                            testCaseCtx.isSourceLocationExpected(cUnit));
                    if (unexpected) {
                        LOGGER.error("testFile {} raised an unexpected exception", testFile, e);
                        if (failedGroup != null) {
                            failedGroup.getTestCase().add(testCaseCtx.getTestCase());
                        }
                        fail(true, testCaseCtx, cUnit, testFileCtxs, pb, testFile, e);
                    } else {
                        LOGGER.info("testFile {} raised an (expected) exception", testFile, e.toString());
                    }
                }
                if (numOfFiles == testFileCtxs.size()) {
                    if (numOfErrors < cUnit.getExpectedError().size()) {
                        LOGGER.error("Test {} failed to raise (an) expected exception(s)", cUnit.getName());
                        throw new Exception(
                                "Test \"" + cUnit.getName() + "\" FAILED; expected exception was not thrown...");
                    }
                    ensureWarnings(actualWarnCount.getValue(), expectedWarnCount, cUnit);
                    LOGGER.info(
                            "[TEST]: " + testCaseCtx.getTestCase().getFilePath() + "/" + cUnit.getName() + " PASSED ");
                    if (passedGroup != null) {
                        passedGroup.getTestCase().add(testCaseCtx.getTestCase());
                    }
                }
            }
        }
    }

    protected void fail(boolean runDiagnostics, TestCaseContext testCaseCtx, CompilationUnit cUnit,
            List<TestFileContext> testFileCtxs, ProcessBuilder pb, File testFile, Exception e) throws Exception {
        if (runDiagnostics) {
            try {
                // execute diagnostic files
                Map<String, Object> variableCtx = new HashMap<>();
                for (TestFileContext ctx : testFileCtxs) {
                    if (ctx.getFile().getName().startsWith(TestExecutor.DIAGNOSE)) {
                        // execute the file
                        final File file = ctx.getFile();
                        final String statement = readTestFile(file);
                        executeTestFile(testCaseCtx, ctx, variableCtx, statement, false, pb, cUnit, new MutableInt(-1),
                                Collections.emptyList(), file, null, new MutableInt(-1));
                    }
                }
            } catch (Exception diagnosticFailure) {
                LOGGER.log(Level.WARN, "Failure during running diagnostics", diagnosticFailure);
            }
        }
        throw new Exception("Test \"" + testFile + "\" FAILED!", e);
    }

    protected boolean isUnExpected(Exception e, List<String> expectedErrors, int numOfErrors, MutableInt queryCount,
            boolean expectedSourceLoc) {
        String expectedError = null;
        if (expectedErrors.size() < numOfErrors) {
            return true;
        } else {
            // Get the expected exception
            expectedError = expectedErrors.get(numOfErrors - 1);
            String actualError = e.toString();
            if (!actualError.contains(expectedError)) {
                LOGGER.error("Expected to find the following in error text: +++++{}+++++", expectedError);
                return true;
            }
            if (expectedSourceLoc && !containsSourceLocation(actualError)) {
                LOGGER.error("Expected to find source location \"{}, {}\" in error text: +++++{}+++++",
                        ERR_MSG_SRC_LOC_LINE_REGEX, ERR_MSG_SRC_LOC_COLUMN_REGEX, actualError);
                return true;
            }
            return false;
        }
    }

    private static final String ERR_MSG_SRC_LOC_LINE_REGEX = "in line \\d+";
    private static final Pattern ERR_MSG_SRC_LOC_LINE_PATTERN =
            Pattern.compile(ERR_MSG_SRC_LOC_LINE_REGEX, Pattern.CASE_INSENSITIVE);

    private static final String ERR_MSG_SRC_LOC_COLUMN_REGEX = "at column \\d+";
    private static final Pattern ERR_MSG_SRC_LOC_COLUMN_PATTERN =
            Pattern.compile(ERR_MSG_SRC_LOC_COLUMN_REGEX, Pattern.CASE_INSENSITIVE);

    private boolean containsSourceLocation(String errorMessage) {
        Matcher lineMatcher = ERR_MSG_SRC_LOC_LINE_PATTERN.matcher(errorMessage);
        return lineMatcher.find() && ERR_MSG_SRC_LOC_COLUMN_PATTERN.matcher(errorMessage).find(lineMatcher.end());
    }

    private static File getTestCaseQueryBeforeCrashFile(String actualPath, TestCaseContext testCaseCtx,
            CompilationUnit cUnit) {
        return new File(
                actualPath + File.separator + testCaseCtx.getTestCase().getFilePath().replace(File.separator, "_") + "_"
                        + cUnit.getName() + "_qbc.adm");
    }

    protected URI createEndpointURI(String path, String query) throws URISyntaxException {
        InetSocketAddress endpoint;
        if (!ncEndPointsList.isEmpty() && path.equals(Servlets.QUERY_SERVICE)) {
            int endpointIdx = Math.abs(endpointSelector++ % ncEndPointsList.size());
            endpoint = ncEndPointsList.get(endpointIdx);
        } else if (isCcEndPointPath(path)) {
            int endpointIdx = Math.abs(endpointSelector++ % endpoints.size());
            endpoint = endpoints.get(endpointIdx);
        } else {
            // allowed patterns: [nc:endpointName URL] or [nc:nodeId:port URL]
            final String[] tokens = path.split(" ");
            if (tokens.length != 2) {
                throw new IllegalArgumentException("Unrecognized http pattern");
            }
            final String endpointName = tokens[0].substring(NC_ENDPOINT_PREFIX.length());
            if (containsPort(endpointName)) {
                // currently only loopback address is supported in the test framework
                final String nodeIP = InetAddress.getLoopbackAddress().getHostAddress();
                final String endpointParts[] = StringUtils.split(endpointName, ':');
                int port = Integer.valueOf(endpointParts[1]);
                endpoint = new InetSocketAddress(nodeIP, port);
            } else {
                endpoint = getNcEndPoint(endpointName);
            }
            path = tokens[1];
        }
        URI uri = new URI("http", null, endpoint.getHostString(), endpoint.getPort(), path, query, null);
        LOGGER.debug("Created endpoint URI: " + uri);
        return uri;
    }

    public URI getEndpoint(String servlet) throws URISyntaxException {
        return createEndpointURI(Servlets.getAbsolutePath(getPath(servlet)), null);
    }

    public URI getEndpoint(String servlet, String extension) throws URISyntaxException {
        return createEndpointURI(Servlets.getAbsolutePath(getPath(servlet)), null);
    }

    public static String stripJavaComments(String text) {
        return JAVA_BLOCK_COMMENT_PATTERN.matcher(text).replaceAll("");
    }

    public static String stripLineComments(String text) {
        return JAVA_SHELL_SQL_LINE_COMMENT_PATTERN.matcher(text).replaceAll("");
    }

    public static String stripAllComments(String statement) {
        return stripLineComments(stripJavaComments(statement));
    }

    public void cleanup(String testCase, List<String> badtestcases) throws Exception {
        try {
            ArrayList<String> toBeDropped = new ArrayList<>();
            InputStream resultStream = executeQueryService(
                    "select dv.DataverseName from Metadata.`Dataverse` as dv order by dv.DataverseName;",
                    getEndpoint(Servlets.QUERY_SERVICE), OutputFormat.CLEAN_JSON);
            JsonNode result = extractResult(IOUtils.toString(resultStream, UTF_8));
            for (int i = 0; i < result.size(); i++) {
                JsonNode json = result.get(i);
                if (json != null) {
                    String dvName = json.get("DataverseName").asText();
                    if (!dvName.equals("Metadata") && !dvName.equals("Default")) {
                        toBeDropped.add(SqlppStatementUtil.enclose(dvName));
                    }
                }
            }
            if (!toBeDropped.isEmpty()) {
                badtestcases.add(testCase);
                LOGGER.info("Last test left some garbage. Dropping dataverses: " + StringUtils.join(toBeDropped, ','));
                StringBuilder dropStatement = new StringBuilder();
                for (String dv : toBeDropped) {
                    dropStatement.setLength(0);
                    dropStatement.append("drop dataverse ");
                    dropStatement.append(dv);
                    dropStatement.append(";\n");
                    resultStream = executeQueryService(dropStatement.toString(), getEndpoint(Servlets.QUERY_SERVICE),
                            OutputFormat.CLEAN_JSON, UTF_8);
                    ResultExtractor.extract(resultStream, UTF_8);
                }
            }
        } catch (Throwable th) {
            th.printStackTrace();
            throw th;
        }
    }

    private JsonNode extractResult(String jsonString) throws IOException {
        ObjectMapper om = new ObjectMapper();
        om.setConfig(om.getDeserializationConfig().with(DeserializationFeature.ACCEPT_EMPTY_STRING_AS_NULL_OBJECT));
        try {
            final JsonNode result = om.readValue(jsonString, ObjectNode.class).get("results");
            if (result == null) {
                throw new IllegalArgumentException("No field 'results' in " + jsonString);
            }
            return result;
        } catch (JsonMappingException e) {
            LOGGER.warn("error mapping response '{}' to json", jsonString, e);
            return om.createArrayNode();
        }
    }

    //This method is here to enable extension
    protected String getPath(String servlet) {
        return servlet;
    }

    public void waitForClusterActive(int timeoutSecs, TimeUnit timeUnit) throws Exception {
        waitForClusterState("ACTIVE", timeoutSecs, timeUnit);
    }

    public void waitForClusterState(String desiredState, int baseTimeout, TimeUnit timeUnit) throws Exception {
        int timeout = (int) (baseTimeout * timeoutMultiplier);
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

    protected void ensureWarnings(int actualWarnCount, int expectedWarnCount, CompilationUnit cUnit) throws Exception {
        if (actualWarnCount < expectedWarnCount) {
            LOGGER.error("Test {} failed to raise (an) expected warning(s)", cUnit.getName());
            throw new Exception("Test \"" + cUnit.getName() + "\" FAILED; expected warning(s) was not returned...");
        }
    }

    private void executeStorageCommand(String[] command) throws Exception {
        String srcNode = command[0];
        String api = command[1];
        final URI endpoint = getEndpoint(srcNode + " " + Servlets.getAbsolutePath(Servlets.STORAGE) + api);
        String partition = command[2];
        String destNode = command[3];
        final InetSocketAddress destAddress = getNcReplicationAddress(destNode);
        List<Parameter> parameters = new ArrayList<>(3);
        Stream.of("partition", "host", "port").forEach(arg -> {
            Parameter p = new Parameter();
            p.setName(arg);
            p.setType(ParameterTypeEnum.STRING);
            parameters.add(p);
        });
        parameters.get(0).setValue(partition);
        parameters.get(1).setValue(destAddress.getHostName());
        parameters.get(2).setValue(String.valueOf(destAddress.getPort()));
        final HttpUriRequest httpUriRequest = constructPostMethod(endpoint, parameters);
        final HttpResponse httpResponse = executeHttpRequest(httpUriRequest);
        Assert.assertEquals(HttpStatus.SC_OK, httpResponse.getStatusLine().getStatusCode());
    }

    private InetSocketAddress getNcEndPoint(String name) {
        if (ncEndPoints == null || !ncEndPoints.containsKey(name)) {
            throw new IllegalStateException("No end point specified for node: " + name);
        }
        return ncEndPoints.get(name);
    }

    private InetSocketAddress getNcReplicationAddress(String nodeId) {
        if (replicationAddress == null || !replicationAddress.containsKey(nodeId)) {
            throw new IllegalStateException("No replication address specified for node: " + nodeId);
        }
        return replicationAddress.get(nodeId);
    }

    private void handlePortCommand(String[] command) throws InterruptedException, TimeoutException {
        if (command.length != 3) {
            throw new IllegalStateException("Unrecognized port command. Expected (host port timeout(sec))");
        }
        String host = command[0];
        int port = Integer.parseInt(command[1]);
        int timeoutSec = (int) (Integer.parseInt(command[2]) * timeoutMultiplier);
        while (isPortActive(host, port)) {
            TimeUnit.SECONDS.sleep(1);
            timeoutSec--;
            if (timeoutSec <= 0) {
                throw new TimeoutException(
                        "Port is still in use: " + host + ":" + port + " after " + command[2] + " secs");
            }
        }
    }

    private boolean isPortActive(String host, int port) {
        try (Socket ignored = new Socket(host, port)) {
            return true;
        } catch (IOException ignored) {
            return false;
        }
    }

    public abstract static class TestLoop extends Exception {

        private static final long serialVersionUID = 1L;
        private final String target;

        TestLoop(String target) {
            this.target = target;
        }

        static TestLoop createLoop(String target, final int count) {
            LOGGER.info("Starting loop '" + count + " times back to '" + target + "'...");
            return new TestLoop(target) {
                private static final long serialVersionUID = 1L;
                int remainingLoops = count;

                @Override
                void executeLoop() throws TestLoop {
                    if (remainingLoops-- > 0) {
                        throw this;
                    }
                    LOGGER.info("Loop to '" + target + "' complete!");
                }
            };
        }

        static TestLoop createLoop(String target, long duration, TimeUnit unit) {
            LOGGER.info("Starting loop for " + unit.toSeconds(duration) + "s back to '" + target + "'...");
            return new TestLoop(target) {
                private static final long serialVersionUID = 1L;
                long endTime = unit.toMillis(duration) + System.currentTimeMillis();

                @Override
                void executeLoop() throws TestLoop {
                    if (System.currentTimeMillis() < endTime) {
                        throw this;
                    }
                    LOGGER.info("Loop to '" + target + "' complete!");
                }
            };
        }

        abstract void executeLoop() throws TestLoop;

        public String getTarget() {
            return target;
        }
    }

    protected static boolean containsClientContextID(String statement) {
        List<Parameter> httpParams = extractParameters(statement);
        return httpParams.stream().map(Parameter::getName)
                .anyMatch(QueryServiceServlet.Parameter.CLIENT_ID.str()::equals);
    }

    private static boolean isCancellable(String type) {
        return !NON_CANCELLABLE.contains(type);
    }

    private InputStream query(CompilationUnit cUnit, String testFile, String statement, Charset responseCharset)
            throws Exception {
        final URI uri = getQueryServiceUri(testFile);
        final InputStream inputStream = executeQueryService(statement, OutputFormat.forCompilationUnit(cUnit), uri,
                cUnit.getParameter(), true, responseCharset);
        return ResultExtractor.extract(inputStream, responseCharset).getResult();
    }

    private URI getQueryServiceUri(String extension) throws URISyntaxException {
        return extension.endsWith(AQL) ? getEndpoint(Servlets.QUERY_AQL) : getEndpoint(Servlets.QUERY_SERVICE);
    }

    private void validateWarnings(List<String> actualWarnings, List<String> expectedWarn, MutableInt actualWarnCount,
            boolean expectedSourceLoc) throws Exception {
        if (actualWarnings != null) {
            for (String actualWarn : actualWarnings) {
                if (expectedWarn.stream().noneMatch(actualWarn::contains)) {
                    throw new Exception("unexpected warning was encountered (" + actualWarn + ")");
                }
                if (expectedSourceLoc && !containsSourceLocation(actualWarn)) {
                    throw new Exception(MessageFormat.format(
                            "Expected to find source location \"{}, {}\" in warning text: +++++{}+++++",
                            ERR_MSG_SRC_LOC_LINE_REGEX, ERR_MSG_SRC_LOC_COLUMN_REGEX, actualWarn));
                }
                actualWarnCount.increment();
                if (actualWarnCount.getValue() > expectedWarn.size()) {
                    throw new Exception("returned warnings exceeded expected warnings");
                }
            }
        }
    }

    private static String toQueryServiceHandle(String handle) {
        return handle.replace("/aql/", "/service/");
    }

    private static boolean isCcEndPointPath(String endPoint) {
        return !endPoint.startsWith(NC_ENDPOINT_PREFIX);
    }

    private static boolean containsPort(String endPoint) {
        return StringUtils.contains(endPoint, ':');
    }
}
