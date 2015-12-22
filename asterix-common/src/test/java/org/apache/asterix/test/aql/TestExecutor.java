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

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.URL;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.asterix.common.config.GlobalConfig;
import org.apache.asterix.testframework.context.TestCaseContext;
import org.apache.asterix.testframework.context.TestCaseContext.OutputFormat;
import org.apache.asterix.testframework.context.TestFileContext;
import org.apache.asterix.testframework.xml.TestCase.CompilationUnit;
import org.apache.asterix.testframework.xml.TestGroup;
import org.apache.commons.httpclient.DefaultHttpMethodRetryHandler;
import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.HttpMethod;
import org.apache.commons.httpclient.HttpMethodBase;
import org.apache.commons.httpclient.HttpStatus;
import org.apache.commons.httpclient.NameValuePair;
import org.apache.commons.httpclient.methods.GetMethod;
import org.apache.commons.httpclient.methods.PostMethod;
import org.apache.commons.httpclient.methods.StringRequestEntity;
import org.apache.commons.httpclient.params.HttpMethodParams;
import org.apache.commons.io.IOUtils;
import org.json.JSONObject;

public class TestExecutor {

    protected static final Logger LOGGER = Logger.getLogger(TestExecutor.class.getName());
    // see
    // https://stackoverflow.com/questions/417142/what-is-the-maximum-length-of-a-url-in-different-browsers/417184
    private static final long MAX_URL_LENGTH = 2000l;
    private static Method managixExecuteMethod = null;

    private static String host;
    private static int port;

    public TestExecutor() {
        this.host = "127.0.0.1";
        this.port = 19002;
    }

    public TestExecutor(String host, int port){
        this.host = host;
        this.port = port;
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

    public void runScriptAndCompareWithResult(File scriptFile, PrintWriter print, File expectedFile, File actualFile)
            throws Exception {
        System.err.println("Expected results file: " + expectedFile.toString());
        BufferedReader readerExpected = new BufferedReader(
                new InputStreamReader(new FileInputStream(expectedFile), "UTF-8"));
        BufferedReader readerActual = new BufferedReader(
                new InputStreamReader(new FileInputStream(actualFile), "UTF-8"));
        String lineExpected, lineActual;
        int num = 1;
        try {
            while ((lineExpected = readerExpected.readLine()) != null) {
                lineActual = readerActual.readLine();
                // Assert.assertEquals(lineExpected, lineActual);
                if (lineActual == null) {
                    if (lineExpected.isEmpty()) {
                        continue;
                    }
                    throw new Exception(
                            "Result for " + scriptFile + " changed at line " + num + ":\n< " + lineExpected + "\n> ");
                }

                if (!equalStrings(lineExpected.split("Time")[0], lineActual.split("Time")[0])) {
                    throw new Exception("Result for " + scriptFile + " changed at line " + num + ":\n< " + lineExpected
                            + "\n> " + lineActual);
                }

                ++num;
            }
            lineActual = readerActual.readLine();
            // Assert.assertEquals(null, lineActual);
            if (lineActual != null) {
                throw new Exception("Result for " + scriptFile + " changed at line " + num + ":\n< \n> " + lineActual);
            }
            // actualFile.delete();
        } finally {
            readerExpected.close();
            readerActual.close();
        }

    }

    private boolean equalStrings(String s1, String s2) {
        String[] rowsOne = s1.split("\n");
        String[] rowsTwo = s2.split("\n");

        for (int i = 0; i < rowsOne.length; i++) {
            String row1 = rowsOne[i];
            String row2 = rowsTwo[i];

            if (row1.equals(row2))
                continue;

            String[] fields1 = row1.split(" ");
            String[] fields2 = row2.split(" ");

            boolean bagEncountered = false;
            Set<String> bagElements1 = new HashSet<String>();
            Set<String> bagElements2 = new HashSet<String>();

            for (int j = 0; j < fields1.length; j++) {
                if (j >= fields2.length) {
                    return false;
                } else if (fields1[j].equals(fields2[j])) {
                    if (fields1[j].equals("{{"))
                        bagEncountered = true;
                    if (fields1[j].startsWith("}}")) {
                        if (!bagElements1.equals(bagElements2))
                            return false;
                        bagEncountered = false;
                        bagElements1.clear();
                        bagElements2.clear();
                    }
                    continue;
                } else if (fields1[j].indexOf('.') < 0) {
                    if (bagEncountered) {
                        bagElements1.add(fields1[j].replaceAll(",$", ""));
                        bagElements2.add(fields2[j].replaceAll(",$", ""));
                        continue;
                    }
                    return false;
                } else {
                    // If the fields are floating-point numbers, test them
                    // for equality safely
                    fields1[j] = fields1[j].split(",")[0];
                    fields2[j] = fields2[j].split(",")[0];
                    try {
                        Double double1 = Double.parseDouble(fields1[j]);
                        Double double2 = Double.parseDouble(fields2[j]);
                        float float1 = (float) double1.doubleValue();
                        float float2 = (float) double2.doubleValue();

                        if (Math.abs(float1 - float2) == 0)
                            continue;
                        else {
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

    // For tests where you simply want the byte-for-byte output.
    private static void writeOutputToFile(File actualFile, InputStream resultStream) throws Exception {
        byte[] buffer = new byte[10240];
        int len;
        java.io.FileOutputStream out = new java.io.FileOutputStream(actualFile);
        try {
            while ((len = resultStream.read(buffer)) != -1) {
                out.write(buffer, 0, len);
            }
        } finally {
            out.close();
        }
    }

    private int executeHttpMethod(HttpMethod method) throws Exception {
        HttpClient client = new HttpClient();
        int statusCode;
        try {
            statusCode = client.executeMethod(method);
        } catch (Exception e) {
            GlobalConfig.ASTERIX_LOGGER.log(Level.SEVERE, e.getMessage(), e);
            e.printStackTrace();
            throw e;
        }
        if (statusCode != HttpStatus.SC_OK) {
            // QQQ For now, we are indeed assuming we get back JSON errors.
            // In future this may be changed depending on the requested
            // output format sent to the servlet.
            String errorBody = method.getResponseBodyAsString();
            JSONObject result = new JSONObject(errorBody);
            String[] errors = { result.getJSONArray("error-code").getString(0), result.getString("summary"),
                    result.getString("stacktrace") };
            GlobalConfig.ASTERIX_LOGGER.log(Level.SEVERE, errors[2]);
            throw new Exception("HTTP operation failed: " + errors[0] + "\nSTATUS LINE: " + method.getStatusLine()
                    + "\nSUMMARY: " + errors[1] + "\nSTACKTRACE: " + errors[2]);
        }
        return statusCode;
    }

    // Executes Query and returns results as JSONArray
    public InputStream executeQuery(String str, OutputFormat fmt, String url, List<CompilationUnit.Parameter> params)
            throws Exception {
        HttpMethodBase method = null;
        if (str.length() + url.length() < MAX_URL_LENGTH) {
            //Use GET for small-ish queries
            method = new GetMethod(url);
            NameValuePair[] parameters = new NameValuePair[params.size() + 1];
            parameters[0] = new NameValuePair("query", str);
            int i = 1;
            for (CompilationUnit.Parameter param : params) {
                parameters[i++] = new NameValuePair(param.getName(), param.getValue());
            }
            method.setQueryString(parameters);
        } else {
            //Use POST for bigger ones to avoid 413 FULL_HEAD
            // QQQ POST API doesn't allow encoding additional parameters
            method = new PostMethod(url);
            ((PostMethod) method).setRequestEntity(new StringRequestEntity(str));
        }

        //Set accepted output response type
        method.setRequestHeader("Accept", fmt.mimeType());
        // Provide custom retry handler is necessary
        method.getParams().setParameter(HttpMethodParams.RETRY_HANDLER, new DefaultHttpMethodRetryHandler(3, false));
        executeHttpMethod(method);
        return method.getResponseBodyAsStream();
    }

    // To execute Update statements
    // Insert and Delete statements are executed here
    public void executeUpdate(String str, String url) throws Exception {
        // Create a method instance.
        PostMethod method = new PostMethod(url);
        method.setRequestEntity(new StringRequestEntity(str));

        // Provide custom retry handler is necessary
        method.getParams().setParameter(HttpMethodParams.RETRY_HANDLER, new DefaultHttpMethodRetryHandler(3, false));

        // Execute the method.
        executeHttpMethod(method);
    }

    // Executes AQL in either async or async-defer mode.
    public InputStream executeAnyAQLAsync(String str, boolean defer, OutputFormat fmt, String url) throws Exception {
        // Create a method instance.
        PostMethod method = new PostMethod(url);
        if (defer) {
            method.setQueryString(new NameValuePair[] { new NameValuePair("mode", "asynchronous-deferred") });
        } else {
            method.setQueryString(new NameValuePair[] { new NameValuePair("mode", "asynchronous") });
        }
        method.setRequestEntity(new StringRequestEntity(str));
        method.setRequestHeader("Accept", fmt.mimeType());

        // Provide custom retry handler is necessary
        method.getParams().setParameter(HttpMethodParams.RETRY_HANDLER, new DefaultHttpMethodRetryHandler(3, false));
        executeHttpMethod(method);
        InputStream resultStream = method.getResponseBodyAsStream();

        String theHandle = IOUtils.toString(resultStream, "UTF-8");

        // take the handle and parse it so results can be retrieved
        InputStream handleResult = getHandleResult(theHandle, fmt);
        return handleResult;
    }

    private InputStream getHandleResult(String handle, OutputFormat fmt) throws Exception {
        final String url = "http://"+host+":"+port+"/query/result";

        // Create a method instance.
        GetMethod method = new GetMethod(url);
        method.setQueryString(new NameValuePair[] { new NameValuePair("handle", handle) });
        method.setRequestHeader("Accept", fmt.mimeType());

        // Provide custom retry handler is necessary
        method.getParams().setParameter(HttpMethodParams.RETRY_HANDLER, new DefaultHttpMethodRetryHandler(3, false));

        executeHttpMethod(method);
        return method.getResponseBodyAsStream();
    }

    // To execute DDL and Update statements
    // create type statement
    // create dataset statement
    // create index statement
    // create dataverse statement
    // create function statement
    public void executeDDL(String str, String url) throws Exception {
        // Create a method instance.
        PostMethod method = new PostMethod(url);
        method.setRequestEntity(new StringRequestEntity(str));
        // Provide custom retry handler is necessary
        method.getParams().setParameter(HttpMethodParams.RETRY_HANDLER, new DefaultHttpMethodRetryHandler(3, false));

        // Execute the method.
        executeHttpMethod(method);
    }

    // Method that reads a DDL/Update/Query File
    // and returns the contents as a string
    // This string is later passed to REST API for execution.
    public String readTestFile(File testFile) throws Exception {
        BufferedReader reader = new BufferedReader(new FileReader(testFile));
        String line = null;
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
        pb.command(scriptPath);
        Process p = pb.start();
        p.waitFor();
        return getProcessOutput(p);
    }

    private static String getScriptPath(String queryPath, String scriptBasePath, String scriptFileName) {
        String targetWord = "queries" + File.separator;
        int targetWordSize = targetWord.lastIndexOf(File.separator);
        int beginIndex = queryPath.lastIndexOf(targetWord) + targetWordSize;
        int endIndex = queryPath.lastIndexOf(File.separator);
        String prefix = queryPath.substring(beginIndex, endIndex);
        String scriptPath = scriptBasePath + prefix + File.separator + scriptFileName;
        return scriptPath;
    }

    private static String getProcessOutput(Process p) throws Exception {
        StringBuilder s = new StringBuilder();
        BufferedInputStream bisIn = new BufferedInputStream(p.getInputStream());
        StringWriter writerIn = new StringWriter();
        IOUtils.copy(bisIn, writerIn, "UTF-8");
        s.append(writerIn.toString());

        BufferedInputStream bisErr = new BufferedInputStream(p.getErrorStream());
        StringWriter writerErr = new StringWriter();
        IOUtils.copy(bisErr, writerErr, "UTF-8");
        s.append(writerErr.toString());
        if (writerErr.toString().length() > 0) {
            StringBuilder sbErr = new StringBuilder();
            sbErr.append("script execution failed - error message:\n");
            sbErr.append("-------------------------------------------\n");
            sbErr.append(s.toString());
            sbErr.append("-------------------------------------------\n");
            LOGGER.info(sbErr.toString().trim());
            throw new Exception(s.toString().trim());
        }
        return s.toString();
    }

    public void executeTest(String actualPath, TestCaseContext testCaseCtx, ProcessBuilder pb,
            boolean isDmlRecoveryTest) throws Exception {
        executeTest(actualPath, testCaseCtx, pb, isDmlRecoveryTest, null);
    }

    public void executeTest(String actualPath, TestCaseContext testCaseCtx, ProcessBuilder pb,
            boolean isDmlRecoveryTest, TestGroup failedGroup) throws Exception {

        File testFile;
        File expectedResultFile;
        String statement;
        List<TestFileContext> expectedResultFileCtxs;
        List<TestFileContext> testFileCtxs;
        File qbcFile = null;
        File qarFile = null;
        int queryCount = 0;

        List<CompilationUnit> cUnits = testCaseCtx.getTestCase().getCompilationUnit();
        for (CompilationUnit cUnit : cUnits) {
            LOGGER.info(
                    "Starting [TEST]: " + testCaseCtx.getTestCase().getFilePath() + "/" + cUnit.getName() + " ... ");
            testFileCtxs = testCaseCtx.getTestFiles(cUnit);
            expectedResultFileCtxs = testCaseCtx.getExpectedResultFiles(cUnit);
            for (TestFileContext ctx : testFileCtxs) {
                testFile = ctx.getFile();
                statement = readTestFile(testFile);
                boolean failed = false;
                try {
                    switch (ctx.getType()) {
                        case "ddl":
                            if (ctx.getFile().getName().endsWith("aql")) {
                                executeDDL(statement, "http://"+host+":"+port+"/ddl");
                            } else {
                                executeDDL(statement, "http://"+host+":"+port+"/ddl/sqlpp");
                            }
                            break;
                        case "update":
                            //isDmlRecoveryTest: set IP address
                            if (isDmlRecoveryTest && statement.contains("nc1://")) {
                                statement = statement.replaceAll("nc1://",
                                        "127.0.0.1://../../../../../../asterix-app/");
                            }
                            if (ctx.getFile().getName().endsWith("aql")) {
                                executeUpdate(statement, "http://"+host+":"+port+"/update");
                            } else {
                                executeUpdate(statement, "http://"+host+":"+port+"/update/sqlpp");
                            }
                            break;
                        case "query":
                        case "async":
                        case "asyncdefer":
                            // isDmlRecoveryTest: insert Crash and Recovery
                            if (isDmlRecoveryTest) {
                                executeScript(pb, pb.environment().get("SCRIPT_HOME") + File.separator + "dml_recovery"
                                        + File.separator + "kill_cc_and_nc.sh");
                                executeScript(pb, pb.environment().get("SCRIPT_HOME") + File.separator + "dml_recovery"
                                        + File.separator + "stop_and_start.sh");
                            }
                            InputStream resultStream = null;
                            OutputFormat fmt = OutputFormat.forCompilationUnit(cUnit);
                            if (ctx.getFile().getName().endsWith("aql")) {
                                if (ctx.getType().equalsIgnoreCase("query")) {
                                    resultStream = executeQuery(statement, fmt, "http://"+host+":"+port+"/query",
                                            cUnit.getParameter());
                                } else if (ctx.getType().equalsIgnoreCase("async")) {
                                    resultStream = executeAnyAQLAsync(statement, false, fmt,
                                            "http://"+host+":"+port+"/aql");
                                } else if (ctx.getType().equalsIgnoreCase("asyncdefer")) {
                                    resultStream = executeAnyAQLAsync(statement, true, fmt,
                                            "http://"+host+":"+port+"/aql");
                                }
                            } else {
                                if (ctx.getType().equalsIgnoreCase("query")) {
                                    resultStream = executeQuery(statement, fmt, "http://"+host+":"+port+"/query/sqlpp",
                                            cUnit.getParameter());
                                } else if (ctx.getType().equalsIgnoreCase("async")) {
                                    resultStream = executeAnyAQLAsync(statement, false, fmt,
                                            "http://"+host+":"+port+"/sqlpp");
                                } else if (ctx.getType().equalsIgnoreCase("asyncdefer")) {
                                    resultStream = executeAnyAQLAsync(statement, true, fmt,
                                            "http://"+host+":"+port+"/sqlpp");
                                }
                            }

                            if (queryCount >= expectedResultFileCtxs.size()) {
                                throw new IllegalStateException(
                                        "no result file for " + testFile.toString() + "; queryCount: " + queryCount
                                                + ", filectxs.size: " + expectedResultFileCtxs.size());
                            }
                            expectedResultFile = expectedResultFileCtxs.get(queryCount).getFile();

                            File actualResultFile = testCaseCtx.getActualResultFile(cUnit, new File(actualPath));
                            actualResultFile.getParentFile().mkdirs();
                            writeOutputToFile(actualResultFile, resultStream);

                            runScriptAndCompareWithResult(testFile, new PrintWriter(System.err), expectedResultFile,
                                    actualResultFile);
                            LOGGER.info("[TEST]: " + testCaseCtx.getTestCase().getFilePath() + "/" + cUnit.getName()
                                    + " PASSED ");

                            queryCount++;
                            break;
                        case "mgx":
                            executeManagixCommand(statement);
                            break;
                        case "txnqbc": //qbc represents query before crash
                            resultStream = executeQuery(statement, OutputFormat.forCompilationUnit(cUnit),
                                    "http://"+host+":"+port+"/query", cUnit.getParameter());
                            qbcFile = new File(actualPath + File.separator
                                    + testCaseCtx.getTestCase().getFilePath().replace(File.separator, "_") + "_"
                                    + cUnit.getName() + "_qbc.adm");
                            qbcFile.getParentFile().mkdirs();
                            writeOutputToFile(qbcFile, resultStream);
                            break;
                        case "txnqar": //qar represents query after recovery
                            resultStream = executeQuery(statement, OutputFormat.forCompilationUnit(cUnit),
                                    "http://"+host+":"+port+"/query", cUnit.getParameter());
                            qarFile = new File(actualPath + File.separator
                                    + testCaseCtx.getTestCase().getFilePath().replace(File.separator, "_") + "_"
                                    + cUnit.getName() + "_qar.adm");
                            qarFile.getParentFile().mkdirs();
                            writeOutputToFile(qarFile, resultStream);
                            runScriptAndCompareWithResult(testFile, new PrintWriter(System.err), qbcFile, qarFile);

                            LOGGER.info("[TEST]: " + testCaseCtx.getTestCase().getFilePath() + "/" + cUnit.getName()
                                    + " PASSED ");
                            break;
                        case "txneu": //eu represents erroneous update
                            try {
                                executeUpdate(statement, "http://"+host+":"+port+"/update");
                            } catch (Exception e) {
                                //An exception is expected.
                                failed = true;
                                e.printStackTrace();
                            }
                            if (!failed) {
                                throw new Exception(
                                        "Test \"" + testFile + "\" FAILED!\n  An exception" + "is expected.");
                            }
                            System.err.println("...but that was expected.");
                            break;
                        case "script":
                            try {
                                String output = executeScript(pb, getScriptPath(testFile.getAbsolutePath(),
                                        pb.environment().get("SCRIPT_HOME"), statement.trim()));
                                if (output.contains("ERROR")) {
                                    throw new Exception(output);
                                }
                            } catch (Exception e) {
                                throw new Exception("Test \"" + testFile + "\" FAILED!\n", e);
                            }
                            break;
                        case "sleep":
                            Thread.sleep(Long.parseLong(statement.trim()));
                            break;
                        case "errddl": // a ddlquery that expects error
                            try {
                                executeDDL(statement, "http://"+host+":"+port+"/ddl");
                            } catch (Exception e) {
                                // expected error happens
                                failed = true;
                                e.printStackTrace();
                            }
                            if (!failed) {
                                throw new Exception(
                                        "Test \"" + testFile + "\" FAILED!\n  An exception" + "is expected.");
                            }
                            System.err.println("...but that was expected.");
                            break;
                        default:
                            throw new IllegalArgumentException("No statements of type " + ctx.getType());
                    }

                } catch (Exception e) {

                    System.err.println("testFile " + testFile.toString() + " raised an exception:");

                    e.printStackTrace();
                    if (cUnit.getExpectedError().isEmpty()) {
                        System.err.println("...Unexpected!");
                        if (failedGroup != null) {
                            failedGroup.getTestCase().add(testCaseCtx.getTestCase());
                        }
                        throw new Exception("Test \"" + testFile + "\" FAILED!", e);
                    } else {
                        LOGGER.info("[TEST]: " + testCaseCtx.getTestCase().getFilePath() + "/" + cUnit.getName()
                                + " failed as expected: " + e.getMessage());
                        System.err.println("...but that was expected.");
                    }

                }
            }
        }
    }
}
