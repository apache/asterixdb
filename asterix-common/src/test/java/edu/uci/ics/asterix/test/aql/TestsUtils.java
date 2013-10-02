/*
 * Copyright 2009-2013 by The Regents of the University of California
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * you may obtain a copy of the License from
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.uci.ics.asterix.test.aql;

import static org.junit.Assert.fail;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.commons.httpclient.DefaultHttpMethodRetryHandler;
import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.HttpStatus;
import org.apache.commons.httpclient.NameValuePair;
import org.apache.commons.httpclient.methods.GetMethod;
import org.apache.commons.httpclient.params.HttpMethodParams;
import org.apache.commons.io.IOUtils;
import org.codehaus.jackson.map.JsonMappingException;
import org.json.JSONException;
import org.json.JSONObject;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;

import edu.uci.ics.asterix.common.config.GlobalConfig;
import edu.uci.ics.asterix.testframework.context.TestCaseContext;
import edu.uci.ics.asterix.testframework.context.TestFileContext;
import edu.uci.ics.asterix.testframework.xml.TestCase.CompilationUnit;

public class TestsUtils {

    private static final String EXTENSION_AQL_RESULT = "adm";
    private static final Logger LOGGER = Logger.getLogger(TestsUtils.class.getName());
    private static Method managixExecuteMethod = null;

    /**
     * Probably does not work well with symlinks.
     */
    public static boolean deleteRec(File path) {
        if (path.isDirectory()) {
            for (File f : path.listFiles()) {
                if (!deleteRec(f)) {
                    return false;
                }
            }
        }
        return path.delete();
    }

    public static void runScriptAndCompareWithResult(File scriptFile, PrintWriter print, File expectedFile,
            File actualFile) throws Exception {
        BufferedReader readerExpected = new BufferedReader(new InputStreamReader(new FileInputStream(expectedFile),
                "UTF-8"));
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
                    throw new Exception("Result for " + scriptFile + " changed at line " + num + ":\n< " + lineExpected
                            + "\n> ");
                }

                if (!equalStrings(lineExpected.split("Timestamp")[0], lineActual.split("Timestamp")[0])) {
                    fail("Result for " + scriptFile + " changed at line " + num + ":\n< " + lineExpected + "\n> "
                            + lineActual);
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

    private static boolean equalStrings(String s1, String s2) {
        String[] rowsOne = s1.split("\n");
        String[] rowsTwo = s2.split("\n");

        for (int i = 0; i < rowsOne.length; i++) {
            String row1 = rowsOne[i];
            String row2 = rowsTwo[i];

            if (row1.equals(row2))
                continue;

            String[] fields1 = row1.split(" ");
            String[] fields2 = row2.split(" ");

            for (int j = 0; j < fields1.length; j++) {
                if (fields1[j].equals(fields2[j])) {
                    continue;
                } else if (fields1[j].indexOf('.') < 0) {
                    return false;
                } else {
                    fields1[j] = fields1[j].split(",")[0];
                    fields2[j] = fields2[j].split(",")[0];
                    Double double1 = Double.parseDouble(fields1[j]);
                    Double double2 = Double.parseDouble(fields2[j]);
                    float float1 = (float) double1.doubleValue();
                    float float2 = (float) double2.doubleValue();

                    if (Math.abs(float1 - float2) == 0)
                        continue;
                    else {
                        return false;
                    }
                }
            }
        }
        return true;
    }

    public static String aqlExtToResExt(String fname) {
        int dot = fname.lastIndexOf('.');
        return fname.substring(0, dot + 1) + EXTENSION_AQL_RESULT;
    }

    public static void writeResultsToFile(File actualFile, InputStream resultStream) throws IOException, JSONException {
        BufferedWriter writer = new BufferedWriter(new FileWriter(actualFile));
        try {
            JsonFactory jsonFactory = new JsonFactory();
            JsonParser resultParser = jsonFactory.createParser(resultStream);
            while (resultParser.nextToken() == JsonToken.START_OBJECT) {
                while (resultParser.nextToken() != JsonToken.END_OBJECT) {
                    String key = resultParser.getCurrentName();
                    if (key.equals("results")) {
                        // Start of array.
                        resultParser.nextToken();
                        while (resultParser.nextToken() != JsonToken.END_ARRAY) {
                            String record = resultParser.getValueAsString();
                            writer.write(record);
                        }
                    } else {
                        String summary = resultParser.getValueAsString();
                        if (key.equals("summary")) {
                            writer.write(summary);
                            throw new JsonMappingException("Could not find results key in the JSON Object");
                        }
                    }
                }
            }
        } finally {
            writer.close();
        }
    }

    private static String[] handleError(GetMethod method) throws Exception {
        String errorBody = method.getResponseBodyAsString();
        JSONObject result = new JSONObject(errorBody);
        String[] errors = { result.getJSONArray("error-code").getString(0), result.getString("summary"),
                result.getString("stacktrace") };
        return errors;
    }

    // Executes Query and returns results as JSONArray
    public static InputStream executeQuery(String str) throws Exception {
        InputStream resultStream = null;

        final String url = "http://localhost:19002/query";

        // Create an instance of HttpClient.
        HttpClient client = new HttpClient();

        // Create a method instance.
        GetMethod method = new GetMethod(url);

        method.setQueryString(new NameValuePair[] { new NameValuePair("query", str) });

        // Provide custom retry handler is necessary
        method.getParams().setParameter(HttpMethodParams.RETRY_HANDLER, new DefaultHttpMethodRetryHandler(3, false));

        try {
            // Execute the method.
            int statusCode = client.executeMethod(method);

            // Check if the method was executed successfully.
            if (statusCode != HttpStatus.SC_OK) {
                GlobalConfig.ASTERIX_LOGGER.log(Level.SEVERE, "Method failed: " + method.getStatusLine());
            }

            // Read the response body as stream
            resultStream = method.getResponseBodyAsStream();
        } catch (Exception e) {
            GlobalConfig.ASTERIX_LOGGER.log(Level.SEVERE, e.getMessage(), e);
            e.printStackTrace();
        }
        return resultStream;
    }

    // To execute Update statements
    // Insert and Delete statements are executed here
    public static void executeUpdate(String str) throws Exception {
        final String url = "http://localhost:19002/update";

        // Create an instance of HttpClient.
        HttpClient client = new HttpClient();

        // Create a method instance.
        GetMethod method = new GetMethod(url);

        method.setQueryString(new NameValuePair[] { new NameValuePair("statements", str) });

        // Provide custom retry handler is necessary
        method.getParams().setParameter(HttpMethodParams.RETRY_HANDLER, new DefaultHttpMethodRetryHandler(3, false));

        // Execute the method.
        int statusCode = client.executeMethod(method);

        // Check if the method was executed successfully.
        if (statusCode != HttpStatus.SC_OK) {
            GlobalConfig.ASTERIX_LOGGER.log(Level.SEVERE, "Method failed: " + method.getStatusLine());
            String[] errors = handleError(method);
            GlobalConfig.ASTERIX_LOGGER.log(Level.SEVERE, errors[2]);
            throw new Exception("DML operation failed: " + errors[0]);
        }
    }

    // To execute DDL and Update statements
    // create type statement
    // create dataset statement
    // create index statement
    // create dataverse statement
    // create function statement
    public static void executeDDL(String str) throws Exception {
        final String url = "http://localhost:19002/ddl";

        // Create an instance of HttpClient.
        HttpClient client = new HttpClient();

        // Create a method instance.
        GetMethod method = new GetMethod(url);

        method.setQueryString(new NameValuePair[] { new NameValuePair("ddl", str) });

        // Provide custom retry handler is necessary
        method.getParams().setParameter(HttpMethodParams.RETRY_HANDLER, new DefaultHttpMethodRetryHandler(3, false));

        // Execute the method.
        int statusCode = client.executeMethod(method);

        // Check if the method was executed successfully.
        if (statusCode != HttpStatus.SC_OK) {
            GlobalConfig.ASTERIX_LOGGER.log(Level.SEVERE, "Method failed: " + method.getStatusLine());
            String[] errors = handleError(method);
            GlobalConfig.ASTERIX_LOGGER.log(Level.SEVERE, errors[2]);
            throw new Exception("DDL operation failed: " + errors[0]);
        }
    }

    // Method that reads a DDL/Update/Query File
    // and returns the contents as a string
    // This string is later passed to REST API for execution.
    public static String readTestFile(File testFile) throws Exception {
        BufferedReader reader = new BufferedReader(new FileReader(testFile));
        String line = null;
        StringBuilder stringBuilder = new StringBuilder();
        String ls = System.getProperty("line.separator");

        while ((line = reader.readLine()) != null) {
            stringBuilder.append(line);
            stringBuilder.append(ls);
        }

        return stringBuilder.toString();
    }

    public static void executeManagixCommand(String command) throws ClassNotFoundException, NoSuchMethodException,
            SecurityException, IllegalAccessException, IllegalArgumentException, InvocationTargetException {
        if (managixExecuteMethod == null) {
            Class clazz = Class.forName("edu.uci.ics.asterix.installer.test.AsterixInstallerIntegrationUtil");
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

    public static void executeTest(String actualPath, TestCaseContext testCaseCtx, ProcessBuilder pb) throws Exception {

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
            LOGGER.info("Starting [TEST]: " + testCaseCtx.getTestCase().getFilePath() + "/" + cUnit.getName() + " ... ");
            testFileCtxs = testCaseCtx.getTestFiles(cUnit);
            expectedResultFileCtxs = testCaseCtx.getExpectedResultFiles(cUnit);

            for (TestFileContext ctx : testFileCtxs) {
                testFile = ctx.getFile();
                statement = TestsUtils.readTestFile(testFile);
                try {
                    switch (ctx.getType()) {
                        case "ddl":
                            TestsUtils.executeDDL(statement);
                            break;
                        case "update":
                            TestsUtils.executeUpdate(statement);
                            break;
                        case "query":
                            try {
                                InputStream resultStream = executeQuery(statement);
                                expectedResultFile = expectedResultFileCtxs.get(queryCount).getFile();

                                File actualFile = new File(actualPath + File.separator
                                        + testCaseCtx.getTestCase().getFilePath().replace(File.separator, "_") + "_"
                                        + cUnit.getName() + ".adm");
                                TestsUtils.writeResultsToFile(actualFile, resultStream);

                                File actualResultFile = testCaseCtx.getActualResultFile(cUnit, new File(actualPath));
                                actualResultFile.getParentFile().mkdirs();

                                TestsUtils.runScriptAndCompareWithResult(testFile, new PrintWriter(System.err),
                                        expectedResultFile, actualFile);
                                LOGGER.info("[TEST]: " + testCaseCtx.getTestCase().getFilePath() + "/"
                                        + cUnit.getName() + " PASSED ");
                            } catch (JsonMappingException e) {
                                throw new Exception("Test \"" + testFile + "\" FAILED!\n");
                            }
                            queryCount++;
                            break;
                        case "mgx":
                            executeManagixCommand(statement);
                            break;
                        case "txnqbc": //qbc represents query before crash
                            try {
                                InputStream resultStream = executeQuery(statement);
                                qbcFile = new File(actualPath + File.separator
                                        + testCaseCtx.getTestCase().getFilePath().replace(File.separator, "_") + "_"
                                        + cUnit.getName() + "_qbc.adm");
                                qbcFile.getParentFile().mkdirs();
                                TestsUtils.writeResultsToFile(qbcFile, resultStream);
                            } catch (JsonMappingException e) {
                                throw new Exception("Test \"" + testFile + "\" FAILED!\n");
                            }
                            break;
                        case "txnqar": //qar represents query after recovery
                            try {
                                InputStream resultStream = executeQuery(statement);
                                qarFile = new File(actualPath + File.separator
                                        + testCaseCtx.getTestCase().getFilePath().replace(File.separator, "_") + "_"
                                        + cUnit.getName() + "_qbc.adm");
                                qarFile.getParentFile().mkdirs();
                                TestsUtils.writeResultsToFile(qarFile, resultStream);
                                TestsUtils.runScriptAndCompareWithResult(testFile, new PrintWriter(System.err),
                                        qbcFile, qarFile);
                                LOGGER.info("[TEST]: " + testCaseCtx.getTestCase().getFilePath() + "/"
                                        + cUnit.getName() + " PASSED ");
                            } catch (JsonMappingException e) {
                                throw new Exception("Test \"" + testFile + "\" FAILED!\n");
                            }
                            break;
                        case "txneu": //eu represents erroneous update
                            try {
                                TestsUtils.executeUpdate(statement);
                            } catch (Exception e) {
                                //An exception is expected.
                            }
                            break;
                        case "script":
                            try {
                                String output = executeScript(
                                        pb,
                                        getScriptPath(testFile.getAbsolutePath(), pb.environment().get("SCRIPT_HOME"),
                                                statement.trim()));
                                if(output.contains("ERROR")) {
                                    throw new Exception(output);
                                }
                            } catch (Exception e) {
                                throw new Exception("Test \"" + testFile + "\" FAILED!\n", e);
                            }
                            break;
                        default:
                            throw new IllegalArgumentException("No statements of type " + ctx.getType());
                    }

                } catch (Exception e) {
                    if (cUnit.getExpectedError().isEmpty()) {
                        throw new Exception("Test \"" + testFile + "\" FAILED!", e);
                    }
                }
            }
        }
    }
}
