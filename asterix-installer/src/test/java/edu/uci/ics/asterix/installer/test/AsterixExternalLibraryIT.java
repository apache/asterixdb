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
package edu.uci.ics.asterix.installer.test;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.logging.Logger;

import org.apache.commons.httpclient.DefaultHttpMethodRetryHandler;
import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.HttpStatus;
import org.apache.commons.httpclient.NameValuePair;
import org.apache.commons.httpclient.methods.GetMethod;
import org.apache.commons.httpclient.params.HttpMethodParams;
import org.json.JSONObject;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runners.Parameterized.Parameters;

import edu.uci.ics.asterix.test.aql.TestsUtils;
import edu.uci.ics.asterix.testframework.context.TestCaseContext;
import edu.uci.ics.asterix.testframework.context.TestFileContext;
import edu.uci.ics.asterix.testframework.xml.TestCase.CompilationUnit;

public class AsterixExternalLibraryIT {

    private static final String LIBRARY_NAME = "testlib";
    private static final String LIBRARY_DATAVERSE = "external";
    private static final String PATH_BASE = "src/test/resources/integrationts/";
    private static final String PATH_ACTUAL = "ittest/";
    private static final Logger LOGGER = Logger.getLogger(AsterixExternalLibraryIT.class.getName());

    @BeforeClass
    public static void setUp() throws Exception {
        AsterixInstallerIntegrationUtil.init();
        String libraryPath = AsterixInstallerIntegrationUtil.getManagixHome() + File.separator
                + "asterix-external-data" + File.separator + "target" + File.separator + "testlib-binary-assembly.zip";
        AsterixInstallerIntegrationUtil.installLibrary(LIBRARY_NAME, LIBRARY_DATAVERSE, libraryPath);
    }

    @AfterClass
    public static void tearDown() throws Exception {
        AsterixInstallerIntegrationUtil.deinit();
    }

    private TestCaseContext tcCtx;

    public AsterixExternalLibraryIT(TestCaseContext tcCtx) {
        this.tcCtx = tcCtx;
    }

    @Parameters
    public static Collection<Object[]> tests() throws Exception {
        Collection<Object[]> testArgs = new ArrayList<Object[]>();
        TestCaseContext.Builder b = new TestCaseContext.Builder();
        for (TestCaseContext ctx : b.build(new File(PATH_BASE))) {
            testArgs.add(new Object[] { ctx });
        }
        return testArgs;
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

        return stringBuilder.toString();
    }

    // To execute DDL and Update statements
    // create type statement
    // create dataset statement
    // create index statement
    // create dataverse statement
    // create function statement
    public void executeDDL(String str) throws Exception {
        final String url = "http://localhost:19101/ddl";

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
            System.err.println("Method failed: " + method.getStatusLine());
        }
    }

    // To execute Update statements
    // Insert and Delete statements are executed here
    public void executeUpdate(String str) throws Exception {
        final String url = "http://localhost:19101/update";

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
            System.err.println("Method failed: " + method.getStatusLine());
        }
    }

    // Executes Query and returns results as JSONArray
    public JSONObject executeQuery(String str) throws Exception {

        final String url = "http://localhost:19101/query";

        // Create an instance of HttpClient.
        HttpClient client = new HttpClient();

        // Create a method instance.
        GetMethod method = new GetMethod(url);

        method.setQueryString(new NameValuePair[] { new NameValuePair("query", str) });

        // Provide custom retry handler is necessary
        method.getParams().setParameter(HttpMethodParams.RETRY_HANDLER, new DefaultHttpMethodRetryHandler(3, false));

        JSONObject result = null;

        try {
            // Execute the method.
            int statusCode = client.executeMethod(method);

            // Check if the method was executed successfully.
            if (statusCode != HttpStatus.SC_OK) {
                System.err.println("Method failed: " + method.getStatusLine());
            }

            // Read the response body as String.
            String responseBody = method.getResponseBodyAsString();

            result = new JSONObject(responseBody);
        } catch (Exception e) {
            System.out.println(e.getMessage());
            e.printStackTrace();
        }
        return result;
    }

    @Test
    public void test() throws Exception {
        List<TestFileContext> testFileCtxs;
        List<TestFileContext> expectedResultFileCtxs;

        File testFile;
        File expectedResultFile;
        String statement;

        int queryCount = 0;
        JSONObject result;

        List<CompilationUnit> cUnits = tcCtx.getTestCase().getCompilationUnit();
        for (CompilationUnit cUnit : cUnits) {
            LOGGER.info("[TEST]: " + tcCtx.getTestCase().getFilePath() + "/" + cUnit.getName());

            testFileCtxs = tcCtx.getTestFiles(cUnit);
            expectedResultFileCtxs = tcCtx.getExpectedResultFiles(cUnit);

            for (TestFileContext ctx : testFileCtxs) {
                testFile = ctx.getFile();
                statement = readTestFile(testFile);
                try {
                    switch (ctx.getType()) {
                        case "ddl":
                            executeDDL(statement);
                            break;
                        case "update":
                            executeUpdate(statement);
                            break;
                        case "query":
                            result = executeQuery(statement);
                            if (!cUnit.getExpectedError().isEmpty()) {
                                if (!result.has("error")) {
                                    throw new Exception("Test \"" + testFile + "\" FAILED!");
                                }
                            } else {
                                expectedResultFile = expectedResultFileCtxs.get(queryCount).getFile();

                                File actualFile = new File(PATH_ACTUAL + File.separator
                                        + tcCtx.getTestCase().getFilePath().replace(File.separator, "_") + "_"
                                        + cUnit.getName() + ".adm");

                                File actualResultFile = tcCtx.getActualResultFile(cUnit, new File(PATH_ACTUAL));
                                actualResultFile.getParentFile().mkdirs();

                                TestsUtils.writeResultsToFile(actualFile, result);

                                TestsUtils.runScriptAndCompareWithResult(testFile, new PrintWriter(System.err),
                                        expectedResultFile, actualFile);
                            }
                            queryCount++;
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
