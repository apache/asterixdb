package edu.uci.ics.asterix.test.aql;

import static org.junit.Assert.fail;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.logging.Logger;

import org.apache.commons.httpclient.DefaultHttpMethodRetryHandler;
import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.HttpStatus;
import org.apache.commons.httpclient.NameValuePair;
import org.apache.commons.httpclient.methods.GetMethod;
import org.apache.commons.httpclient.params.HttpMethodParams;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import edu.uci.ics.asterix.testframework.context.TestCaseContext;
import edu.uci.ics.asterix.testframework.context.TestFileContext;
import edu.uci.ics.asterix.testframework.xml.TestCase.CompilationUnit;
import edu.uci.ics.hyracks.algebricks.common.exceptions.NotImplementedException;

public class TestsUtils {

    private static final String EXTENSION_AQL_RESULT = "adm";
    private static final Logger LOGGER = Logger.getLogger(TestsUtils.class.getName());

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

    public static void writeResultsToFile(File actualFile, JSONObject result) throws IOException, JSONException {
        BufferedWriter writer = new BufferedWriter(new FileWriter(actualFile));
        Results res = new Results(result);
        for (String line : res) {
            writer.write(line);
            writer.newLine();
        }
        writer.close();
    }

    public static class Results implements Iterable<String> {
        private final JSONArray chunks;

        public Results(JSONObject result) throws JSONException {
            chunks = result.getJSONArray("results");
        }

        public Iterator<String> iterator() {
            return new ResultIterator(chunks);
        }
    }

    public static class ResultIterator implements Iterator<String> {
        private final JSONArray chunks;

        private int chunkCounter = 0;
        private int recordCounter = 0;

        public ResultIterator(JSONArray chunks) {
            this.chunks = chunks;
        }

        @Override
        public boolean hasNext() {
            JSONArray resultArray;
            try {
                resultArray = chunks.getJSONArray(chunkCounter);
                if (resultArray.getString(recordCounter) != null) {
                    return true;
                }
            } catch (JSONException e) {
                return false;
            }
            return false;
        }

        @Override
        public String next() throws NoSuchElementException {
            JSONArray resultArray;
            String item = "";

            try {
                resultArray = chunks.getJSONArray(chunkCounter);
                item = resultArray.getString(recordCounter);
                if (item == null) {
                    throw new NoSuchElementException();
                }
                item = item.trim();

                recordCounter++;
                if (recordCounter >= resultArray.length()) {
                    chunkCounter++;
                    recordCounter = 0;
                }
            } catch (JSONException e) {
                throw new NoSuchElementException(e.getMessage());
            }
            return item;
        }

        @Override
        public void remove() {
            throw new NotImplementedException();
        }
    }

    // Executes Query and returns results as JSONArray
    public static JSONObject executeQuery(String str) throws Exception {

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

    // To execute Update statements
    // Insert and Delete statements are executed here
    public static void executeUpdate(String str) throws Exception {
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

    // To execute DDL and Update statements
    // create type statement
    // create dataset statement
    // create index statement
    // create dataverse statement
    // create function statement
    public static void executeDDL(String str) throws Exception {
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

    public static void executeTest(String actualPath, TestCaseContext testCaseCtx)throws Exception {

        File testFile;
        File expectedResultFile;
        String statement;
        List<TestFileContext> expectedResultFileCtxs;
        List<TestFileContext> testFileCtxs;

        int queryCount = 0;
        JSONObject result;

        List<CompilationUnit> cUnits = testCaseCtx.getTestCase().getCompilationUnit();
        for (CompilationUnit cUnit : cUnits) {
            LOGGER.info("[TEST]: " + testCaseCtx.getTestCase().getFilePath() + "/" + cUnit.getName());

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
                            result = TestsUtils.executeQuery(statement);
                            if (!cUnit.getExpectedError().isEmpty()) {
                                if (!result.has("error")) {
                                    throw new Exception("Test \"" + testFile + "\" FAILED!");
                                }
                            } else {
                                expectedResultFile = expectedResultFileCtxs.get(queryCount).getFile();

                                File actualFile = new File(actualPath + File.separator
                                        + testCaseCtx.getTestCase().getFilePath().replace(File.separator, "_") + "_"
                                        + cUnit.getName() + ".adm");

                                File actualResultFile = testCaseCtx.getActualResultFile(cUnit, new File(actualPath));
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
