package edu.uci.ics.asterix.test.runtime;

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
import org.apache.commons.io.FileUtils;
import org.json.JSONObject;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import edu.uci.ics.asterix.api.common.AsterixHyracksIntegrationUtil;
import edu.uci.ics.asterix.common.config.GlobalConfig;
import edu.uci.ics.asterix.external.dataset.adapter.FileSystemBasedAdapter;
import edu.uci.ics.asterix.external.util.IdentitiyResolverFactory;
import edu.uci.ics.asterix.test.aql.TestsUtils;
import edu.uci.ics.asterix.testframework.context.TestCaseContext;
import edu.uci.ics.asterix.testframework.context.TestFileContext;
import edu.uci.ics.asterix.testframework.xml.TestCase.CompilationUnit;

/**
 * Runs the runtime test cases under 'asterix-app/src/test/resources/runtimets'.
 */
@RunWith(Parameterized.class)
public class ExecutionTest {
    private static final String PATH_ACTUAL = "rttest/";
    private static final String PATH_BASE = "src/test/resources/runtimets/";

    private static final String TEST_CONFIG_FILE_NAME = "test.properties";
    private static final String[] ASTERIX_DATA_DIRS = new String[] { "nc1data", "nc2data" };

    private static final Logger LOGGER = Logger.getLogger(ExecutionTest.class.getName());

    // private static NCBootstrapImpl _bootstrap = new NCBootstrapImpl();

    @BeforeClass
    public static void setUp() throws Exception {
        System.setProperty(GlobalConfig.CONFIG_FILE_PROPERTY, TEST_CONFIG_FILE_NAME);
        System.setProperty(GlobalConfig.WEB_SERVER_PORT_PROPERTY, "19002");
        File outdir = new File(PATH_ACTUAL);
        outdir.mkdirs();

        File log = new File("asterix_logs");
        if (log.exists()) {
            FileUtils.deleteDirectory(log);
        }

        AsterixHyracksIntegrationUtil.init();

        // TODO: Uncomment when hadoop version is upgraded and adapters are
        // ported. 
        HDFSCluster.getInstance().setup();

        // Set the node resolver to be the identity resolver that expects node names 
        // to be node controller ids; a valid assumption in test environment. 
        System.setProperty(FileSystemBasedAdapter.NODE_RESOLVER_FACTORY_PROPERTY,
                IdentitiyResolverFactory.class.getName());
    }

    @AfterClass
    public static void tearDown() throws Exception {
        AsterixHyracksIntegrationUtil.deinit();
        File outdir = new File(PATH_ACTUAL);
        File[] files = outdir.listFiles();
        if (files == null || files.length == 0) {
            outdir.delete();
        }
        // clean up the files written by the ASTERIX storage manager
        for (String d : ASTERIX_DATA_DIRS) {
            TestsUtils.deleteRec(new File(d));
        }

        File log = new File("asterix_logs");
        if (log.exists()) {
            FileUtils.deleteDirectory(log);
        }
        HDFSCluster.getInstance().cleanup();
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

    private TestCaseContext tcCtx;

    public ExecutionTest(TestCaseContext tcCtx) {
        this.tcCtx = tcCtx;
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

//            if (!(tcCtx.getTestCase().getFilePath().contains("dml") && cUnit.getName().equals(
//                    "delete-from-loaded-dataset-with-index"))) {
//                continue;
//            }
//
//            System.out.println("/Test/: " + tcCtx.getTestCase().getFilePath() + "/" + cUnit.getName());

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
                                TestsUtils
                                        .runScriptAndCompareWithResult(
                                                AsterixHyracksIntegrationUtil.getHyracksClientConnection(), testFile,
                                                new PrintWriter(System.err), expectedResultFile,
                                                result.getJSONArray("results"));
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
