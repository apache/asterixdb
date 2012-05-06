package edu.uci.ics.asterix.test.metadata;

import static org.junit.Assert.fail;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.io.Reader;
import java.util.ArrayList;
import java.util.Collection;
import java.util.logging.Logger;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import edu.uci.ics.asterix.api.common.AsterixHyracksIntegrationUtil;
import edu.uci.ics.asterix.api.java.AsterixJavaClient;
import edu.uci.ics.asterix.common.config.GlobalConfig;
import edu.uci.ics.asterix.test.aql.TestsUtils;

@RunWith(Parameterized.class)
public class MetadataTransactionsTest {

    private static final Logger LOGGER = Logger.getLogger(MetadataTransactionsTest.class.getName());

    private static final PrintWriter ERR = new PrintWriter(System.err);
    private static final String EXTENSION_QUERY = "aql";
    private static final String EXTENSION_RESULT = "adm";
    private static final String PATH_ACTUAL = "rttest/";
    private static final String PATH_BASE = "src/test/resources/metadata-transactions/";
    private static final String CHECK_STATE_QUERIES_PATH = PATH_BASE + "check-state-queries/";
    private static final String CHECK_STATE_RESULTS_PATH = PATH_BASE + "check-state-results/";
    private static final String CHECK_STATE_FILE = PATH_BASE + "check-state-queries.txt";
    private static final String INIT_STATE_QUERIES_PATH = PATH_BASE + "init-state-queries/";
    private static final String INIT_STATE_FILE = PATH_BASE + "init-state-queries.txt";
    private static final String TEST_QUERIES_PATH = PATH_BASE + "queries/";
    private static final String QUERIES_FILE = PATH_BASE + "queries.txt";

    private static String _oldConfigFileName;
    private static final String TEST_CONFIG_FILE_NAME = "asterix-metadata.properties";
    private static final String[] ASTERIX_DATA_DIRS = new String[] { "nc1data", "nc2data" };

    private static String aqlExtToResExt(String fname) {
        int dot = fname.lastIndexOf('.');
        return fname.substring(0, dot + 1) + EXTENSION_RESULT;
    }

    private static void executeQueryTuple(Object[] queryTuple, boolean expectFailure, boolean executeQuery) {
        String queryFileName = (String) queryTuple[0];
        String expectedFileName = (String) queryTuple[1];
        String actualFileName = (String) queryTuple[2];
        try {
            Reader query = new BufferedReader(new InputStreamReader(new FileInputStream(queryFileName), "UTF-8"));
            AsterixJavaClient asterix = new AsterixJavaClient(
                    AsterixHyracksIntegrationUtil.getHyracksClientConnection(), query, ERR);
            LOGGER.info("Query is: " + queryFileName);
            try {
                asterix.compile(true, false, false, false, false, executeQuery, false);
            } finally {
                query.close();
            }
            // We don't want to execute a query if we expect only DDL
            // modifications.
            if (executeQuery) {
                asterix.execute();
            }
            query.close();
        } catch (Exception e) {
            if (!expectFailure) {
                fail("Unexpected failure of AQL query in file: " + queryFileName + "\n" + e.getMessage());
            }
            return;
        }
        // Do we expect failure?
        if (expectFailure) {
            fail("Unexpected success of AQL query in file: " + queryFileName);
        }
        // If no expected or actual file names were given, then we don't want to
        // compare them.
        if (expectedFileName == null || actualFileName == null) {
            return;
        }
        // Compare actual and expected results.
        try {
            File actualFile = new File(actualFileName);
            File expectedFile = new File(expectedFileName);
            if (actualFile.exists()) {
                BufferedReader readerExpected = new BufferedReader(new InputStreamReader(new FileInputStream(
                        expectedFile), "UTF-8"));
                BufferedReader readerActual = new BufferedReader(new InputStreamReader(new FileInputStream(actualFile),
                        "UTF-8"));
                String lineExpected, lineActual;
                int num = 1;
                try {
                    while ((lineExpected = readerExpected.readLine()) != null) {
                        lineActual = readerActual.readLine();
                        if (lineActual == null) {
                            fail("Result for " + queryFileName + " changed at line " + num + ":\n< " + lineExpected
                                    + "\n> ");
                        }
                        if (!lineExpected.split("Timestamp")[0].equals(lineActual.split("Timestamp")[0])) {
                            fail("Result for " + queryFileName + " changed at line " + num + ":\n< " + lineExpected
                                    + "\n> " + lineActual);
                        }
                        ++num;
                    }
                    lineActual = readerActual.readLine();
                    if (lineActual != null) {
                        fail("Result for " + queryFileName + " changed at line " + num + ":\n< \n> " + lineActual);
                    }
                    // actualFile.delete();
                } finally {
                    readerExpected.close();
                    readerActual.close();
                }
            }
        } catch (Exception e) {
            fail("Execption occurred while comparing expected and actual results: " + e.getMessage());
        }
    }

    // Collection of object arrays. Each object array contains exactly 3 string
    // elements:
    // 1. String QueryFile
    // 2. String expectedFile
    // 3. String actualFile
    private static Collection<Object[]> checkQuerySuite = new ArrayList<Object[]>();

    private static void checkMetadataState() {
        for (Object[] checkTuple : checkQuerySuite) {
            executeQueryTuple(checkTuple, false, true);
        }
    }

    @BeforeClass
    public static void setUp() throws Exception {
        _oldConfigFileName = System.getProperty(GlobalConfig.CONFIG_FILE_PROPERTY);
        System.setProperty(GlobalConfig.CONFIG_FILE_PROPERTY, TEST_CONFIG_FILE_NAME);
        File outdir = new File(PATH_ACTUAL);
        outdir.mkdirs();
        AsterixHyracksIntegrationUtil.init();

        // Create initial metadata state by adding the customers and orders
        // metadata.
        Collection<Object[]> initQuerySuite = new ArrayList<Object[]>();
        prepareQuerySuite(INIT_STATE_FILE, INIT_STATE_QUERIES_PATH, null, null, initQuerySuite);
        for (Object[] queryTuple : initQuerySuite) {
            executeQueryTuple(queryTuple, false, false);
        }

        // Prepare the query suite for checking the metadata state is still
        // correct.
        prepareQuerySuite(CHECK_STATE_FILE, CHECK_STATE_QUERIES_PATH, CHECK_STATE_RESULTS_PATH, PATH_ACTUAL,
                checkQuerySuite);

        // Make sure the initial metadata state is set up correctly.
        checkMetadataState();
    }

    @AfterClass
    public static void tearDown() throws Exception {
        // _bootstrap.stop();
        AsterixHyracksIntegrationUtil.deinit();
        File outdir = new File(PATH_ACTUAL);
        File[] files = outdir.listFiles();
        if (files == null || files.length == 0) {
            // outdir.delete();
        }
        if (_oldConfigFileName != null) {
            System.setProperty(GlobalConfig.CONFIG_FILE_PROPERTY, _oldConfigFileName);
        }
        // clean up the files written by the ASTERIX storage manager
        for (String d : ASTERIX_DATA_DIRS) {
            TestsUtils.deleteRec(new File(d));
        }
    }

    private static void prepareQuerySuite(String queryListPath, String queryPath, String expectedPath,
            String actualPath, Collection<Object[]> output) throws IOException {
        BufferedReader br = null;
        try {
            File queryListFile = new File(queryListPath);
            br = new BufferedReader(new InputStreamReader(new FileInputStream(queryListFile), "UTF-8"));
            String strLine;
            String queryFileName;
            File queryFile;
            while ((strLine = br.readLine()) != null) {
                // Ignore commented test files.
                if (strLine.startsWith("//")) {
                    continue;
                }
                queryFileName = queryPath + strLine;
                queryFile = new File(queryPath + strLine);
                // If no expected or actual path was given, just add the
                // queryFile.
                if (expectedPath == null || actualPath == null) {
                    output.add(new Object[] { queryFileName, null, null });
                    continue;
                }
                // We want to compare expected and actual results. Construct the
                // expected and actual files.
                if (queryFile.getName().endsWith(EXTENSION_QUERY)) {
                    String resultFileName = aqlExtToResExt(queryFile.getName());
                    String expectedFileName = expectedPath + resultFileName;
                    String actualFileName = actualPath + resultFileName;
                    output.add(new Object[] { queryFileName, expectedFileName, actualFileName });
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (br != null) {
                br.close();
            }
        }
    }

    @Parameters
    public static Collection<Object[]> tests() throws IOException {
        Collection<Object[]> testArgs = new ArrayList<Object[]>();
        prepareQuerySuite(QUERIES_FILE, TEST_QUERIES_PATH, null, null, testArgs);
        return testArgs;
    }

    private String actualFileName;
    private String expectedFileName;
    private String queryFileName;

    public MetadataTransactionsTest(String queryFileName, String expectedFileName, String actualFileName) {
        this.queryFileName = queryFileName;
        this.expectedFileName = expectedFileName;
        this.actualFileName = actualFileName;
    }

    @Test
    public void test() throws Exception {
        // Re-create query tuple.
        Object[] queryTuple = new Object[] { queryFileName, expectedFileName, actualFileName };

        // Execute query tuple, expecting failure.
        executeQueryTuple(queryTuple, true, false);

        // Validate metadata state after failed query above.
        checkMetadataState();
    }
}
