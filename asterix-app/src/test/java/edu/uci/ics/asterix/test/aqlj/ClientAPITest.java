package edu.uci.ics.asterix.test.aqlj;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Collection;
import java.util.logging.Logger;

import org.junit.AfterClass;
import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.internal.AssumptionViolatedException;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import edu.uci.ics.asterix.api.common.AsterixHyracksIntegrationUtil;
import edu.uci.ics.asterix.common.config.GlobalConfig;
import edu.uci.ics.asterix.hyracks.bootstrap.CCBootstrapImpl;
import edu.uci.ics.asterix.test.aql.TestsUtils;
import edu.uci.ics.asterix.test.common.TestHelper;

@RunWith(Parameterized.class)
public class ClientAPITest {
    private static final PrintWriter ERR = new PrintWriter(System.err);
    private static final String EXTENSION_QUERY = "aql";
    private static final String FILENAME_IGNORE = "ignore.txt";
    private static final String FILENAME_ONLY = "only.txt";
    private static final ArrayList<String> ignore = readFile(FILENAME_IGNORE);
    private static final ArrayList<String> only = readFile(FILENAME_ONLY);
    private static final String PATH_ACTUAL = "aqljtest/";
    private static final String PATH_BASE = "src/test/resources/aqljts/";
    private static final String PATH_EXPECTED = PATH_BASE + "results/";
    private static final String PATH_QUERIES = PATH_BASE + "queries/";
    private static final String SEPARATOR = System.getProperty("file.separator");

    private static final String TEST_CONFIG_FILE_NAME = "test.properties";
    private static final String[] ASTERIX_DATA_DIRS = new String[] { "nc1data", "nc2data" };

    private static final Logger LOGGER = Logger.getLogger(ClientAPITest.class.getName());

    private static ArrayList<String> readFile(String fileName) {
        ArrayList<String> list = new ArrayList<String>();
        BufferedReader result;
        try {
            result = new BufferedReader(new FileReader(PATH_BASE + fileName));
            while (true) {
                String line = result.readLine();
                if (line == null) {
                    break;
                } else {
                    String s = line.trim();
                    if (s.length() > 0) {
                        list.add(s);
                    }
                }
            }
            result.close();
        } catch (FileNotFoundException e) {
        } catch (IOException e) {
        }
        return list;
    }

    @BeforeClass
    public static void setUp() throws Exception {
        System.setProperty(GlobalConfig.CONFIG_FILE_PROPERTY, TEST_CONFIG_FILE_NAME);
        System.setProperty(GlobalConfig.WEB_SERVER_PORT_PROPERTY, "19002");
        File outdir = new File(PATH_ACTUAL);
        outdir.mkdirs();
        AsterixHyracksIntegrationUtil.init();
        // _bootstrap.start();
    }

    @AfterClass
    public static void tearDown() throws Exception {
        // _bootstrap.stop();
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
    }

    private static void suiteBuild(File dir, Collection<Object[]> testArgs, String path) {
        for (File file : dir.listFiles()) {
            if (file.isDirectory() && !file.getName().startsWith(".")) {
                suiteBuild(file, testArgs, path + file.getName() + SEPARATOR);
            }
            if (file.isFile() && file.getName().endsWith(EXTENSION_QUERY)
            // && !ignore.contains(path + file.getName())
            ) {
                String resultFileName = TestsUtils.aqlExtToResExt(file.getName());
                File expectedFile = new File(PATH_EXPECTED + path + resultFileName);
                File actualFile = new File(PATH_ACTUAL + SEPARATOR + path.replace(SEPARATOR, "_") + resultFileName);
                testArgs.add(new Object[] { file, expectedFile, actualFile });
            }
        }
    }

    @Parameters
    public static Collection<Object[]> tests() {
        Collection<Object[]> testArgs = new ArrayList<Object[]>();
        suiteBuild(new File(PATH_QUERIES), testArgs, "");
        return testArgs;
    }

    private File actualFile;
    private File expectedFile;
    private File queryFile;

    public ClientAPITest(File queryFile, File expectedFile, File actualFile) {
        this.queryFile = queryFile;
        this.expectedFile = expectedFile;
        this.actualFile = actualFile;
    }

    @Test
    public void test() throws Exception {
        try {
            String queryFileShort = queryFile.getPath().substring(PATH_QUERIES.length())
                    .replace(SEPARATOR.charAt(0), '/');

            if (!only.isEmpty()) {
                Assume.assumeTrue(TestHelper.isInPrefixList(only, queryFileShort));
            }
            Assume.assumeTrue(!TestHelper.isInPrefixList(ignore, queryFileShort));
            LOGGER.severe("RUNNING TEST: " + queryFile + " \n");
            TestsUtils.runScriptAndCompareWithResultViaClientAPI(queryFile, ERR, expectedFile, actualFile,
                    CCBootstrapImpl.DEFAULT_API_SERVER_PORT);
        } catch (Exception e) {
            if (!(e instanceof AssumptionViolatedException)) {
                LOGGER.severe("Test \"" + queryFile.getPath() + "\" FAILED!");
                throw new Exception("Test \"" + queryFile.getPath() + "\" FAILED!", e);
            } else {
                throw e;
            }
        }
    }
}
