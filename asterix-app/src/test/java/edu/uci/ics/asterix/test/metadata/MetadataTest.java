package edu.uci.ics.asterix.test.metadata;

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
import edu.uci.ics.asterix.common.exceptions.AsterixException;
import edu.uci.ics.asterix.test.aql.TestsUtils;
import edu.uci.ics.asterix.test.runtime.RuntimeTest;

@RunWith(Parameterized.class)
public class MetadataTest {

    private static final Logger LOGGER = Logger.getLogger(RuntimeTest.class.getName());

    private static final PrintWriter ERR = new PrintWriter(System.err);
    private static final String EXTENSION_QUERY = "aql";
    private static final String EXTENSION_RESULT = "adm";
    private static final String PATH_ACTUAL = "rttest/";
    private static final String PATH_BASE = "src/test/resources/metadata/";
    private static final String PATH_EXPECTED = PATH_BASE + "results/";
    private static final String PATH_QUERIES = PATH_BASE + "queries/";
    private static final String QUERIES_FILE = PATH_BASE + "queries.txt";
    private static final String SEPARATOR = System.getProperty("file.separator");

    private static String _oldConfigFileName;
    //private static final String TEST_CONFIG_FILE_NAME = "asterix-metadata.properties";
    private static final String TEST_CONFIG_FILE_NAME = "test.properties";
    private static final String[] ASTERIX_DATA_DIRS = new String[] { "nc1data", "nc2data" };

    private static String aqlExtToResExt(String fname) {
        int dot = fname.lastIndexOf('.');
        return fname.substring(0, dot + 1) + EXTENSION_RESULT;
    }

    @BeforeClass
    public static void setUp() throws Exception {
        _oldConfigFileName = System.getProperty(GlobalConfig.CONFIG_FILE_PROPERTY);
        System.setProperty(GlobalConfig.CONFIG_FILE_PROPERTY, TEST_CONFIG_FILE_NAME);
        File outdir = new File(PATH_ACTUAL);
        outdir.mkdirs();
        AsterixHyracksIntegrationUtil.init();
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
        if (_oldConfigFileName != null) {
            System.setProperty(GlobalConfig.CONFIG_FILE_PROPERTY, _oldConfigFileName);
        }
        // clean up the files written by the ASTERIX storage manager
        for (String d : ASTERIX_DATA_DIRS) {
            TestsUtils.deleteRec(new File(d));
        }
    }

    private static void suiteBuild(File f, Collection<Object[]> testArgs, String path) throws IOException {
        BufferedReader br = null;
        try {
            br = new BufferedReader(new InputStreamReader(new FileInputStream(f), "UTF-8"));
            String strLine;
            File file;
            while ((strLine = br.readLine()) != null) {
                // Ignore commented out lines.
                if (strLine.startsWith("//")) {
                    continue;
                }
                file = new File(PATH_QUERIES + SEPARATOR + strLine);
                if (file.getName().endsWith(EXTENSION_QUERY)) {
                    String resultFileName = aqlExtToResExt(file.getName());
                    File expectedFile = new File(PATH_EXPECTED + path + resultFileName);
                    File actualFile = new File(PATH_ACTUAL + SEPARATOR + path.replace(SEPARATOR, "_") + resultFileName);
                    testArgs.add(new Object[] { file, expectedFile, actualFile });
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
        suiteBuild(new File(QUERIES_FILE), testArgs, "");
        return testArgs;
    }

    private File actualFile;
    private File expectedFile;
    private File queryFile;

    public MetadataTest(File queryFile, File expectedFile, File actualFile) {
        this.queryFile = queryFile;
        this.expectedFile = expectedFile;
        this.actualFile = actualFile;
    }

    @Test
    public void test() throws Exception {
        Reader query = new BufferedReader(new InputStreamReader(new FileInputStream(queryFile), "UTF-8"));
        AsterixJavaClient asterix = new AsterixJavaClient(AsterixHyracksIntegrationUtil.getHyracksClientConnection(),
                query, ERR);
        try {
            LOGGER.info("Query is: " + queryFile);
            asterix.compile(true, false, false, false, false, true, false);
        } catch (AsterixException e) {
            throw new Exception("Compile ERROR for " + queryFile + ": " + e.getMessage(), e);
        } finally {
            query.close();
        }
        asterix.execute();
        query.close();

        if (actualFile.exists()) {
            BufferedReader readerExpected = new BufferedReader(new InputStreamReader(new FileInputStream(expectedFile),
                    "UTF-8"));
            BufferedReader readerActual = new BufferedReader(new InputStreamReader(new FileInputStream(actualFile),
                    "UTF-8"));
            String lineExpected, lineActual;
            int num = 1;
            try {
                while ((lineExpected = readerExpected.readLine()) != null) {
                    lineActual = readerActual.readLine();
                    if (lineActual == null) {
                        throw new Exception("Result for " + queryFile + " changed at line " + num + ":\n< "
                                + lineExpected + "\n> ");
                    }
                    if (!lineExpected.split("Timestamp")[0].equals(lineActual.split("Timestamp")[0])) {
                        throw new Exception("Result for " + queryFile + " changed at line " + num + ":\n< "
                                + lineExpected + "\n> " + lineActual);
                    }
                    ++num;
                }
                lineActual = readerActual.readLine();
                if (lineActual != null) {
                    throw new Exception("Result for " + queryFile + " changed at line " + num + ":\n< \n> "
                            + lineActual);
                }
                actualFile.delete();
            } finally {
                readerExpected.close();
                readerActual.close();
            }
        }
    }
}
