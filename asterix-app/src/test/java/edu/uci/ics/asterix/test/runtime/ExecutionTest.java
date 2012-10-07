package edu.uci.ics.asterix.test.runtime;

import java.io.File;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.logging.Logger;

import org.apache.commons.io.FileUtils;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import edu.uci.ics.asterix.api.common.AsterixHyracksIntegrationUtil;
import edu.uci.ics.asterix.common.config.GlobalConfig;
import edu.uci.ics.asterix.test.aql.TestsUtils;
import edu.uci.ics.asterix.testframework.context.TestCaseContext;
import edu.uci.ics.asterix.testframework.xml.TestCase.CompilationUnit;

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
        if (log.exists())
            FileUtils.deleteDirectory(log);
        File lsn = new File("last_checkpoint_lsn");
        lsn.deleteOnExit();

        AsterixHyracksIntegrationUtil.init();

        // TODO: Uncomment when hadoop version is upgraded and adapters are ported
        //HDFSCluster.getInstance().setup();
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
        if (log.exists())
            FileUtils.deleteDirectory(log);
        File lsn = new File("last_checkpoint_lsn");
        lsn.deleteOnExit();
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

    @Test
    public void test() throws Exception {
        if (System.getProperty("run_new_tests") != null) {
            List<CompilationUnit> cUnits = tcCtx.getTestCase().getCompilationUnit();
            for (CompilationUnit cUnit : cUnits) {
                File testFile = tcCtx.getTestFile(cUnit);
                File expectedResultFile = tcCtx.getExpectedResultFile(cUnit);
                File actualFile = new File(PATH_ACTUAL + File.separator
                        + tcCtx.getTestCase().getFilePath().replace(File.separator, "_") + "_" + cUnit.getName()
                        + ".adm");

                File actualResultFile = tcCtx.getActualResultFile(cUnit, new File(PATH_ACTUAL));
                actualResultFile.getParentFile().mkdirs();
                try {
                    TestsUtils.runScriptAndCompareWithResult(
                            AsterixHyracksIntegrationUtil.getHyracksClientConnection(), testFile, new PrintWriter(
                                    System.err), expectedResultFile, actualFile);
                } catch (Exception e) {
                    LOGGER.severe("Test \"" + testFile + "\" FAILED!");
                    e.printStackTrace();
                    if (cUnit.getExpectedError().isEmpty()) {
                        throw new Exception("Test \"" + testFile + "\" FAILED!", e);
                    }
                }
            }
        }
    }

}
