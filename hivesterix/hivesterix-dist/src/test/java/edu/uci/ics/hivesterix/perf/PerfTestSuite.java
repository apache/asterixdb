package edu.uci.ics.hivesterix.perf;

import java.io.File;
import java.util.List;

import junit.framework.Test;
import junit.framework.TestResult;
import edu.uci.ics.hivesterix.perf.base.AbstractPerfTestSuiteClass;

public class PerfTestSuite extends AbstractPerfTestSuiteClass {

    private static final String PATH_TO_QUERIES = "src/test/resources/perf/queries/";
    private static final String PATH_TO_RESULTS = "src/test/resources/perf/results/";
    private static final String PATH_TO_IGNORES = "src/test/resources/perf/ignore.txt";

    private static final String FILE_EXTENSION_OF_RESULTS = "result";

    public static Test suite() throws Exception {
        List<String> ignores = getIgnoreList(PATH_TO_IGNORES);
        File testData = new File(PATH_TO_QUERIES);
        File[] queries = testData.listFiles();
        PerfTestSuite testSuite = new PerfTestSuite();

        // set hdfs and hyracks cluster, and load test data to hdfs
        try {
            testSuite.setup();
            testSuite.loadData();
        } catch (Exception e) {
            e.printStackTrace();
            throw new IllegalStateException(e.getMessage());
        }

        for (File qFile : queries) {
            if (isIgnored(qFile.getName(), ignores))
                continue;

            if (qFile.isFile()) {
                String resultFileName = hiveExtToResExt(qFile.getName());
                File rFile = new File(PATH_TO_RESULTS + resultFileName);
                testSuite.addTest(new PerfTestCase(qFile, rFile));
            }
        }
        return testSuite;
    }

    private static String hiveExtToResExt(String fname) {
        int dot = fname.lastIndexOf('.');
        return fname.substring(0, dot + 1) + FILE_EXTENSION_OF_RESULTS;
    }

    /**
     * Runs the tests and collects their result in a TestResult.
     */
    @Override
    public void run(TestResult result) {

        int testCount = countTestCases();
        for (int i = 0; i < testCount; i++) {
            Test each = this.testAt(i);
            if (result.shouldStop())
                break;
            runTest(each, result);
        }

        // cleanup hdfs and hyracks cluster
        try {
            cleanup();
        } catch (Exception e) {
            e.printStackTrace();
            throw new IllegalStateException(e.getMessage());
        }
    }

}
