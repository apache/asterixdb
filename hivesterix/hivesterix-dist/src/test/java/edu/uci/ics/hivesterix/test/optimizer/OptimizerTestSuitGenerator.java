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
package edu.uci.ics.hivesterix.test.optimizer;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.List;

import junit.framework.Test;
import junit.framework.TestResult;
import edu.uci.ics.hivesterix.test.base.AbstractTestSuiteClass;

public class OptimizerTestSuitGenerator extends AbstractTestSuiteClass {
    private static final String PATH_TO_QUERIES = "src/test/resources/optimizerts/queries/";
    private static final String PATH_TO_RESULTS = "src/test/resources/optimizerts/results/";
    private static final String PATH_TO_IGNORES = "src/test/resources/optimizerts/ignore.txt";

    private static final String FILE_EXTENSION_OF_RESULTS = "plan";

    public static Test suite() throws UnsupportedEncodingException, FileNotFoundException, IOException {
        List<String> ignores = getIgnoreList(PATH_TO_IGNORES);
        File testData = new File(PATH_TO_QUERIES);
        File[] queries = testData.listFiles();
        OptimizerTestSuitGenerator testSuite = new OptimizerTestSuitGenerator();
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
                String resultFileName = aqlExtToResExt(qFile.getName());
                File rFile = new File(PATH_TO_RESULTS + resultFileName);
                testSuite.addTest(new OptimizerTestSuiteCaseGenerator(qFile, rFile));
            }
        }
        return testSuite;
    }

    private static String aqlExtToResExt(String fname) {
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
