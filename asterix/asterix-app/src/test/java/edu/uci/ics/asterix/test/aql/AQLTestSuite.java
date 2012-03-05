package edu.uci.ics.asterix.test.aql;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.UnsupportedEncodingException;

import junit.framework.Test;
import junit.framework.TestSuite;
import edu.uci.ics.asterix.aql.parser.ParseException;

public class AQLTestSuite extends TestSuite {
    private static String AQLTS_PATH = "src/test/resources/AQLTS/queries/";

    public static Test suite() throws ParseException, UnsupportedEncodingException, FileNotFoundException {
        File testData = new File(AQLTS_PATH);
        File[] queries = testData.listFiles();
        TestSuite testSuite = new TestSuite();
        for (File file : queries) {
            if (file.isFile()) {
                testSuite.addTest(new AQLTestCase(file));
            }
        }

        return testSuite;

    }

    public static void main(String args[]) throws Throwable {
        junit.textui.TestRunner.run(AQLTestSuite.suite());
    }
}
