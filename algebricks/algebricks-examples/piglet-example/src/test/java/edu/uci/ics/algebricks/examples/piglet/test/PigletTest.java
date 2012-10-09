package edu.uci.ics.algebricks.examples.piglet.test;

import java.io.File;

import junit.framework.Test;
import junit.framework.TestSuite;

public class PigletTest {
    public static Test suite() {
        TestSuite suite = new TestSuite();
        File dir = new File("testcases");
        findAndAddTests(suite, dir);

        return suite;
    }

    private static void findAndAddTests(TestSuite suite, File dir) {
        for (final File f : dir.listFiles()) {
            if (f.getName().startsWith(".")) {
                continue;
            }
            if (f.isDirectory()) {
                findAndAddTests(suite, f);
            } else if (f.getName().endsWith(".piglet")) {
                suite.addTest(new PigletTestCase(f));
            }
        }
    }
}
