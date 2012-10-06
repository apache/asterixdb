package edu.uci.ics.asterix.testframework.context;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import edu.uci.ics.asterix.testframework.xml.TestCase;
import edu.uci.ics.asterix.testframework.xml.TestCase.CompilationUnit;
import edu.uci.ics.asterix.testframework.xml.TestGroup;
import edu.uci.ics.asterix.testframework.xml.TestSuite;
import edu.uci.ics.asterix.testframework.xml.TestSuiteParser;

public class TestCaseContext {
    public static final String DEFAULT_TESTSUITE_XML_NAME = "testsuite.xml";

    private File tsRoot;

    private TestSuite testSuite;

    private TestGroup[] testGroups;

    private TestCase testCase;

    public TestCaseContext(File tsRoot, TestSuite testSuite, TestGroup[] testGroups, TestCase testCase) {
        this.tsRoot = tsRoot;
        this.testSuite = testSuite;
        this.testGroups = testGroups;
        this.testCase = testCase;
    }

    public File getTsRoot() {
        return tsRoot;
    }

    public TestSuite getTestSuite() {
        return testSuite;
    }

    public TestGroup[] getTestGroups() {
        return testGroups;
    }

    public TestCase getTestCase() {
        return testCase;
    }

    public File getTestFile(CompilationUnit cUnit) {
        File path = tsRoot;
        path = new File(path, testSuite.getQueryOffsetPath());
        path = new File(path, testCase.getFilePath());
        return new File(path, cUnit.getName() + testSuite.getQueryFileExtension());
    }

    public File getExpectedResultFile(CompilationUnit cUnit) {
        File path = tsRoot;
        path = new File(path, testSuite.getResultOffsetPath());
        path = new File(path, testCase.getFilePath());
        return new File(path, cUnit.getOutputFile().getValue());
    }
    
    public File getActualResultFile(CompilationUnit cUnit, File actualResultsBase) {
        File path = actualResultsBase;
        path = new File(path, testSuite.getResultOffsetPath());
        path = new File(path, testCase.getFilePath());
        return new File(path, cUnit.getOutputFile().getValue());
    }

    public static class Builder {
        public Builder() {
        }

        public List<TestCaseContext> build(File tsRoot) throws Exception {
            return build(tsRoot, DEFAULT_TESTSUITE_XML_NAME);
        }

        public List<TestCaseContext> build(File tsRoot, String tsXMLFilePath) throws Exception {
            File tsFile = new File(tsRoot, tsXMLFilePath);
            TestSuiteParser tsp = new TestSuiteParser();
            TestSuite ts = tsp.parse(tsFile);
            List<TestCaseContext> tccs = new ArrayList<TestCaseContext>();
            List<TestGroup> tgPath = new ArrayList<TestGroup>();
            addContexts(tsRoot, ts, tgPath, ts.getTestGroup(), tccs);
            return tccs;
        }

        private void addContexts(File tsRoot, TestSuite ts, List<TestGroup> tgPath, List<TestGroup> testGroups,
                List<TestCaseContext> tccs) {
            for (TestGroup tg : testGroups) {
                tgPath.add(tg);
                addContexts(tsRoot, ts, tgPath, tccs);
                tgPath.remove(tgPath.size() - 1);
            }
        }

        private void addContexts(File tsRoot, TestSuite ts, List<TestGroup> tgPath, List<TestCaseContext> tccs) {
            TestGroup tg = tgPath.get(tgPath.size() - 1);
            for (TestCase tc : tg.getTestCase()) {
                tccs.add(new TestCaseContext(tsRoot, ts, tgPath.toArray(new TestGroup[tgPath.size()]), tc));
            }
            addContexts(tsRoot, ts, tgPath, tg.getTestGroup(), tccs);
        }
    }
}