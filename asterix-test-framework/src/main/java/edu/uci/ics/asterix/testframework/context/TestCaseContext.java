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
package edu.uci.ics.asterix.testframework.context;

import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
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

    public List<TestFileContext> getTestFiles(CompilationUnit cUnit) {
        List<TestFileContext> testFileCtxs = new ArrayList<TestFileContext>();

        File path = tsRoot;
        path = new File(path, testSuite.getQueryOffsetPath());
        path = new File(path, testCase.getFilePath());
        path = new File(path, cUnit.getName());

        String fileNames[] = path.list();
        for (String fName : fileNames) {
            if (fName.startsWith(".")) {
                continue;
            }
            
            File testFile = new File(path, fName);
            TestFileContext tfsc = new TestFileContext(testFile);
            String[] nameSplits = fName.split("\\.");
            if (nameSplits.length < 3) {
                throw new IllegalArgumentException("Test file '" + cUnit.getName() + File.separatorChar
                        + fName + "' does not have the proper test file name format.");
            }
            tfsc.setSeqNum(nameSplits[nameSplits.length - 3]);
            tfsc.setType(nameSplits[nameSplits.length - 2]);
            testFileCtxs.add(tfsc);
        }
        Collections.sort(testFileCtxs);
        return testFileCtxs;
    }

    public List<TestFileContext> getExpectedResultFiles(CompilationUnit cUnit) {
        List<TestFileContext> resultFileCtxs = new ArrayList<TestFileContext>();

        File path = tsRoot;
        path = new File(path, testSuite.getResultOffsetPath());
        path = new File(path, testCase.getFilePath());
        path = new File(path, cUnit.getOutputDir().getValue());

        String fileNames[] = path.list();

        if (fileNames != null) {
            for (String fName : fileNames) {
                if (fName.startsWith(".")) {
                    continue;
                }
                
                File testFile = new File(path, fName);
                TestFileContext tfsc = new TestFileContext(testFile);
                String[] nameSplits = fName.split("\\.");
                
                if (nameSplits.length < 3) {
                    throw new IllegalArgumentException("Test file '" + cUnit.getName() + File.separatorChar
                            + fName + "' does not have the proper test file name format.");
                }
                
                tfsc.setSeqNum(nameSplits[nameSplits.length - 2]);
                resultFileCtxs.add(tfsc);
            }
            Collections.sort(resultFileCtxs);
        }
        return resultFileCtxs;
    }

    public File getActualResultFile(CompilationUnit cUnit, File actualResultsBase) {
        File path = actualResultsBase;
        path = new File(path, testSuite.getResultOffsetPath());
        path = new File(path, testCase.getFilePath());
        return new File(path, cUnit.getOutputDir().getValue() + ".adm");
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