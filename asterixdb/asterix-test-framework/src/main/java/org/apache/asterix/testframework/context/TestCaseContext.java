/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.asterix.testframework.context;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import org.apache.asterix.testframework.template.TemplateHelper;
import org.apache.asterix.testframework.xml.CategoryEnum;
import org.apache.asterix.testframework.xml.TestCase;
import org.apache.asterix.testframework.xml.TestCase.CompilationUnit;
import org.apache.asterix.testframework.xml.TestGroup;
import org.apache.asterix.testframework.xml.TestSuite;
import org.apache.asterix.testframework.xml.TestSuiteParser;

public class TestCaseContext {

    /**
     * For specifying the desired output formatting of results.
     */
    public enum OutputFormat {
        NONE("", ""),
        ADM("adm", "application/x-adm"),
        LOSSLESS_JSON("json", "application/json; lossless=true"),
        CLEAN_JSON("json", "application/json"),
        CSV("csv", "text/csv"),
        CSV_HEADER("csv-header", "text/csv; header=present"),
        AST("ast", "application/x-ast"),
        PLAN("plan", "application/x-plan"),
        BINARY("", "application/octet-stream");

        private final String extension;
        private final String mimetype;

        OutputFormat(String ext, String mime) {
            this.extension = ext;
            this.mimetype = mime;
        }

        public String extension() {
            return extension;
        }

        public String mimeType() {
            return mimetype;
        }

        //
        public static OutputFormat forCompilationUnit(CompilationUnit cUnit) {
            switch (cUnit.getOutputDir().getCompare()) {
                case TEXT:
                    return OutputFormat.ADM;
                case LOSSLESS_JSON:
                    return OutputFormat.LOSSLESS_JSON;
                case CLEAN_JSON:
                    return OutputFormat.CLEAN_JSON;
                case CSV:
                    return OutputFormat.CSV;
                case CSV_HEADER:
                    return OutputFormat.CSV_HEADER;
                case BINARY:
                    return OutputFormat.BINARY;
                case INSPECT:
                case IGNORE:
                    return OutputFormat.NONE;
                case AST:
                    return OutputFormat.AST;
                case PLAN:
                    return OutputFormat.PLAN;
                default:
                    assert false : "Unknown ComparisonEnum!";
                    return OutputFormat.NONE;
            }
        }
    }

    public static final String DEFAULT_TESTSUITE_XML_NAME = "testsuite.xml";
    public static final String ONLY_TESTSUITE_XML_NAME = "only.xml";
    public static final String DEFAULT_REPEATED_TESTSUITE_XML_NAME = "repeatedtestsuite.xml";

    private File tsRoot;

    private TestSuite testSuite;

    private TestGroup[] testGroups;

    private TestCase testCase;
    private Map<String, Object> kv;

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

    public int getRepeat() {
        return testCase.getRepeat().intValue();
    }

    public void put(String key, Object object) {
        if (kv == null) {
            kv = new HashMap<>();
        }
        kv.put(key, object);
    }

    @SuppressWarnings("unchecked")
    public <T> T get(String key) {
        return (T) kv.get(key);
    }

    public List<TestFileContext> getFilesInDir(String basePath, String dirName, boolean withType) {
        List<TestFileContext> testFileCtxs = new ArrayList<>();

        File path = tsRoot;
        path = new File(path, basePath);
        path = new File(path, testCase.getFilePath());
        path = new File(path, dirName);

        if (path.isDirectory()) {
            String fileNames[] = path.list();
            for (String fName : fileNames) {
                if (fName.startsWith(".")) {
                    continue;
                }

                File testFile = new File(path, fName);
                if (fName.endsWith(".template")) {
                    try {
                        testFile = TemplateHelper.INSTANCE.resolveTemplateFile(testFile);
                    } catch (IOException e) {
                        throw new IllegalArgumentException(e);
                    }
                    fName = testFile.getName();
                }
                TestFileContext tfsc = new TestFileContext(testFile);
                String[] nameSplits = fName.split("\\.");
                if (nameSplits.length < 3) {
                    throw new IllegalArgumentException("Test file '" + dirName + File.separatorChar + fName
                            + "' does not have the proper test file name format.");
                }
                if (withType) {
                    tfsc.setSeqNum(nameSplits[nameSplits.length - 3]);
                    tfsc.setType(nameSplits[nameSplits.length - 2]);
                } else {
                    tfsc.setSeqNum(nameSplits[nameSplits.length - 2]);
                }
                testFileCtxs.add(tfsc);
            }
        }
        Collections.sort(testFileCtxs);
        return testFileCtxs;
    }

    public List<TestFileContext> getTestFiles(CompilationUnit cUnit) {
        return getFilesInDir(testSuite.getQueryOffsetPath(), cUnit.getName(), true);
    }

    public List<TestFileContext> getExpectedResultFiles(CompilationUnit cUnit) {
        return getFilesInDir(testSuite.getResultOffsetPath(), cUnit.getOutputDir().getValue(), false);
    }

    public File getActualResultFile(CompilationUnit cUnit, File expectedFile, File actualResultsBase) {
        File path = actualResultsBase;
        String resultOffsetPath = removeUpward(testSuite.getResultOffsetPath());
        path = new File(path, resultOffsetPath);
        String testCaseFilePath = removeUpward(testCase.getFilePath());
        String expectedFilePath = removeUpward(expectedFile.getName());
        path = new File(path, testCaseFilePath);
        return new File(path, cUnit.getOutputDir().getValue() + File.separator + expectedFilePath);
    }

    private String removeUpward(String filePath) {
        String evil = ".." + File.separatorChar;
        while (filePath.contains(evil)) {
            filePath = filePath.replace(evil, ""); // NOSONAR
        }
        return filePath;
    }

    public boolean isSourceLocationExpected(CompilationUnit cUnit) {
        Boolean v = cUnit.isSourceLocation();
        return v != null ? v : testSuite.isSourceLocation();
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder(testCase.getFilePath());
        sb.append(':');
        for (CompilationUnit cu : testCase.getCompilationUnit()) {
            sb.append(' ');
            sb.append(cu.getName());
        }
        return sb.toString();
    }

    public static class Builder {
        private final boolean m_doSlow;
        private final Pattern m_re;

        public Builder() {
            m_doSlow = System.getProperty("runSlowAQLTests", "false").equals("true");
            String re = System.getProperty("testre");
            if (re == null) {
                m_re = null;
            } else {
                m_re = Pattern.compile(re);
            }
        }

        public List<TestCaseContext> build(File tsRoot) throws Exception {
            return build(tsRoot, DEFAULT_TESTSUITE_XML_NAME);
        }

        public List<TestCaseContext> build(File tsRoot, String tsXMLFilePath) throws Exception {
            return build(tsRoot, new File(tsRoot, tsXMLFilePath));
        }

        public List<TestCaseContext> build(File tsRoot, File tsFile) throws Exception {
            TestSuiteParser tsp = new TestSuiteParser();
            TestSuite ts = tsp.parse(tsFile);
            return build(tsRoot, ts);
        }

        public List<TestCaseContext> build(File tsRoot, TestSuite ts) throws Exception {
            List<TestCaseContext> tccs = new ArrayList<>();
            List<TestGroup> tgPath = new ArrayList<>();
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
                if (m_doSlow || tc.getCategory() != CategoryEnum.SLOW) {
                    boolean matches = false;
                    if (m_re != null) {
                        // Check all compilation units for matching
                        // name. If ANY match, add the test.
                        for (TestCase.CompilationUnit cu : tc.getCompilationUnit()) {
                            if (m_re.matcher(cu.getName()).find()) {
                                matches = true;
                                break;
                            }
                        }
                    } else {
                        // No regex == everything matches
                        matches = true;
                    }

                    if (matches) {
                        tccs.add(new TestCaseContext(tsRoot, ts, tgPath.toArray(new TestGroup[tgPath.size()]), tc));
                    }
                }
            }
            addContexts(tsRoot, ts, tgPath, tg.getTestGroup(), tccs);
        }
    }
}
