/*
 *
 *  * Licensed to the Apache Software Foundation (ASF) under one
 *  * or more contributor license agreements.  See the NOTICE file
 *  * distributed with this work for additional information
 *  * regarding copyright ownership.  The ASF licenses this file
 *  * to you under the Apache License, Version 2.0 (the
 *  * "License"); you may not use this file except in compliance
 *  * with the License.  You may obtain a copy of the License at
 *  *
 *  *   http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing,
 *  * software distributed under the License is distributed on an
 *  * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  * KIND, either express or implied.  See the License for the
 *  * specific language governing permissions and limitations
 *  * under the License.
 *
 */
package org.apache.asterix.test.runtime;

import java.io.File;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.apache.asterix.app.external.TestLibrarian;
import org.apache.asterix.common.library.ILibraryManager;
import org.apache.asterix.test.aql.TestExecutor;
import org.apache.asterix.testframework.context.TestCaseContext;
import org.apache.commons.lang3.StringUtils;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

/**
 * Utils for running SQL++ or AQL runtime tests.
 */
@RunWith(Parameterized.class)
public class LangExecutionUtil {

    private static final String PATH_ACTUAL = "target" + File.separator + "rttest" + File.separator;
    private static final String PATH_BASE = StringUtils.join(new String[] { "src", "test", "resources", "runtimets" },
            File.separator);

    private static final boolean cleanupOnStart = true;
    private static final boolean cleanupOnStop = true;
    private static final List<String> badTestCases = new ArrayList<>();
    private static final TestExecutor testExecutor = new TestExecutor();

    private static TestLibrarian librarian;
    private static final int repeat = Integer.getInteger("test.repeat", 1);

    public static void setUp(String configFile) throws Exception {
        File outdir = new File(PATH_ACTUAL);
        outdir.mkdirs();
        List<ILibraryManager> libraryManagers = ExecutionTestUtil.setUp(cleanupOnStart, configFile);
        TestLibrarian.removeLibraryDir();
        librarian = new TestLibrarian(libraryManagers);
        testExecutor.setLibrarian(librarian);
        if (repeat != 1) {
            System.out.println("FYI: each test will be run " + repeat + " times.");
        }
    }

    public static void tearDown() throws Exception {
        TestLibrarian.removeLibraryDir();
        ExecutionTestUtil.tearDown(cleanupOnStop);
        ExecutionTestUtil.integrationUtil.removeTestStorageFiles();
        if (!badTestCases.isEmpty()) {
            System.out.println("The following test cases left some data");
            for (String testCase : badTestCases) {
                System.out.println(testCase);
            }
        }
    }

    public static Collection<Object[]> tests(String onlyFilePath, String suiteFilePath) throws Exception {
        Collection<Object[]> testArgs = buildTestsInXml(onlyFilePath);
        if (testArgs.size() == 0) {
            testArgs = buildTestsInXml(suiteFilePath);
        }
        return testArgs;
    }

    protected static Collection<Object[]> buildTestsInXml(String xmlfile) throws Exception {
        Collection<Object[]> testArgs = new ArrayList<>();
        TestCaseContext.Builder b = new TestCaseContext.Builder();
        for (TestCaseContext ctx : b.build(new File(PATH_BASE), xmlfile)) {
            testArgs.add(new Object[] { ctx });
        }
        return testArgs;
    }

    public static void test(TestCaseContext tcCtx) throws Exception {
        int repeat = LangExecutionUtil.repeat * tcCtx.getRepeat();
        try {
            for (int i = 1; i <= repeat; i++) {
                if (repeat > 1) {
                    System.err.print("[" + i + "/" + repeat + "] ");
                }
                librarian.cleanup();
                testExecutor.executeTest(PATH_ACTUAL, tcCtx, null, false, ExecutionTestUtil.FailedGroup);
                try {
                    testExecutor.cleanup(tcCtx.toString(), badTestCases);
                } catch (Throwable th) {
                    th.printStackTrace();
                    throw th;
                }
            }
        } finally {
            System.err.flush();
        }
    }
}
