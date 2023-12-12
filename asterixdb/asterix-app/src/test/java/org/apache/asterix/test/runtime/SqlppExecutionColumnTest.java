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
package org.apache.asterix.test.runtime;

import java.io.File;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.asterix.common.config.GlobalConfig;
import org.apache.asterix.test.base.AsterixTestHelper;
import org.apache.asterix.test.common.TestExecutor;
import org.apache.asterix.testframework.context.TestCaseContext;
import org.apache.asterix.testframework.xml.TestCase;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

/**
 * Runs the SQL++ runtime tests with columnar storage format.
 */
@RunWith(Parameterized.class)
public class SqlppExecutionColumnTest {
    private static final String TEST_CONFIG_FILE_NAME = "src/test/resources/cc-columnar.conf";
    private static final String IGNORE_FILE = "src/test/resources/runtimets/ignore_column.txt";
    private static final String DELTA_RESULT_PATH = "results_column";
    private final static Set<String> IGNORED_GROUPS = Set.of("column");
    private static Set<String> IGNORED;

    @BeforeClass
    public static void setUp() throws Exception {
        final TestExecutor testExecutor = new TestExecutor(DELTA_RESULT_PATH);
        LangExecutionUtil.setUp(TEST_CONFIG_FILE_NAME, testExecutor);
        System.setProperty(GlobalConfig.CONFIG_FILE_PROPERTY, TEST_CONFIG_FILE_NAME);
    }

    @AfterClass
    public static void tearDown() throws Exception {
        LangExecutionUtil.tearDown();
    }

    @Parameters(name = "SqlppColumnarExecutionTest {index}: {0}")
    public static Collection<Object[]> tests() throws Exception {
        IGNORED = new HashSet<>(AsterixTestHelper.readTestListFile(new File(IGNORE_FILE)));
        Collection<Object[]> tests = LangExecutionUtil.tests("only_sqlpp.xml", "testsuite_sqlpp_column.xml");
        return tests.stream().filter(SqlppExecutionColumnTest::allow).collect(Collectors.toList());
    }

    private static boolean allow(Object[] test) {
        TestCaseContext ctx = (TestCaseContext) test[0];
        TestCase testCase = ctx.getTestCase();
        boolean notIgnored = !IGNORED_GROUPS.contains(testCase.getFilePath()) && !IGNORED.contains(ctx.toString());
        boolean noCorrelatedPrefix = testCase.getCompilationUnit().stream().noneMatch(cu -> cu.getPlaceholder().stream()
                .anyMatch(ph -> ph.getValue().toLowerCase().contains("correlated-prefix")));
        return notIgnored && noCorrelatedPrefix;
    }

    protected TestCaseContext tcCtx;

    public SqlppExecutionColumnTest(TestCaseContext tcCtx) {
        this.tcCtx = tcCtx;
    }

    @Test
    public void test() throws Exception {
        LangExecutionUtil.test(tcCtx);
    }
}
