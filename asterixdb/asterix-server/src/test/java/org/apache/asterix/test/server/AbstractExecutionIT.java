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
package org.apache.asterix.test.server;

import static org.apache.asterix.test.server.NCServiceExecutionIT.APP_HOME;
import static org.apache.asterix.test.server.NCServiceExecutionIT.ASTERIX_APP_DIR;
import static org.apache.hyracks.util.file.FileUtil.joinPath;

import java.io.File;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Objects;

import org.apache.asterix.external.util.ExternalDataConstants;
import org.apache.asterix.external.util.IdentitiyResolverFactory;
import org.apache.asterix.test.base.RetainLogsRule;
import org.apache.asterix.test.common.TestExecutor;
import org.apache.asterix.testframework.context.TestCaseContext;
import org.apache.asterix.testframework.context.TestFileContext;
import org.apache.asterix.testframework.xml.TestCase.CompilationUnit;
import org.apache.asterix.testframework.xml.TestGroup;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.codehaus.plexus.util.FileUtils;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestRule;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

/**
 * Runs the runtime test cases under 'asterix-app/src/test/resources/runtimets'.
 */
@RunWith(Parameterized.class)
public abstract class AbstractExecutionIT {

    protected static final Logger LOGGER = LogManager.getLogger();

    protected static final String PATH_ACTUAL = joinPath("target", "ittest");
    protected static final String PATH_BASE = joinPath("..", "asterix-app", "src", "test", "resources", "runtimets");

    protected static final String HDFS_BASE = "../asterix-app/";

    protected static final TestExecutor testExecutor = new TestExecutor();

    private static final String EXTERNAL_LIBRARY_TEST_GROUP = "lib";

    private static final List<String> badTestCases = new ArrayList<>();

    private static String reportPath = new File(joinPath("target", "failsafe-reports")).getAbsolutePath();

    @Rule
    public TestRule retainLogs = new RetainLogsRule(NCServiceExecutionIT.LOG_DIR, reportPath, this);

    @BeforeClass
    public static void setUp() throws Exception {
        System.out.println("Starting setup");
        if (LOGGER.isInfoEnabled()) {
            LOGGER.info("Starting setup");
        }
        File outdir = new File(PATH_ACTUAL);
        outdir.mkdirs();

        File externalTestsJar = Objects.requireNonNull(new File(joinPath("..", "asterix-external-data", "target"))
                .listFiles((dir, name) -> name.matches("asterix-external-data-.*-tests.jar")))[0];

        FileUtils.copyFile(externalTestsJar, new File(APP_HOME, joinPath("repo", externalTestsJar.getName())));

        NCServiceExecutionIT.setUp();

        FileUtils.copyDirectoryStructure(new File(joinPath("..", "asterix-app", "data")),
                new File(ASTERIX_APP_DIR, joinPath("clusters", "local", "working_dir", "data")));

        FileUtils.copyDirectoryStructure(new File(joinPath("..", "asterix-app", "target", "data")),
                new File(ASTERIX_APP_DIR, joinPath("clusters", "local", "working_dir", "target", "data")));

        // Set the node resolver to be the identity resolver that expects node names
        // to be node controller ids; a valid assumption in test environment.
        System.setProperty(ExternalDataConstants.NODE_RESOLVER_FACTORY_PROPERTY,
                IdentitiyResolverFactory.class.getName());

        reportPath = new File(joinPath("target", "failsafe-reports")).getAbsolutePath();
    }

    @AfterClass
    public static void tearDown() throws Exception {
        File outdir = new File(PATH_ACTUAL);
        File[] files = outdir.listFiles();
        if ((files == null) || (files.length == 0)) {
            outdir.delete();
        }
        //AsterixLifecycleIT.tearDown();
        NCServiceExecutionIT.tearDown();
        if (!badTestCases.isEmpty()) {
            System.out.println("The following test cases left some data");
            for (String testCase : badTestCases) {
                System.out.println(testCase);
            }
        }
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

    public AbstractExecutionIT(TestCaseContext tcCtx) {
        this.tcCtx = tcCtx;
    }

    @Test
    public void test() throws Exception {
        if (skip()) {
            return;
        }
        testExecutor.executeTest(PATH_ACTUAL, tcCtx, null, false);
        testExecutor.cleanup(tcCtx.toString(), badTestCases);
    }

    protected boolean skip() {
        // If the test case contains library commands, we skip them
        List<CompilationUnit> cUnits = tcCtx.getTestCase().getCompilationUnit();
        for (CompilationUnit cUnit : cUnits) {
            List<TestFileContext> testFileCtxs = tcCtx.getTestFiles(cUnit);
            for (TestFileContext ctx : testFileCtxs) {
                if (ctx.getType().equals(EXTERNAL_LIBRARY_TEST_GROUP)) {
                    return true;
                }
            }
        }
        // For now we skip api tests.
        for (TestGroup group : tcCtx.getTestGroups()) {
            if (group != null && "api".equals(group.getName())) {
                LOGGER.info("Skipping test: " + tcCtx.toString());
                return true;
            }
        }
        return false;
    }
}
