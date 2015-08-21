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
package edu.uci.ics.asterix.test.runtime;

import java.io.File;
import java.util.ArrayList;
import java.util.Collection;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import edu.uci.ics.asterix.api.common.AsterixHyracksIntegrationUtil;
import edu.uci.ics.asterix.common.config.AsterixPropertiesAccessor;
import edu.uci.ics.asterix.common.config.AsterixTransactionProperties;
import edu.uci.ics.asterix.common.config.GlobalConfig;
import edu.uci.ics.asterix.external.dataset.adapter.FileSystemBasedAdapter;
import edu.uci.ics.asterix.external.util.IdentitiyResolverFactory;
import edu.uci.ics.asterix.test.aql.TestsUtils;
import edu.uci.ics.asterix.testframework.context.TestCaseContext;

/**
 * Runs the runtime test cases under 'asterix-app/src/test/resources/runtimets'.
 */
@RunWith(Parameterized.class)
public class ExecutionTest {

    protected static final Logger LOGGER = Logger.getLogger(ExecutionTest.class.getName());

    protected static final String PATH_ACTUAL = "rttest" + File.separator;
    protected static final String PATH_BASE = StringUtils.join(
            new String[] { "src", "test", "resources", "runtimets" }, File.separator);

    protected static final String TEST_CONFIG_FILE_NAME = "asterix-build-configuration.xml";

    protected static AsterixTransactionProperties txnProperties;

    @BeforeClass
    public static void setUp() throws Exception {
        System.out.println("Starting setup");
        if (LOGGER.isLoggable(Level.INFO)) {
            LOGGER.info("Starting setup");
        }
        System.setProperty(GlobalConfig.CONFIG_FILE_PROPERTY, TEST_CONFIG_FILE_NAME);
        File outdir = new File(PATH_ACTUAL);
        outdir.mkdirs();

        AsterixPropertiesAccessor apa = new AsterixPropertiesAccessor();
        txnProperties = new AsterixTransactionProperties(apa);

        deleteTransactionLogs();

        if (LOGGER.isLoggable(Level.INFO)) {
            LOGGER.info("initializing pseudo cluster");
        }
        AsterixHyracksIntegrationUtil.init();

        if (LOGGER.isLoggable(Level.INFO)) {
            LOGGER.info("initializing HDFS");
        }

        HDFSCluster.getInstance().setup();

        // Set the node resolver to be the identity resolver that expects node names
        // to be node controller ids; a valid assumption in test environment.
        System.setProperty(FileSystemBasedAdapter.NODE_RESOLVER_FACTORY_PROPERTY,
                IdentitiyResolverFactory.class.getName());
    }

    @AfterClass
    public static void tearDown() throws Exception {
        AsterixHyracksIntegrationUtil.deinit();
        File outdir = new File(PATH_ACTUAL);
        File[] files = outdir.listFiles();
        if (files == null || files.length == 0) {
            outdir.delete();
        }
        // clean up the files written by the ASTERIX storage manager
        for (String d : AsterixHyracksIntegrationUtil.getDataDirs()) {
            TestsUtils.deleteRec(new File(d));
        }
        HDFSCluster.getInstance().cleanup();
    }

    private static void deleteTransactionLogs() throws Exception {
        for (String ncId : AsterixHyracksIntegrationUtil.getNcNames()) {
            File log = new File(txnProperties.getLogDirectory(ncId));
            if (log.exists()) {
                FileUtils.deleteDirectory(log);
            }
        }
    }

    @Parameters
    public static Collection<Object[]> tests() throws Exception {
        Collection<Object[]> testArgs = buildTestsInXml(TestCaseContext.ONLY_TESTSUITE_XML_NAME);
        if (testArgs.size() == 0) {
            testArgs = buildTestsInXml(TestCaseContext.DEFAULT_TESTSUITE_XML_NAME);
        }
        return testArgs;
    }

    protected static Collection<Object[]> buildTestsInXml(String xmlfile) throws Exception {
        Collection<Object[]> testArgs = new ArrayList<Object[]>();
        TestCaseContext.Builder b = new TestCaseContext.Builder();
        for (TestCaseContext ctx : b.build(new File(PATH_BASE), xmlfile)) {
            testArgs.add(new Object[] { ctx });
        }
        return testArgs;

    }

    protected TestCaseContext tcCtx;

    public ExecutionTest(TestCaseContext tcCtx) {
        this.tcCtx = tcCtx;
    }

    @Test
    public void test() throws Exception {
            TestsUtils.executeTest(PATH_ACTUAL, tcCtx, null, false);
    }
}
