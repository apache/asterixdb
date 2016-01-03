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
package org.apache.asterix.installer.test;

import java.io.File;
import java.util.ArrayList;
import java.util.Collection;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.asterix.external.util.ExternalDataConstants;
import org.apache.asterix.external.util.IdentitiyResolverFactory;
import org.apache.asterix.test.aql.TestExecutor;
import org.apache.asterix.test.runtime.HDFSCluster;
import org.apache.asterix.testframework.context.TestCaseContext;
import org.apache.commons.lang3.StringUtils;
import org.codehaus.plexus.util.FileUtils;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

/**
 * Runs the runtime test cases under 'asterix-app/src/test/resources/runtimets'.
 */
@RunWith(Parameterized.class)
public abstract class AbstractExecutionIT {

    protected static final Logger LOGGER = Logger.getLogger(AbstractExecutionIT.class.getName());

    protected static final String PATH_ACTUAL = "ittest" + File.separator;
    protected static final String PATH_BASE = StringUtils
            .join(new String[] { "..", "asterix-app", "src", "test", "resources", "runtimets" }, File.separator);

    protected static final String HDFS_BASE = "../asterix-app/";

    protected final static TestExecutor testExecutor = new TestExecutor();

    @BeforeClass
    public static void setUp() throws Exception {
        System.out.println("Starting setup");
        if (LOGGER.isLoggable(Level.INFO)) {
            LOGGER.info("Starting setup");
        }
        File outdir = new File(PATH_ACTUAL);
        outdir.mkdirs();

        HDFSCluster.getInstance().setup(HDFS_BASE);

        //This is nasty but there is no very nice way to set a system property on each NC that I can figure.
        //The main issue is that we need the NC resolver to be the IdentityResolver and not the DNSResolver.
        FileUtils
                .copyFile(
                        new File(StringUtils.join(new String[] { "src", "test", "resources", "integrationts",
                                "asterix-configuration.xml" }, File.separator)),
                new File(AsterixInstallerIntegrationUtil.getManagixHome() + "/conf/asterix-configuration.xml"));

        AsterixLifecycleIT.setUp();

        FileUtils.copyDirectoryStructure(
                new File(StringUtils.join(new String[] { "..", "asterix-app", "data" }, File.separator)),
                new File(AsterixInstallerIntegrationUtil.getManagixHome() + "/clusters/local/working_dir/data"));

        // Set the node resolver to be the identity resolver that expects node names
        // to be node controller ids; a valid assumption in test environment.
        System.setProperty(ExternalDataConstants.NODE_RESOLVER_FACTORY_PROPERTY,
                IdentitiyResolverFactory.class.getName());
    }

    @AfterClass
    public static void tearDown() throws Exception {
        File outdir = new File(PATH_ACTUAL);
        File[] files = outdir.listFiles();
        if (files == null || files.length == 0) {
            outdir.delete();
        }
        AsterixLifecycleIT.tearDown();

        HDFSCluster.getInstance().cleanup();
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
        testExecutor.executeTest(PATH_ACTUAL, tcCtx, null, false);
    }
}
