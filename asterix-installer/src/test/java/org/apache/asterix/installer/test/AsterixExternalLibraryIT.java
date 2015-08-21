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
package edu.uci.ics.asterix.installer.test;

import java.io.File;
import java.util.List;
import java.util.logging.Logger;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import edu.uci.ics.asterix.event.model.AsterixInstance.State;
import edu.uci.ics.asterix.test.aql.TestsUtils;
import edu.uci.ics.asterix.testframework.context.TestCaseContext;

public class AsterixExternalLibraryIT {

    private static final String LIBRARY_NAME = "testlib";
    private static final String LIBRARY_DATAVERSE = "externallibtest";
    private static final String PATH_BASE = "src/test/resources/integrationts/library";
    private static final String PATH_ACTUAL = "ittest/";
    private static final String LIBRARY_PATH = "asterix-external-data" + File.separator + "target" + File.separator
            + "testlib-zip-binary-assembly.zip";
    private static final Logger LOGGER = Logger.getLogger(AsterixExternalLibraryIT.class.getName());
    private static List<TestCaseContext> testCaseCollection;

    @BeforeClass
    public static void setUp() throws Exception {
        AsterixInstallerIntegrationUtil.init();
        File asterixInstallerProjectDir = new File(System.getProperty("user.dir"));
        String asterixExternalLibraryPath = asterixInstallerProjectDir.getParentFile().getAbsolutePath()
                + File.separator + LIBRARY_PATH;
        LOGGER.info("Installing library :" + LIBRARY_NAME + " located at " + asterixExternalLibraryPath
                + " in dataverse " + LIBRARY_DATAVERSE);
        AsterixInstallerIntegrationUtil.installLibrary(LIBRARY_NAME, LIBRARY_DATAVERSE, asterixExternalLibraryPath);
        AsterixInstallerIntegrationUtil.transformIntoRequiredState(State.ACTIVE);
        TestCaseContext.Builder b = new TestCaseContext.Builder();
        testCaseCollection = b.build(new File(PATH_BASE));
    }

    @AfterClass
    public static void tearDown() throws Exception {
        AsterixInstallerIntegrationUtil.deinit();
    }

    @Test
    public void test() throws Exception {
        for (TestCaseContext testCaseCtx : testCaseCollection) {
            TestsUtils.executeTest(PATH_ACTUAL, testCaseCtx, null, false);
        }
    }

    public static void main(String[] args) throws Exception {
        try {
            setUp();
            new AsterixExternalLibraryIT().test();
        } catch (Exception e) {
            e.printStackTrace();
            LOGGER.info("TEST CASES FAILED");
        } finally {
            tearDown();
        }
    }

}
