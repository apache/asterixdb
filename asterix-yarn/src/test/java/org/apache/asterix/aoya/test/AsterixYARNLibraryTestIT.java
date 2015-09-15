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
package org.apache.asterix.aoya.test;

import java.io.File;
import java.util.List;
import java.util.logging.Logger;

import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.asterix.aoya.AsterixYARNClient;
import org.apache.asterix.test.aql.TestsUtils;
import org.apache.asterix.testframework.context.TestCaseContext;

public class AsterixYARNLibraryTestIT {
    private static final String LIBRARY_NAME = "testlib";
    private static final String LIBRARY_DATAVERSE = "externallibtest";
    private static final String INSTANCE_NAME = "asterix-lib-test";
    private static final String PATH_BASE = "src/test/resources/library";
    private static final String PATH_ACTUAL = "ittest/";
    private static final Logger LOGGER = Logger.getLogger(AsterixYARNLifecycleIT.class.getName());
    private static String configPath;
    private static String aoyaServerPath;
    private static String parameterPath;
    private static AsterixYARNInstanceUtil instance;
    private static YarnConfiguration appConf;
    private static List<TestCaseContext> testCaseCollection;
    private static final String LIBRARY_PATH = "asterix-external-data" + File.separator + "target" + File.separator
            + "testlib-zip-binary-assembly.zip";

    @BeforeClass
    public static void setUp() throws Exception {
        instance = new AsterixYARNInstanceUtil();
        appConf = instance.setUp();
        configPath = instance.configPath;
        aoyaServerPath = instance.aoyaServerPath;
        parameterPath = instance.parameterPath;

        String command = "-n " + INSTANCE_NAME + " -c " + configPath + " -bc " + parameterPath + " -zip "
                + aoyaServerPath + " install";
        executeAoyaCommand(command);

        command = "-n " + INSTANCE_NAME + " -bc " + parameterPath + " stop";
        executeAoyaCommand(command);

        String asterixExternalLibraryPath = new File(System.getProperty("user.dir")).getParentFile().getAbsolutePath()
                + File.separator + LIBRARY_PATH;
        command = "-n " + INSTANCE_NAME + " -l " + asterixExternalLibraryPath + " -ld " + LIBRARY_DATAVERSE + " -bc " + parameterPath + " libinstall";
        executeAoyaCommand(command);

        command = "-n " + INSTANCE_NAME + " -bc " + parameterPath + " start";
        executeAoyaCommand(command);

        TestCaseContext.Builder b = new TestCaseContext.Builder();
        testCaseCollection = b.build(new File(PATH_BASE));
    }

    @AfterClass
    public static void tearDown() throws Exception {
        String command = "-n " + INSTANCE_NAME + " -zip " +  aoyaServerPath + " -f" + " -bc " + parameterPath + " destroy";
        executeAoyaCommand(command);
        instance.tearDown();
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
            new AsterixYARNLibraryTestIT().test();
        } catch (Exception e) {
            e.printStackTrace();
            LOGGER.info("TEST CASES FAILED");
        } finally {
            tearDown();
        }
    }

    static void executeAoyaCommand(String cmd) throws Exception {
        AsterixYARNClient aoyaClient = new AsterixYARNClient(appConf);
        aoyaClient.init(cmd.split(" "));
        AsterixYARNClient.execute(aoyaClient);
    }
}
