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
package org.apache.asterix.test.metadata;

import org.apache.asterix.api.common.AsterixHyracksIntegrationUtil;
import org.apache.asterix.common.config.GlobalConfig;
import org.apache.asterix.test.common.TestExecutor;
import org.apache.asterix.testframework.context.TestCaseContext;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class MetadataManagerTest {

    protected static final String TEST_CONFIG_FILE_NAME = "src/main/resources/cc.conf";
    private static final TestExecutor testExecutor = new TestExecutor();
    private static final AsterixHyracksIntegrationUtil integrationUtil = new AsterixHyracksIntegrationUtil();

    @Before
    public void setUp() throws Exception {
        System.setProperty(GlobalConfig.CONFIG_FILE_PROPERTY, TEST_CONFIG_FILE_NAME);
        integrationUtil.init(true, TEST_CONFIG_FILE_NAME);
    }

    @After
    public void tearDown() throws Exception {
        integrationUtil.deinit(true);
    }

    @Test
    public void dropDataverseWithIndexesAfterRestart() throws Exception {
        final TestCaseContext.OutputFormat cleanJson = TestCaseContext.OutputFormat.CLEAN_JSON;
        StringBuilder sql = new StringBuilder();
        sql.append("create dataverse test;");
        sql.append("use test;");
        sql.append("create type testType as{id:int};");
        sql.append("create dataset testDS(testType) primary key id;");
        sql.append("create primary index primaryIdx on testDS;");
        sql.append("insert into testDS{\"id\":1};");
        testExecutor.executeSqlppUpdateOrDdl(sql.toString(), cleanJson);
        // restart
        integrationUtil.deinit(false);
        integrationUtil.init(false, TEST_CONFIG_FILE_NAME);
        // drop then recreate dataverse
        testExecutor.executeSqlppUpdateOrDdl("drop dataverse test;", cleanJson);
        testExecutor.executeSqlppUpdateOrDdl(sql.toString(), cleanJson);
    }
}
