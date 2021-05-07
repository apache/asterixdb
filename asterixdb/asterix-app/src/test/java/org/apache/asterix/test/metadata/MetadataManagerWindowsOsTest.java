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

import java.util.ArrayList;
import java.util.List;

import org.apache.asterix.api.common.AsterixHyracksIntegrationUtil;
import org.apache.asterix.common.config.GlobalConfig;
import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.test.common.TestExecutor;
import org.apache.asterix.testframework.context.TestCaseContext;
import org.apache.commons.lang3.SystemUtils;
import org.jetbrains.annotations.NotNull;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class MetadataManagerWindowsOsTest {

    static {
        System.setProperty("os.name", "Windows");
    }

    protected static final String TEST_CONFIG_FILE_NAME = "src/main/resources/cc.conf";
    private static final TestExecutor testExecutor = new TestExecutor();
    private static final AsterixHyracksIntegrationUtil integrationUtil = new AsterixHyracksIntegrationUtil();

    @Before
    public void setUp() throws Exception {
        System.setProperty(GlobalConfig.CONFIG_FILE_PROPERTY, TEST_CONFIG_FILE_NAME);
        integrationUtil.init(true, TEST_CONFIG_FILE_NAME);
        Assert.assertTrue("wrong os reported", SystemUtils.IS_OS_WINDOWS);
    }

    @After
    public void tearDown() throws Exception {
        integrationUtil.deinit(true);
    }

    @Test
    public void testInvalidCharacters() throws Exception {
        TestCaseContext.OutputFormat cleanJson = TestCaseContext.OutputFormat.CLEAN_JSON;

        List<String> dvNameBadCharsList = new ArrayList<>();

        for (char c = 0; c <= 0x1F; c++) {
            dvNameBadCharsList.add(badCharName(c));
        }
        dvNameBadCharsList.add(badCharName('\u007f'));
        dvNameBadCharsList.add(badCharName("\\\\"));
        dvNameBadCharsList.add(badCharName('/'));
        dvNameBadCharsList.add(badCharName('>'));
        dvNameBadCharsList.add(badCharName('\n'));
        dvNameBadCharsList.add(badCharName('|'));

        ErrorCode invalidNameErrCode = ErrorCode.INVALID_DATABASE_OBJECT_NAME;
        for (String dvName : dvNameBadCharsList) {
            String sql = String.format("create dataverse `%s`;", dvName);
            try {
                testExecutor.executeSqlppUpdateOrDdl(sql, cleanJson);
                Assert.fail("Expected failure: " + invalidNameErrCode);
            } catch (Exception e) {
                Assert.assertTrue("Unexpected error message: " + e.getMessage(),
                        e.getMessage().contains(invalidNameErrCode.errorCode()));
            }
        }
    }

    @NotNull
    protected String badCharName(char c) {
        return badCharName(String.valueOf(c));
    }

    @NotNull
    protected String badCharName(String s) {
        return "abc" + s + "def";
    }
}
