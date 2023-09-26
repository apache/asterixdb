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

import java.io.File;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.apache.asterix.api.common.AsterixHyracksIntegrationUtil;
import org.apache.asterix.common.config.GlobalConfig;
import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.common.metadata.DataverseName;
import org.apache.asterix.common.metadata.MetadataConstants;
import org.apache.asterix.common.utils.StoragePathUtil;
import org.apache.asterix.test.common.TestExecutor;
import org.apache.asterix.testframework.context.TestCaseContext;
import org.apache.commons.lang3.StringUtils;
import org.junit.After;
import org.junit.Assert;
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

    @Test
    public void testDataverseNameLimits() throws Exception {
        TestCaseContext.OutputFormat cleanJson = TestCaseContext.OutputFormat.CLEAN_JSON;

        // at max dataverse name limits

        char auml = 228, euml = 235;

        List<DataverseName> dvNameOkList =
                Arrays.asList(
                        // #1. max single-part name
                        DataverseName.createSinglePartName(
                                StringUtils.repeat('a', MetadataConstants.METADATA_OBJECT_NAME_LENGTH_LIMIT_UTF8)),
                        // #2. max single-part name (2-byte characters)
                        DataverseName.createSinglePartName(
                                StringUtils.repeat(auml, MetadataConstants.METADATA_OBJECT_NAME_LENGTH_LIMIT_UTF8 / 2)),
                        // #3. 4 max parts
                        DataverseName.create(Arrays.asList(
                                StringUtils.repeat('a', MetadataConstants.METADATA_OBJECT_NAME_LENGTH_LIMIT_UTF8),
                                StringUtils.repeat('b', MetadataConstants.METADATA_OBJECT_NAME_LENGTH_LIMIT_UTF8),
                                StringUtils.repeat('c', MetadataConstants.METADATA_OBJECT_NAME_LENGTH_LIMIT_UTF8),
                                StringUtils.repeat('d', MetadataConstants.METADATA_OBJECT_NAME_LENGTH_LIMIT_UTF8))),
                        // #3. 4 max parts (2-byte characters)
                        DataverseName.create(Arrays.asList(
                                StringUtils.repeat(auml, MetadataConstants.METADATA_OBJECT_NAME_LENGTH_LIMIT_UTF8 / 2),
                                StringUtils.repeat(euml, MetadataConstants.METADATA_OBJECT_NAME_LENGTH_LIMIT_UTF8 / 2),
                                StringUtils.repeat(auml, MetadataConstants.METADATA_OBJECT_NAME_LENGTH_LIMIT_UTF8 / 2),
                                StringUtils.repeat(euml,
                                        MetadataConstants.METADATA_OBJECT_NAME_LENGTH_LIMIT_UTF8 / 2))),
                        // #4. single-part name containing continuation char
                        DataverseName
                                .createSinglePartName("abc" + StoragePathUtil.DATAVERSE_CONTINUATION_MARKER + "def"),
                        // #5. multi-part name containing continuation chars
                        DataverseName
                                .create(Arrays.asList("abc" + StoragePathUtil.DATAVERSE_CONTINUATION_MARKER + "def",
                                        StoragePathUtil.DATAVERSE_CONTINUATION_MARKER + "def")));

        for (DataverseName dvNameOk : dvNameOkList) {
            String sql = String.format("create dataverse %s;", dvNameOk);
            testExecutor.executeSqlppUpdateOrDdl(sql, cleanJson);
        }

        // exceeding dataverse name limits

        char iuml = 239, ouml = 246;

        List<DataverseName> dvNameErrList =
                Arrays.asList(
                        // #1. single-part name exceeds part length limit
                        DataverseName.createSinglePartName(
                                StringUtils.repeat('A', MetadataConstants.METADATA_OBJECT_NAME_LENGTH_LIMIT_UTF8 + 1)),
                        // #2 single-part name exceeds part length limit (2-byte characters)
                        DataverseName.createSinglePartName(StringUtils.repeat(iuml,
                                MetadataConstants.METADATA_OBJECT_NAME_LENGTH_LIMIT_UTF8 / 2 + 1)),
                        // #3. 2-part name, 2nd part exceed part length limit
                        DataverseName.create(Arrays.asList("A",
                                StringUtils.repeat('B', MetadataConstants.METADATA_OBJECT_NAME_LENGTH_LIMIT_UTF8 + 1))),
                        // #4. 2-part name, 2nd part exceed part length limit (2-byte characters)
                        DataverseName.create(Arrays.asList("A",
                                StringUtils.repeat(ouml,
                                        MetadataConstants.METADATA_OBJECT_NAME_LENGTH_LIMIT_UTF8 / 2 + 1))),
                        // #5. 5-part name, each part at the part length limit, total length limit is exceeded
                        DataverseName.create(Arrays.asList(
                                StringUtils.repeat('A', MetadataConstants.METADATA_OBJECT_NAME_LENGTH_LIMIT_UTF8),
                                StringUtils.repeat('B', MetadataConstants.METADATA_OBJECT_NAME_LENGTH_LIMIT_UTF8),
                                StringUtils.repeat('C', MetadataConstants.METADATA_OBJECT_NAME_LENGTH_LIMIT_UTF8),
                                StringUtils.repeat('D', MetadataConstants.METADATA_OBJECT_NAME_LENGTH_LIMIT_UTF8),
                                StringUtils.repeat('E', MetadataConstants.METADATA_OBJECT_NAME_LENGTH_LIMIT_UTF8))),
                        // #6. 5-part name, each part at the part length limit, total length limit is exceeded (2-byte characters)
                        DataverseName.create(Arrays.asList(
                                StringUtils.repeat(iuml, MetadataConstants.METADATA_OBJECT_NAME_LENGTH_LIMIT_UTF8 / 2),
                                StringUtils.repeat(ouml, MetadataConstants.METADATA_OBJECT_NAME_LENGTH_LIMIT_UTF8 / 2),
                                StringUtils.repeat(iuml, MetadataConstants.METADATA_OBJECT_NAME_LENGTH_LIMIT_UTF8 / 2),
                                StringUtils.repeat(ouml, MetadataConstants.METADATA_OBJECT_NAME_LENGTH_LIMIT_UTF8 / 2),
                                StringUtils.repeat(iuml,
                                        MetadataConstants.METADATA_OBJECT_NAME_LENGTH_LIMIT_UTF8 / 2))),
                        // #7. Multi-part name, each part at the part length limit, total length limit is exceeded
                        DataverseName.create(
                                Collections.nCopies(MetadataConstants.DATAVERSE_NAME_TOTAL_LENGTH_LIMIT_UTF8 + 1, "A")),
                        // #8. Multi-part name, each part at the part length limit, total length limit is exceeded (2-bytes characters)
                        DataverseName.create(
                                Collections.nCopies(MetadataConstants.DATAVERSE_NAME_TOTAL_LENGTH_LIMIT_UTF8 / 2 + 1,
                                        String.valueOf(iuml))));

        ErrorCode invalidNameErrCode = ErrorCode.INVALID_DATABASE_OBJECT_NAME;
        for (DataverseName dvNameErr : dvNameErrList) {
            String sql = String.format("create dataverse %s;", dvNameErr);
            try {
                testExecutor.executeSqlppUpdateOrDdl(sql, cleanJson);
                Assert.fail("Expected failure: " + invalidNameErrCode);
            } catch (Exception e) {

                Assert.assertTrue("Unexpected error message: " + e.getMessage(),
                        e.getMessage().contains(invalidNameErrCode.errorCode()));
            }
        }
    }

    @Test
    public void testInvalidCharacters() throws Exception {
        TestCaseContext.OutputFormat cleanJson = TestCaseContext.OutputFormat.CLEAN_JSON;

        List<List<String>> dvNameBadCharsList = Arrays.asList(
                // #1. nul characters
                Collections.singletonList("abc\u0000def"),
                // #2. leading whitespace
                Collections.singletonList(" abcdef"),
                // #3. file separator
                Collections.singletonList("abc" + File.separatorChar + "def"),
                // #4. single-part starting with ^
                Collections.singletonList(StoragePathUtil.DATAVERSE_CONTINUATION_MARKER + "abcdef"),
                // #5. multi-part w/ first part starting with ^
                Arrays.asList(StoragePathUtil.DATAVERSE_CONTINUATION_MARKER + "abcdef", "abcdef"));

        ErrorCode invalidNameErrCode = ErrorCode.INVALID_DATABASE_OBJECT_NAME;
        for (List<String> dvName : dvNameBadCharsList) {
            String sql = String.format("create dataverse `%s`;", StringUtils.join(dvName, "`.`"));
            try {
                testExecutor.executeSqlppUpdateOrDdl(sql, cleanJson);
                Assert.fail("Expected failure: " + invalidNameErrCode);
            } catch (Exception e) {
                Assert.assertTrue("Unexpected error message: " + e.getMessage(),
                        e.getMessage().contains(invalidNameErrCode.errorCode()));
            }
        }
    }
}
