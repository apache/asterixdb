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
package org.apache.asterix.test.txn;

import java.io.File;
import java.io.InputStream;
import java.util.Random;

import org.apache.asterix.api.common.AsterixHyracksIntegrationUtil;
import org.apache.asterix.common.config.GlobalConfig;
import org.apache.asterix.common.configuration.AsterixConfiguration;
import org.apache.asterix.common.configuration.Property;
import org.apache.asterix.common.utils.Servlets;
import org.apache.asterix.test.common.TestExecutor;
import org.apache.asterix.test.common.TestHelper;
import org.apache.asterix.testframework.context.TestCaseContext;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

public class RecoveryManagerTest {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private static final String DEFAULT_TEST_CONFIG_FILE_NAME = "asterix-build-configuration.xml";
    private static final String TEST_CONFIG_FILE_NAME = "asterix-test-configuration.xml";
    private static final String TEST_CONFIG_PATH =
            System.getProperty("user.dir") + File.separator + "target" + File.separator + "config";
    private static final String TEST_CONFIG_FILE_PATH = TEST_CONFIG_PATH + File.separator + TEST_CONFIG_FILE_NAME;
    private static final TestExecutor testExecutor = new TestExecutor();
    private static final AsterixHyracksIntegrationUtil integrationUtil = new AsterixHyracksIntegrationUtil();
    private static final Random random = new Random();
    private static final int numRecords = 1;

    @Before
    public void setUp() throws Exception {
        // Read default test configurations
        AsterixConfiguration ac = TestHelper.getConfigurations(DEFAULT_TEST_CONFIG_FILE_NAME);
        // override memory config to enforce dataset eviction
        ac.getProperty().add(new Property("storage.memorycomponent.globalbudget", "128MB", ""));
        ac.getProperty().add(new Property("storage.memorycomponent.numpages", "32", ""));
        // Write test config file
        TestHelper.writeConfigurations(ac, TEST_CONFIG_FILE_PATH);
        System.setProperty(GlobalConfig.CONFIG_FILE_PROPERTY, TEST_CONFIG_FILE_PATH);
        integrationUtil.setGracefulShutdown(false);
        integrationUtil.init(true);
    }

    @After
    public void tearDown() throws Exception {
        integrationUtil.deinit(true);
    }

    @Test
    public void multiDatasetRecovery() throws Exception {
        String datasetNamePrefix = "ds_";
        final TestCaseContext.OutputFormat format = TestCaseContext.OutputFormat.CLEAN_JSON;
        testExecutor.executeSqlppUpdateOrDdl("CREATE TYPE KeyType AS { id: int };", format);
        int numDatasets = 50;
        String datasetName = null;
        for (int i = 1; i <= numDatasets; i++) {
            datasetName = datasetNamePrefix + i;
            testExecutor.executeSqlppUpdateOrDdl("CREATE DATASET " + datasetName + "(KeyType) PRIMARY KEY id;", format);
            insertData(datasetName);
        }
        // do ungraceful shutdown to enforce recovery
        integrationUtil.deinit(false);
        integrationUtil.init(false);
        validateRecovery(datasetName);

        // create more datasets after recovery
        numDatasets = 100;
        for (int i = 51; i <= numDatasets; i++) {
            datasetName = datasetNamePrefix + i;
            testExecutor.executeSqlppUpdateOrDdl("CREATE DATASET " + datasetName + "(KeyType) PRIMARY KEY id;", format);
            insertData(datasetName);
        }
        // do ungraceful shutdown to enforce recovery again
        integrationUtil.deinit(false);
        integrationUtil.init(false);
        validateRecovery(datasetName);
    }

    private void insertData(String datasetName) throws Exception {
        for (int i = 0; i < numRecords; i++) {
            testExecutor.executeSqlppUpdateOrDdl("UPSERT INTO " + datasetName + " ({\"id\": " + random.nextInt() + "})",
                    TestCaseContext.OutputFormat.CLEAN_JSON);
        }
    }

    private void validateRecovery(String datasetName) throws Exception {
        final String query = "select value count(*) from `" + datasetName + "`;";
        final InputStream inputStream = testExecutor
                .executeQueryService(query, testExecutor.getEndpoint(Servlets.QUERY_SERVICE),
                        TestCaseContext.OutputFormat.CLEAN_JSON);
        final ObjectNode jsonNodes = OBJECT_MAPPER.readValue(inputStream, ObjectNode.class);
        JsonNode result = jsonNodes.get("results");
        // make sure there is result
        Assert.assertEquals(1, result.size());
        for (int i = 0; i < result.size(); i++) {
            JsonNode json = result.get(i);
            Assert.assertEquals(numRecords, json.asInt());
        }
    }
}