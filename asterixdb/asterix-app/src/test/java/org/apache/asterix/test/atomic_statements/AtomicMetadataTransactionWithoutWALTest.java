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
package org.apache.asterix.test.atomic_statements;

import static org.apache.hyracks.util.file.FileUtil.joinPath;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Random;

import org.apache.asterix.api.common.AsterixHyracksIntegrationUtil;
import org.apache.asterix.api.common.LocalCloudUtil;
import org.apache.asterix.common.TestDataUtil;
import org.apache.asterix.common.utils.Servlets;
import org.apache.asterix.test.common.TestExecutor;
import org.apache.asterix.testframework.context.TestCaseContext;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

public class AtomicMetadataTransactionWithoutWALTest {
    public static final String RESOURCES_PATH = joinPath(System.getProperty("user.dir"), "src", "test", "resources");
    public static final String CONFIG_FILE = joinPath(RESOURCES_PATH, "cc-cloud-storage.conf");
    private static final AsterixHyracksIntegrationUtil integrationUtil = new AsterixHyracksIntegrationUtil();
    private static final TestExecutor TEST_EXECUTOR = new TestExecutor();
    private static final TestCaseContext.OutputFormat OUTPUT_FORMAT = TestCaseContext.OutputFormat.CLEAN_JSON;
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    private static final String DATASET_NAME_PREFIX = "ds_";
    private static final int NUM_DATASETS = 500;
    private static final int NUM_RECOVERIES = 10;

    @Before
    public void setUp() throws Exception {
        boolean cleanStart = Boolean.getBoolean("cleanup.start");
        LocalCloudUtil.startS3CloudEnvironment(cleanStart);
        integrationUtil.setGracefulShutdown(false);
        integrationUtil.init(true, CONFIG_FILE);
    }

    @After
    public void tearDown() throws Exception {
        integrationUtil.deinit(true);
    }

    private void createDatasets() throws Exception {
        String datasetName;
        for (int i = 0; i < NUM_DATASETS; i++) {
            datasetName = DATASET_NAME_PREFIX + i;
            TestDataUtil.createDatasetWithoutType(datasetName, Map.of("id", "uuid"), true);
        }
    }

    @Test
    public void testAtomicityWithFailures() throws Exception {
        final String leftJoinQuery = "SELECT VALUE COUNT(*) FROM Metadata.`Dataset` ds LEFT JOIN Metadata.`Index` i "
                + "ON ds.DatasetName=i.DatasetName AND i.IsPrimary=true WHERE ds.DatasetId!=0 AND i.DatasetName IS MISSING;";
        final String rightJoinQuery = "SELECT VALUE COUNT(*) FROM Metadata.`Dataset` ds RIGHT JOIN Metadata.`Index` i "
                + "ON ds.DatasetName=i.DatasetName AND i.IsPrimary=true WHERE ds.DatasetId!=0 AND ds.DatasetName IS MISSING;";
        for (int i = 0; i <= NUM_RECOVERIES; i++) {
            Thread thread = new Thread(() -> {
                try {
                    createDatasets();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            });
            thread.start();
            Random rnd = new Random();
            Thread.sleep(rnd.nextInt(100) + 10);
            integrationUtil.deinit(true);
            integrationUtil.init(true, CONFIG_FILE);

            Assert.assertEquals(0, runCountQuery(leftJoinQuery));
            Assert.assertEquals(0, runCountQuery(rightJoinQuery));
        }
    }

    private int runCountQuery(String query) throws Exception {
        InputStream responseStream = TEST_EXECUTOR.executeQueryService(query,
                TEST_EXECUTOR.getEndpoint(Servlets.QUERY_SERVICE), OUTPUT_FORMAT, StandardCharsets.UTF_8);
        ObjectNode response = OBJECT_MAPPER.readValue(responseStream, ObjectNode.class);
        JsonNode result = response.get("results");
        Assert.assertEquals(1, result.size());
        return result.get(0).asInt();
    }
}
