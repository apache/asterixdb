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
package org.apache.asterix.common;

import java.io.InputStream;
import java.util.Arrays;
import java.util.LinkedHashSet;

import org.apache.asterix.api.common.AsterixHyracksIntegrationUtil;
import org.apache.asterix.app.active.ActiveNotificationHandler;
import org.apache.asterix.common.api.IMetadataLockManager;
import org.apache.asterix.common.dataflow.ICcApplicationContext;
import org.apache.asterix.common.utils.Servlets;
import org.apache.asterix.metadata.declared.MetadataProvider;
import org.apache.asterix.rebalance.NoOpDatasetRebalanceCallback;
import org.apache.asterix.test.common.TestExecutor;
import org.apache.asterix.testframework.context.TestCaseContext;
import org.apache.asterix.utils.RebalanceUtil;
import org.junit.Assert;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

public class TestDataUtil {

    private static final TestExecutor TEST_EXECUTOR = new TestExecutor();
    private static final TestCaseContext.OutputFormat OUTPUT_FORMAT = TestCaseContext.OutputFormat.CLEAN_JSON;
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    private TestDataUtil() {
    }

    /**
     * Creates dataset with a single field called id as its primary key.
     *
     * @param dataset
     * @throws Exception
     */
    public static void createIdOnlyDataset(String dataset) throws Exception {
        TEST_EXECUTOR.executeSqlppUpdateOrDdl("CREATE TYPE KeyType IF NOT EXISTS AS { id: int };", OUTPUT_FORMAT);
        TEST_EXECUTOR.executeSqlppUpdateOrDdl("CREATE DATASET " + dataset + "(KeyType) PRIMARY KEY id;", OUTPUT_FORMAT);
    }

    /**
     * Upserts {@code count} ids into {@code dataset}
     *
     * @param dataset
     * @param count
     * @throws Exception
     */
    public static void upsertData(String dataset, long count) throws Exception {
        for (int i = 0; i < count; i++) {
            TEST_EXECUTOR.executeSqlppUpdateOrDdl("UPSERT INTO " + dataset + " ({\"id\": " + i + "});",
                    TestCaseContext.OutputFormat.CLEAN_JSON);
        }
    }

    /**
     * Gets the number of records in dataset {@code dataset}
     *
     * @param datasetName
     * @return The count
     * @throws Exception
     */
    public static long getDatasetCount(String datasetName) throws Exception {
        final String query = "SELECT VALUE COUNT(*) FROM `" + datasetName + "`;";
        final InputStream responseStream = TEST_EXECUTOR
                .executeQueryService(query, TEST_EXECUTOR.getEndpoint(Servlets.QUERY_SERVICE), OUTPUT_FORMAT);
        final ObjectNode response = OBJECT_MAPPER.readValue(responseStream, ObjectNode.class);
        final JsonNode result = response.get("results");
        // make sure there is a single value in result
        Assert.assertEquals(1, result.size());
        return result.get(0).asInt();
    }

    /**
     * Rebalances a dataset to {@code targetNodes}
     *
     * @param integrationUtil
     * @param dataverseName
     * @param datasetName
     * @param targetNodes
     * @throws Exception
     */
    public static void rebalanceDataset(AsterixHyracksIntegrationUtil integrationUtil, String dataverseName,
            String datasetName, String[] targetNodes) throws Exception {
        ICcApplicationContext ccAppCtx =
                (ICcApplicationContext) integrationUtil.getClusterControllerService().getApplicationContext();
        MetadataProvider metadataProvider = new MetadataProvider(ccAppCtx, null);
        try {
            ActiveNotificationHandler activeNotificationHandler =
                    (ActiveNotificationHandler) ccAppCtx.getActiveNotificationHandler();
            activeNotificationHandler.suspend(metadataProvider);
            try {
                IMetadataLockManager lockManager = ccAppCtx.getMetadataLockManager();
                lockManager.acquireDatasetExclusiveModificationLock(metadataProvider.getLocks(),
                        dataverseName + '.' + datasetName);
                RebalanceUtil.rebalance(dataverseName, datasetName, new LinkedHashSet<>(Arrays.asList(targetNodes)),
                        metadataProvider, ccAppCtx.getHcc(), NoOpDatasetRebalanceCallback.INSTANCE);
            } finally {
                activeNotificationHandler.resume(metadataProvider);
            }
        } finally {
            metadataProvider.getLocks().unlock();
        }
    }
}
