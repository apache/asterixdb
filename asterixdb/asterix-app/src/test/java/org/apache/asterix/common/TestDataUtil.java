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
import java.nio.charset.StandardCharsets;
import java.rmi.RemoteException;
import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Optional;

import org.apache.asterix.api.common.AsterixHyracksIntegrationUtil;
import org.apache.asterix.app.active.ActiveNotificationHandler;
import org.apache.asterix.common.api.IMetadataLockManager;
import org.apache.asterix.common.dataflow.ICcApplicationContext;
import org.apache.asterix.common.utils.Servlets;
import org.apache.asterix.metadata.MetadataManager;
import org.apache.asterix.metadata.MetadataTransactionContext;
import org.apache.asterix.metadata.bootstrap.MetadataBuiltinEntities;
import org.apache.asterix.metadata.declared.MetadataProvider;
import org.apache.asterix.metadata.entities.Dataset;
import org.apache.asterix.metadata.utils.SplitsAndConstraintsUtil;
import org.apache.asterix.rebalance.NoOpDatasetRebalanceCallback;
import org.apache.asterix.test.common.TestExecutor;
import org.apache.asterix.testframework.context.TestCaseContext;
import org.apache.asterix.utils.RebalanceUtil;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.api.io.FileSplit;
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

    public static void dropDataset(String dataset) throws Exception {
        TEST_EXECUTOR.executeSqlppUpdateOrDdl("DROP DATASET " + dataset + ";", OUTPUT_FORMAT);
    }

    /**
     * Creates a dataset with multiple fields
     * @param dataset The name of the dataset
     * @param fields The fields of the dataset
     * @param PKName The primary key field name
     * @throws Exception
     */
    public static void createDataset(String dataset, Map<String, String> fields, String PKName) throws Exception {
        StringBuilder stringBuilder = new StringBuilder("");
        fields.forEach((fName, fType) -> stringBuilder.append(fName).append(":").append(fType).append(","));
        stringBuilder.deleteCharAt(stringBuilder.length() - 1);
        TEST_EXECUTOR.executeSqlppUpdateOrDdl("CREATE TYPE dsType AS {" + stringBuilder + "};", OUTPUT_FORMAT);
        TEST_EXECUTOR.executeSqlppUpdateOrDdl("CREATE DATASET " + dataset + "(dsType) PRIMARY KEY " + PKName + ";",
                OUTPUT_FORMAT);
    }

    /**
     * Creates a secondary primary index
     * @param dataset the name of the dataset
     * @param indexName the name of the index
     * @throws Exception
     */
    public static void createPrimaryIndex(String dataset, String indexName) throws Exception {
        TEST_EXECUTOR.executeSqlppUpdateOrDdl("CREATE PRIMARY INDEX " + indexName + " ON " + dataset + ";",
                OUTPUT_FORMAT);
    }

    /**
     * Creates a secondary BTree index
     * @param dataset the name of the dataset
     * @param indexName the name of the index
     * @param SKName the name of the field
     * @throws Exception
     */
    public static void createSecondaryBTreeIndex(String dataset, String indexName, String SKName) throws Exception {
        TEST_EXECUTOR.executeSqlppUpdateOrDdl("CREATE INDEX " + indexName + " ON " + dataset + "(" + SKName + ");",
                OUTPUT_FORMAT);
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
        final InputStream responseStream = TEST_EXECUTOR.executeQueryService(query,
                TEST_EXECUTOR.getEndpoint(Servlets.QUERY_SERVICE), OUTPUT_FORMAT, StandardCharsets.UTF_8);
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

    /**
     * Gets the reference of dataset {@code dataset} from metadata
     *
     * @param integrationUtil
     * @param datasetName
     * @return the dataset reference if found. Otherwise null.
     * @throws AlgebricksException
     * @throws RemoteException
     */
    public static Dataset getDataset(AsterixHyracksIntegrationUtil integrationUtil, String datasetName)
            throws AlgebricksException, RemoteException {
        final ICcApplicationContext appCtx =
                (ICcApplicationContext) integrationUtil.getClusterControllerService().getApplicationContext();
        final MetadataProvider metadataProvider = new MetadataProvider(appCtx, null);
        final MetadataTransactionContext mdTxnCtx = MetadataManager.INSTANCE.beginTransaction();
        metadataProvider.setMetadataTxnContext(mdTxnCtx);
        Dataset dataset;
        try {
            dataset = metadataProvider.findDataset(MetadataBuiltinEntities.DEFAULT_DATAVERSE_NAME, datasetName);
        } finally {
            MetadataManager.INSTANCE.commitTransaction(mdTxnCtx);
            metadataProvider.getLocks().unlock();
        }
        return dataset;
    }

    /**
     * Gets the file splits of {@code dataset}
     *
     * @param integrationUtil
     * @param dataset
     * @return the file splits of the dataset
     * @throws RemoteException
     * @throws AlgebricksException
     */
    public static FileSplit[] getDatasetSplits(AsterixHyracksIntegrationUtil integrationUtil, Dataset dataset)
            throws RemoteException, AlgebricksException {
        final ICcApplicationContext ccAppCtx =
                (ICcApplicationContext) integrationUtil.getClusterControllerService().getApplicationContext();
        final MetadataTransactionContext mdTxnCtx = MetadataManager.INSTANCE.beginTransaction();
        try {
            return SplitsAndConstraintsUtil.getIndexSplits(dataset, dataset.getDatasetName(), mdTxnCtx,
                    ccAppCtx.getClusterStateManager());
        } finally {
            MetadataManager.INSTANCE.commitTransaction(mdTxnCtx);
        }
    }

    public static String getIndexPath(AsterixHyracksIntegrationUtil integrationUtil, Dataset dataset, String nodeId)
            throws Exception {
        final FileSplit[] datasetSplits = TestDataUtil.getDatasetSplits(integrationUtil, dataset);
        final Optional<FileSplit> nodeFileSplit =
                Arrays.stream(datasetSplits).filter(s -> s.getNodeName().equals(nodeId)).findFirst();
        Assert.assertTrue(nodeFileSplit.isPresent());
        return nodeFileSplit.get().getPath();
    }
}
