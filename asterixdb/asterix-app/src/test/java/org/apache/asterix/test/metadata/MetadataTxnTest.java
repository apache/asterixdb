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

import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.asterix.api.common.AsterixHyracksIntegrationUtil;
import org.apache.asterix.common.config.GlobalConfig;
import org.apache.asterix.common.dataflow.ICcApplicationContext;
import org.apache.asterix.metadata.MetadataManager;
import org.apache.asterix.metadata.MetadataTransactionContext;
import org.apache.asterix.metadata.bootstrap.MetadataBuiltinEntities;
import org.apache.asterix.metadata.declared.MetadataProvider;
import org.apache.asterix.metadata.entities.Dataset;
import org.apache.asterix.metadata.entities.NodeGroup;
import org.apache.asterix.metadata.utils.DatasetUtil;
import org.apache.asterix.test.common.TestExecutor;
import org.apache.asterix.testframework.context.TestCaseContext;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class MetadataTxnTest {

    private static final String TEST_CONFIG_FILE_NAME = "asterix-build-configuration.xml";
    private static final TestExecutor testExecutor = new TestExecutor();
    private static final AsterixHyracksIntegrationUtil integrationUtil = new AsterixHyracksIntegrationUtil();

    @Before
    public void setUp() throws Exception {
        System.setProperty(GlobalConfig.CONFIG_FILE_PROPERTY, TEST_CONFIG_FILE_NAME);
        integrationUtil.init(true);
    }

    @After
    public void tearDown() throws Exception {
        integrationUtil.deinit(true);
    }

    @Test
    public void abortMetadataTxn() throws Exception {
        ICcApplicationContext appCtx =
                (ICcApplicationContext) integrationUtil.getClusterControllerService().getApplicationContext();
        final MetadataProvider metadataProvider = new MetadataProvider(appCtx, null);
        final MetadataTransactionContext mdTxn = MetadataManager.INSTANCE.beginTransaction();
        metadataProvider.setMetadataTxnContext(mdTxn);
        final String nodeGroupName = "ng";
        try {
            final List<String> ngNodes = Arrays.asList("asterix_nc1");
            MetadataManager.INSTANCE.addNodegroup(mdTxn, new NodeGroup(nodeGroupName, ngNodes));
            MetadataManager.INSTANCE.abortTransaction(mdTxn);
        } finally {
            metadataProvider.getLocks().unlock();
        }

        // ensure that the node group was not added
        final MetadataTransactionContext readMdTxn = MetadataManager.INSTANCE.beginTransaction();
        try {
            final NodeGroup nodegroup = MetadataManager.INSTANCE.getNodegroup(readMdTxn, nodeGroupName);
            if (nodegroup != null) {
                throw new AssertionError("nodegroup was found after metadata txn was aborted");
            }
        } finally {
            MetadataManager.INSTANCE.commitTransaction(readMdTxn);
        }
    }

    @Test
    public void rebalanceFailureMetadataTxn() throws Exception {
        ICcApplicationContext appCtx =
                (ICcApplicationContext) integrationUtil.getClusterControllerService().getApplicationContext();
        String nodeGroup = "ng";
        String datasetName = "dataset1";
        final TestCaseContext.OutputFormat format = TestCaseContext.OutputFormat.CLEAN_JSON;
        // create original node group
        testExecutor.executeSqlppUpdateOrDdl("CREATE nodegroup " + nodeGroup + " on asterix_nc2;", format);
        // create original dataset
        testExecutor.executeSqlppUpdateOrDdl("CREATE TYPE KeyType AS { id: int };", format);
        testExecutor.executeSqlppUpdateOrDdl(
                "CREATE DATASET " + datasetName + "(KeyType) PRIMARY KEY id on " + nodeGroup + ";", format);
        // find source dataset
        Dataset sourceDataset;
        MetadataProvider metadataProvider = new MetadataProvider(appCtx, null);
        final MetadataTransactionContext mdTxnCtx = MetadataManager.INSTANCE.beginTransaction();
        metadataProvider.setMetadataTxnContext(mdTxnCtx);
        try {
            sourceDataset = metadataProvider.findDataset(MetadataBuiltinEntities.DEFAULT_DATAVERSE_NAME, datasetName);
            MetadataManager.INSTANCE.commitTransaction(mdTxnCtx);
        } finally {
            metadataProvider.getLocks().unlock();
        }

        // create rebalance metadata provider and metadata txn
        metadataProvider = new MetadataProvider(appCtx, null);
        final MetadataTransactionContext rebalanceTxn = MetadataManager.INSTANCE.beginTransaction();
        metadataProvider.setMetadataTxnContext(rebalanceTxn);
        try {
            final Set<String> rebalanceToNodes = Stream.of("asterix_nc1").collect(Collectors.toSet());
            DatasetUtil.createNodeGroupForNewDataset(sourceDataset.getDataverseName(), sourceDataset.getDatasetName(),
                    sourceDataset.getRebalanceCount() + 1, rebalanceToNodes, metadataProvider);
            // rebalance failed --> abort txn
            MetadataManager.INSTANCE.abortTransaction(rebalanceTxn);
        } finally {
            metadataProvider.getLocks().unlock();
        }
        // ensure original dataset can be dropped after rebalance failure
        testExecutor.executeSqlppUpdateOrDdl("DROP DATASET " + datasetName + ";", format);

        // ensure the node group was dropped too since its only dataset was dropped
        final MetadataTransactionContext readMdTxn = MetadataManager.INSTANCE.beginTransaction();
        try {
            final NodeGroup nodegroup = MetadataManager.INSTANCE.getNodegroup(readMdTxn, nodeGroup);
            if (nodegroup != null) {
                throw new AssertionError("nodegroup was found after its only dataset was dropped");
            }
        } finally {
            MetadataManager.INSTANCE.commitTransaction(readMdTxn);
        }
    }
}