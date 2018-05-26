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
package org.apache.asterix.test.ddl;

import java.util.HashMap;
import java.util.Map;

import org.apache.asterix.api.common.AsterixHyracksIntegrationUtil;
import org.apache.asterix.common.TestDataUtil;
import org.apache.asterix.common.config.GlobalConfig;
import org.apache.asterix.common.dataflow.ICcApplicationContext;
import org.apache.asterix.metadata.MetadataManager;
import org.apache.asterix.metadata.MetadataTransactionContext;
import org.apache.asterix.metadata.bootstrap.MetadataBuiltinEntities;
import org.apache.asterix.metadata.declared.MetadataProvider;
import org.apache.asterix.metadata.entities.Dataset;
import org.apache.asterix.metadata.entities.Index;
import org.apache.asterix.metadata.utils.IndexUtil;
import org.apache.hyracks.api.job.JobSpecification;
import org.apache.hyracks.dataflow.std.sort.AbstractSorterOperatorDescriptor;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class SecondaryBTreeOperationsHelperTest {
    protected static final String TEST_CONFIG_FILE_NAME = "src/main/resources/cc.conf";
    private static final AsterixHyracksIntegrationUtil integrationUtil = new AsterixHyracksIntegrationUtil();

    @BeforeClass
    public static void setUp() throws Exception {
        System.setProperty(GlobalConfig.CONFIG_FILE_PROPERTY, TEST_CONFIG_FILE_NAME);
        integrationUtil.init(true, TEST_CONFIG_FILE_NAME);
    }

    @AfterClass
    public static void tearDown() throws Exception {
        integrationUtil.deinit(true);
    }

    /**
     * Tests eliminating the sort operator from the job spec when the index to be created is secondary primary index
     *
     * @throws Exception
     */
    @Test
    public void createPrimaryIndex() throws Exception {
        ICcApplicationContext appCtx =
                (ICcApplicationContext) integrationUtil.getClusterControllerService().getApplicationContext();
        final MetadataProvider metadataProvider = new MetadataProvider(appCtx, null);
        MetadataTransactionContext mdTxn = MetadataManager.INSTANCE.beginTransaction();
        metadataProvider.setMetadataTxnContext(mdTxn);
        try {
            final String datasetName = "DS";
            final String primaryIndexName = "PIxd";
            final String secondaryIndexName = "SIdx";
            final String PKFieldName = "id";
            final String SKFieldName = "name";
            final Map<String, String> fields = new HashMap<>(2);
            fields.put(PKFieldName, "int");
            fields.put(SKFieldName, "string");

            // create the dataset
            TestDataUtil.createDataset(datasetName, fields, PKFieldName);
            final Dataset dataset = metadataProvider.findDataset(null, datasetName);
            Assert.assertNotNull(dataset);

            Index index;
            JobSpecification jobSpecification;

            // create a secondary primary index
            TestDataUtil.createPrimaryIndex(datasetName, primaryIndexName);
            index = metadataProvider.getIndex(MetadataBuiltinEntities.DEFAULT_DATAVERSE_NAME, datasetName,
                    primaryIndexName);
            Assert.assertNotNull(index);
            jobSpecification = IndexUtil.buildSecondaryIndexLoadingJobSpec(dataset, index, metadataProvider, null);
            jobSpecification.getOperatorMap().values().forEach(iOperatorDescriptor -> {
                Assert.assertFalse(iOperatorDescriptor instanceof AbstractSorterOperatorDescriptor);
            });

            // create a normal BTree index
            TestDataUtil.createSecondaryBTreeIndex(datasetName, secondaryIndexName, SKFieldName);
            index = metadataProvider.getIndex(MetadataBuiltinEntities.DEFAULT_DATAVERSE_NAME, datasetName,
                    secondaryIndexName);
            Assert.assertNotNull(index);
            jobSpecification = IndexUtil.buildSecondaryIndexLoadingJobSpec(dataset, index, metadataProvider, null);
            final long numOfSortOperators = jobSpecification.getOperatorMap().values().stream()
                    .filter(op -> op instanceof AbstractSorterOperatorDescriptor).count();
            Assert.assertTrue(numOfSortOperators != 0);
        } finally {
            MetadataManager.INSTANCE.commitTransaction(mdTxn);
            metadataProvider.getLocks().unlock();
        }
    }
}
