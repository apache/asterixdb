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

import org.apache.asterix.api.common.AsterixHyracksIntegrationUtil;
import org.apache.asterix.common.TestDataUtil;
import org.apache.asterix.common.config.StorageProperties;
import org.apache.asterix.metadata.bootstrap.MetadataBuiltinEntities;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.fasterxml.jackson.databind.ObjectMapper;

public class RecoveryManagerTest {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private static final String TEST_CONFIG_FILE_NAME = "cc.conf";
    private static final String TEST_CONFIG_PATH = System.getProperty("user.dir") + File.separator + "src"
            + File.separator + "main" + File.separator + "resources";
    private static final String TEST_CONFIG_FILE_PATH = TEST_CONFIG_PATH + File.separator + TEST_CONFIG_FILE_NAME;
    private static final AsterixHyracksIntegrationUtil integrationUtil = new AsterixHyracksIntegrationUtil();

    @Before
    public void setUp() throws Exception {
        integrationUtil.addOption(StorageProperties.Option.STORAGE_MAX_ACTIVE_WRITABLE_DATASETS, 20);
        integrationUtil.setGracefulShutdown(false);
        integrationUtil.init(true, TEST_CONFIG_FILE_PATH);
    }

    @After
    public void tearDown() throws Exception {
        integrationUtil.deinit(true);
    }

    @Test
    public void multiDatasetRecovery() throws Exception {
        String datasetNamePrefix = "ds_";
        int numDatasets = 50;
        String datasetName = null;
        for (int i = 1; i <= numDatasets; i++) {
            datasetName = datasetNamePrefix + i;
            TestDataUtil.createIdOnlyDataset(datasetName);
            TestDataUtil.upsertData(datasetName, 10);
        }
        final long countBeforeFirstRecovery = TestDataUtil.getDatasetCount(datasetName);
        // do ungraceful shutdown to enforce recovery
        integrationUtil.deinit(false);
        integrationUtil.init(false, TEST_CONFIG_FILE_PATH);
        final long countAfterFirstRecovery = TestDataUtil.getDatasetCount(datasetName);
        Assert.assertEquals(countBeforeFirstRecovery, countAfterFirstRecovery);
        // create more datasets after recovery
        numDatasets = 100;
        for (int i = 51; i <= numDatasets; i++) {
            datasetName = datasetNamePrefix + i;
            TestDataUtil.createIdOnlyDataset(datasetName);
            TestDataUtil.upsertData(datasetName, 1);
        }
        final long countBeforeSecondRecovery = TestDataUtil.getDatasetCount(datasetName);
        // do ungraceful shutdown to enforce recovery again
        integrationUtil.deinit(false);
        integrationUtil.init(false, TEST_CONFIG_FILE_PATH);
        final long countAfterSecondRecovery = TestDataUtil.getDatasetCount(datasetName);
        Assert.assertEquals(countBeforeSecondRecovery, countAfterSecondRecovery);
    }

    @Test
    public void reoveryAfterRebalance() throws Exception {
        String datasetName = "ds";
        TestDataUtil.createIdOnlyDataset(datasetName);
        TestDataUtil.upsertData(datasetName, 10);
        final long countBeforeRebalance = TestDataUtil.getDatasetCount(datasetName);
        // rebalance dataset to single nc
        TestDataUtil.rebalanceDataset(integrationUtil, MetadataBuiltinEntities.DEFAULT_DATAVERSE.getDataverseName(),
                datasetName, new String[] { "asterix_nc2" });
        // check data after rebalance
        final long countAfterRebalance = TestDataUtil.getDatasetCount(datasetName);
        Assert.assertEquals(countBeforeRebalance, countAfterRebalance);
        // insert data after rebalance
        TestDataUtil.upsertData(datasetName, 20);
        final long countBeforeRecovery = TestDataUtil.getDatasetCount(datasetName);
        // do ungraceful shutdown to enforce recovery
        integrationUtil.deinit(false);
        integrationUtil.init(false, TEST_CONFIG_FILE_PATH);
        final long countAfterRecovery = TestDataUtil.getDatasetCount(datasetName);
        Assert.assertEquals(countBeforeRecovery, countAfterRecovery);
    }
}
