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
package org.apache.asterix.test.storage;

import org.apache.asterix.api.common.AsterixHyracksIntegrationUtil;
import org.apache.asterix.common.TestDataUtil;
import org.apache.asterix.common.api.IDatasetLifecycleManager;
import org.apache.asterix.common.api.INcApplicationContext;
import org.apache.asterix.common.config.GlobalConfig;
import org.apache.asterix.common.context.DatasetInfo;
import org.apache.asterix.metadata.api.IMetadataIndex;
import org.apache.asterix.metadata.bootstrap.MetadataPrimaryIndexes;
import org.apache.asterix.test.common.TestExecutor;
import org.apache.asterix.testframework.context.TestCaseContext;
import org.apache.hyracks.storage.am.lsm.common.impls.AbstractLSMIndex;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class LsmIndexLifecycleTest {

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
    public void fullMergeTest() throws Exception {
        String datasetName = "ds";
        TestDataUtil.createIdOnlyDataset(datasetName);
        INcApplicationContext appCtx = (INcApplicationContext) (integrationUtil.ncs[0].getApplicationContext());
        IDatasetLifecycleManager dlcm = appCtx.getDatasetLifecycleManager();
        IMetadataIndex dsIdx = MetadataPrimaryIndexes.DATASET_DATASET;
        DatasetInfo datasetInfo = dlcm.getDatasetInfo(dsIdx.getDatasetId().getId());
        // flush to ensure multiple disk components
        dlcm.flushAllDatasets();
        datasetInfo.waitForIO();
        AbstractLSMIndex index = (AbstractLSMIndex) dlcm.getIndex(dsIdx.getDatasetId().getId(), dsIdx.getResourceId());
        Assert.assertTrue(index.getDiskComponents().size() > 1);
        // trigger full merge and ensure we have a single disk component when merge completes
        testExecutor.executeSqlppUpdateOrDdl("COMPACT DATASET Metadata.`Dataset`;",
                TestCaseContext.OutputFormat.CLEAN_JSON);
        datasetInfo.waitForIO();
        Assert.assertTrue(index.getDiskComponents().size() == 1);
    }
}
