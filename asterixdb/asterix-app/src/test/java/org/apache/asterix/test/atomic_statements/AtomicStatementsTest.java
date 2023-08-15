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

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.apache.asterix.api.common.AsterixHyracksIntegrationUtil;
import org.apache.asterix.common.TestDataUtil;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class AtomicStatementsTest {
    private static final String TEST_CONFIG_FILE_NAME = "cc.conf";
    private static final String TEST_CONFIG_PATH = System.getProperty("user.dir") + File.separator + "src"
            + File.separator + "main" + File.separator + "resources";
    private static final String TEST_CONFIG_FILE_PATH = TEST_CONFIG_PATH + File.separator + TEST_CONFIG_FILE_NAME;
    private static final AsterixHyracksIntegrationUtil integrationUtil = new AsterixHyracksIntegrationUtil();

    private static final String DATASET_NAME_PREFIX = "ds_";
    private static final int NUM_DATASETS = 5;
    private static final int BATCH_SIZE = 100;
    private static final int NUM_UPSERTS = 100;
    private static final int NUM_RECOVERIES = 10;

    @Before
    public void setUp() throws Exception {
        integrationUtil.setGracefulShutdown(false);
        integrationUtil.init(true, TEST_CONFIG_FILE_PATH);
        createDatasets();
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
            TestDataUtil.createSecondaryBTreeIndex(datasetName, datasetName + "_sidx", "name:string");
        }
    }

    private Thread insertRecords(String dataset) {
        Thread thread = new Thread(() -> {
            try {
                for (int i = 0; i < NUM_UPSERTS; i++) {
                    TestDataUtil.insertBulkData(dataset, BATCH_SIZE);
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        });
        thread.start();
        return thread;
    }

    @Test
    public void testAtomicityWithFailures() throws Exception {
        for (int i = 0; i <= NUM_RECOVERIES; i++) {
            List<Thread> threads = new ArrayList<>();
            for (int j = 0; j < NUM_DATASETS; j++) {
                threads.add(insertRecords(DATASET_NAME_PREFIX + j));
            }
            Random rnd = new Random();
            Thread.sleep(rnd.nextInt(2000) + 500);
            integrationUtil.deinit(false);
            integrationUtil.init(false, TEST_CONFIG_FILE_PATH);

            for (int j = 0; j < NUM_DATASETS; j++) {
                final long countAfterRecovery = TestDataUtil.getDatasetCount(DATASET_NAME_PREFIX + j);
                Assert.assertEquals(0, countAfterRecovery % BATCH_SIZE);
            }
        }
    }

    @Test
    public void testAtomicityWithoutFailures() throws Exception {
        List<Thread> threads = new ArrayList<>();
        for (int j = 0; j < NUM_DATASETS; j++) {
            threads.add(insertRecords(DATASET_NAME_PREFIX + j));
            threads.add(insertRecords(DATASET_NAME_PREFIX + j));
        }
        for (Thread thread : threads) {
            thread.join();
        }
        for (int j = 0; j < NUM_DATASETS; j++) {
            long count = TestDataUtil.getDatasetCount(DATASET_NAME_PREFIX + j);
            Assert.assertEquals(2 * NUM_UPSERTS * BATCH_SIZE, count);
        }

    }
}
