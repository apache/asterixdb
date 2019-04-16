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
package org.apache.asterix.test.runtime;

import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.asterix.api.common.AsterixHyracksIntegrationUtil;
import org.apache.asterix.common.TestDataUtil;
import org.apache.asterix.common.config.GlobalConfig;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class DdlStressTest {

    protected static final String TEST_CONFIG_FILE_NAME = "src/main/resources/cc.conf";
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
    public void concurrentDdlStressTest() throws Exception {
        final String datasetNamePrefix = "ds";
        AtomicBoolean failed = new AtomicBoolean(false);
        Thread[] ddlThreads = new Thread[5];
        for (int i = 0; i < ddlThreads.length; i++) {
            String datasetName = datasetNamePrefix + i;
            ddlThreads[i] = new Thread(() -> {
                try {
                    createUpsertDropDataset(datasetName, 50);
                } catch (Exception e) {
                    failed.set(true);
                    e.printStackTrace();
                }
            });
        }
        for (Thread ddlThread : ddlThreads) {
            ddlThread.start();
        }
        for (Thread ddlThread : ddlThreads) {
            ddlThread.join();
        }
        Assert.assertFalse("unexpected ddl failure", failed.get());
    }

    private void createUpsertDropDataset(String datasetName, int count) throws Exception {
        for (int i = 0; i < count; i++) {
            TestDataUtil.createIdOnlyDataset(datasetName);
            TestDataUtil.createPrimaryIndex(datasetName, "pIdx_" + datasetName);
            TestDataUtil.upsertData(datasetName, 5);
            TestDataUtil.dropDataset(datasetName);
        }
    }
}
