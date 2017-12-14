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

import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.function.Function;

import org.apache.asterix.api.common.AsterixHyracksIntegrationUtil;
import org.apache.asterix.common.TestDataUtil;
import org.apache.asterix.common.api.INcApplicationContext;
import org.apache.asterix.common.config.GlobalConfig;
import org.apache.asterix.common.storage.IndexPathElements;
import org.apache.asterix.common.transactions.Checkpoint;
import org.apache.asterix.common.utils.StorageConstants;
import org.apache.asterix.common.utils.StoragePathUtil;
import org.apache.asterix.transaction.management.service.recovery.AbstractCheckpointManager;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class MigrateStorageResourcesTaskTest {

    private static final String DEFAULT_TEST_CONFIG_FILE_NAME = "src/main/resources/cc.conf";
    private static final AsterixHyracksIntegrationUtil integrationUtil = new AsterixHyracksIntegrationUtil();

    @Before
    public void setUp() throws Exception {
    }

    @After
    public void tearDown() throws Exception {
        integrationUtil.deinit(true);
    }

    @Test
    public void storageStructureMigration() throws Exception {
        Function<IndexPathElements, String> legacyIndexPathProvider = (
                pathElements) -> (pathElements.getRebalanceCount().equals("0") ? ""
                        : pathElements.getRebalanceCount() + File.separator) + pathElements.getDatasetName()
                        + StorageConstants.LEGACY_DATASET_INDEX_NAME_SEPARATOR + pathElements.getIndexName();
        StoragePathUtil.setIndexPathProvider(legacyIndexPathProvider);
        integrationUtil.init(true, DEFAULT_TEST_CONFIG_FILE_NAME);
        // create dataset and insert data using legacy structure
        String datasetName = "ds";
        TestDataUtil.createIdOnlyDataset(datasetName);
        TestDataUtil.upsertData(datasetName, 100);
        final long countBeforeMigration = TestDataUtil.getDatasetCount(datasetName);
        // stop NCs
        integrationUtil.deinit(false);
        // forge a checkpoint with old version to force migration to new storage structure on all ncs
        final INcApplicationContext nc1AppCtx = (INcApplicationContext) integrationUtil.ncs[0].getApplicationContext();
        final AbstractCheckpointManager nc1CheckpointManager = (AbstractCheckpointManager) nc1AppCtx
                .getTransactionSubsystem().getCheckpointManager();
        forgeOldVersionCheckpoint(nc1CheckpointManager);
        final INcApplicationContext nc2AppCtx = (INcApplicationContext) integrationUtil.ncs[1].getApplicationContext();
        final AbstractCheckpointManager nc2CheckpointManager = (AbstractCheckpointManager) nc2AppCtx
                .getTransactionSubsystem().getCheckpointManager();
        forgeOldVersionCheckpoint(nc2CheckpointManager);

        // remove the legacy path provider to use the new default structure
        StoragePathUtil.setIndexPathProvider(null);
        // start the NCs to do the migration
        integrationUtil.init(false, DEFAULT_TEST_CONFIG_FILE_NAME);
        final long countAfterMigration = TestDataUtil.getDatasetCount(datasetName);
        // ensure data migrated to new structure without issues
        Assert.assertEquals(countBeforeMigration, countAfterMigration);
    }

    private void forgeOldVersionCheckpoint(AbstractCheckpointManager manger) throws HyracksDataException {
        Checkpoint cp = new Checkpoint(-1, -1, 0, System.currentTimeMillis(), true,
                StorageConstants.REBALANCE_STORAGE_VERSION - 1);
        Path path = manger.getCheckpointPath(cp.getTimeStamp());
        // Write checkpoint file to disk
        try (BufferedWriter writer = Files.newBufferedWriter(path)) {
            writer.write(cp.asJson());
            writer.flush();
        } catch (IOException e) {
            throw HyracksDataException.create(e);
        }
    }
}
