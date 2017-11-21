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
package org.apache.asterix.test.logging;

import java.io.File;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.apache.asterix.app.bootstrap.TestNodeController;
import org.apache.asterix.app.data.gen.TupleGenerator;
import org.apache.asterix.app.data.gen.TupleGenerator.GenerationFunction;
import org.apache.asterix.common.config.DatasetConfig.DatasetType;
import org.apache.asterix.common.config.TransactionProperties;
import org.apache.asterix.common.configuration.AsterixConfiguration;
import org.apache.asterix.common.configuration.Property;
import org.apache.asterix.common.dataflow.LSMInsertDeleteOperatorNodePushable;
import org.apache.asterix.common.transactions.Checkpoint;
import org.apache.asterix.common.transactions.ICheckpointManager;
import org.apache.asterix.common.transactions.IRecoveryManager;
import org.apache.asterix.common.transactions.ITransactionContext;
import org.apache.asterix.common.transactions.ITransactionManager;
import org.apache.asterix.common.transactions.ITransactionSubsystem;
import org.apache.asterix.common.transactions.TransactionOptions;
import org.apache.asterix.external.util.DataflowUtils;
import org.apache.asterix.file.StorageComponentProvider;
import org.apache.asterix.metadata.entities.Dataset;
import org.apache.asterix.metadata.entities.Index;
import org.apache.asterix.metadata.entities.InternalDatasetDetails;
import org.apache.asterix.metadata.entities.InternalDatasetDetails.PartitioningStrategy;
import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.asterix.om.types.IAType;
import org.apache.asterix.test.common.TestHelper;
import org.apache.asterix.transaction.management.service.logging.LogManager;
import org.apache.asterix.transaction.management.service.recovery.AbstractCheckpointManager;
import org.apache.hyracks.api.comm.VSizeFrame;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.job.JobId;
import org.apache.hyracks.dataflow.common.comm.io.FrameTupleAppender;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;
import org.apache.hyracks.storage.am.lsm.common.impls.NoMergePolicyFactory;
import org.apache.hyracks.util.StorageUtil;
import org.apache.hyracks.util.StorageUtil.StorageUnit;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class CheckpointingTest {

    private static final String DEFAULT_TEST_CONFIG_FILE_NAME = "asterix-build-configuration.xml";
    private static final String TEST_CONFIG_FILE_NAME = "asterix-test-configuration.xml";
    private static final String TEST_CONFIG_PATH =
            System.getProperty("user.dir") + File.separator + "target" + File.separator + "config";
    private static final String TEST_CONFIG_FILE_PATH = TEST_CONFIG_PATH + File.separator + TEST_CONFIG_FILE_NAME;
    private static final IAType[] KEY_TYPES = { BuiltinType.AINT32 };
    private static final ARecordType RECORD_TYPE = new ARecordType("TestRecordType", new String[] { "key", "value" },
            new IAType[] { BuiltinType.AINT32, BuiltinType.AINT64 }, false);
    private static final GenerationFunction[] RECORD_GEN_FUNCTION =
            { GenerationFunction.DETERMINISTIC, GenerationFunction.DETERMINISTIC };
    private static final boolean[] UNIQUE_RECORD_FIELDS = { true, false };
    private static final ARecordType META_TYPE = null;
    private static final GenerationFunction[] META_GEN_FUNCTION = null;
    private static final boolean[] UNIQUE_META_FIELDS = null;
    private static final int[] KEY_INDEXES = { 0 };
    private static final int[] KEY_INDICATOR = { Index.RECORD_INDICATOR };
    private static final List<Integer> KEY_INDICATOR_LIST = Arrays.asList(new Integer[] { Index.RECORD_INDICATOR });
    private static final int DATASET_ID = 101;
    private static final String DATAVERSE_NAME = "TestDV";
    private static final String DATASET_NAME = "TestDS";
    private static final String DATA_TYPE_NAME = "DUMMY";
    private static final String NODE_GROUP_NAME = "DEFAULT";
    private static final int TXN_LOG_PARTITION_SIZE = StorageUtil.getIntSizeInBytes(2, StorageUnit.MEGABYTE);

    @Before
    public void setUp() throws Exception {
        System.out.println("SetUp: ");
        TestHelper.deleteExistingInstanceFiles();
        // Read default test configurations
        AsterixConfiguration ac = TestHelper.getConfigurations(DEFAULT_TEST_CONFIG_FILE_NAME);
        // Set log file size to 2MB
        ac.getProperty().add(new Property(TransactionProperties.TXN_LOG_PARTITIONSIZE_KEY,
                String.valueOf(TXN_LOG_PARTITION_SIZE), ""));
        // Disable checkpointing by making checkpoint thread wait max wait time
        ac.getProperty().add(new Property(TransactionProperties.TXN_LOG_CHECKPOINT_POLLFREQUENCY_KEY,
                String.valueOf(Integer.MAX_VALUE), ""));
        // Write test config file
        TestHelper.writeConfigurations(ac, TEST_CONFIG_FILE_PATH);
    }

    @After
    public void tearDown() throws Exception {
        System.out.println("TearDown");
        TestHelper.deleteExistingInstanceFiles();
    }

    @Test
    public void testDeleteOldLogFiles() {
        try {
            TestNodeController nc = new TestNodeController(new File(TEST_CONFIG_FILE_PATH).getAbsolutePath(), false);
            StorageComponentProvider storageManager = new StorageComponentProvider();
            nc.init();
            List<List<String>> partitioningKeys = new ArrayList<>();
            partitioningKeys.add(Collections.singletonList("key"));
            Dataset dataset = new Dataset(DATAVERSE_NAME, DATASET_NAME, DATAVERSE_NAME, DATA_TYPE_NAME, NODE_GROUP_NAME,
                    NoMergePolicyFactory.NAME, null, new InternalDatasetDetails(null, PartitioningStrategy.HASH,
                            partitioningKeys, null, null, null, false, null, false),
                    null, DatasetType.INTERNAL, DATASET_ID, 0);
            try {
                nc.createPrimaryIndex(dataset, KEY_TYPES, RECORD_TYPE, META_TYPE, null, storageManager, KEY_INDEXES,
                        KEY_INDICATOR_LIST, 0);
                JobId jobId = nc.newJobId();
                IHyracksTaskContext ctx = nc.createTestContext(jobId, 0, false);
                ITransactionContext txnCtx = nc.getTransactionManager().beginTransaction(nc.getTxnJobId(ctx),
                        new TransactionOptions(ITransactionManager.AtomicityLevel.ENTITY_LEVEL));
                // Prepare insert operation
                LSMInsertDeleteOperatorNodePushable insertOp = nc.getInsertPipeline(ctx, dataset, KEY_TYPES,
                        RECORD_TYPE, META_TYPE, null, KEY_INDEXES, KEY_INDICATOR_LIST, storageManager).getLeft();
                insertOp.open();
                TupleGenerator tupleGenerator = new TupleGenerator(RECORD_TYPE, META_TYPE, KEY_INDEXES, KEY_INDICATOR,
                        RECORD_GEN_FUNCTION, UNIQUE_RECORD_FIELDS, META_GEN_FUNCTION, UNIQUE_META_FIELDS);
                VSizeFrame frame = new VSizeFrame(ctx);
                FrameTupleAppender tupleAppender = new FrameTupleAppender(frame);

                IRecoveryManager recoveryManager = nc.getTransactionSubsystem().getRecoveryManager();
                ICheckpointManager checkpointManager = nc.getTransactionSubsystem().getCheckpointManager();
                LogManager logManager = (LogManager) nc.getTransactionSubsystem().getLogManager();
                // Number of log files after node startup should be one
                int numberOfLogFiles = logManager.getLogFileIds().size();
                Assert.assertEquals(1, numberOfLogFiles);

                // Low-water mark LSN
                long lowWaterMarkLSN = recoveryManager.getMinFirstLSN();
                // Low-water mark log file id
                long initialLowWaterMarkFileId = logManager.getLogFileId(lowWaterMarkLSN);
                // Initial Low-water mark should be in the only available log file
                Assert.assertEquals(initialLowWaterMarkFileId, logManager.getLogFileIds().get(0).longValue());

                // Insert records until a new log file is created
                while (logManager.getLogFileIds().size() == 1) {
                    ITupleReference tuple = tupleGenerator.next();
                    DataflowUtils.addTupleToFrame(tupleAppender, tuple, insertOp);
                }

                // Check if the new low-water mark is still in the initial low-water mark log file
                lowWaterMarkLSN = recoveryManager.getMinFirstLSN();
                long currentLowWaterMarkLogFileId = logManager.getLogFileId(lowWaterMarkLSN);

                if (currentLowWaterMarkLogFileId == initialLowWaterMarkFileId) {
                    /*
                     * Make sure checkpoint will not delete the initial log file since
                     * the low-water mark is still in it (i.e. it is still required for
                     * recovery)
                     */
                    int numberOfLogFilesBeforeCheckpoint = logManager.getLogFileIds().size();
                    checkpointManager.tryCheckpoint(logManager.getAppendLSN());
                    int numberOfLogFilesAfterCheckpoint = logManager.getLogFileIds().size();
                    Assert.assertEquals(numberOfLogFilesBeforeCheckpoint, numberOfLogFilesAfterCheckpoint);

                    /*
                     * Insert records until the low-water mark is not in the initialLowWaterMarkFileId
                     * either because of the asynchronous flush caused by the previous checkpoint or a flush
                     * due to the dataset memory budget getting full.
                     */
                    while (currentLowWaterMarkLogFileId == initialLowWaterMarkFileId) {
                        ITupleReference tuple = tupleGenerator.next();
                        DataflowUtils.addTupleToFrame(tupleAppender, tuple, insertOp);
                        lowWaterMarkLSN = recoveryManager.getMinFirstLSN();
                        currentLowWaterMarkLogFileId = logManager.getLogFileId(lowWaterMarkLSN);
                    }
                }

                /*
                 * At this point, the low-water mark is not in the initialLowWaterMarkFileId, so
                 * a checkpoint should delete it.
                 */
                checkpointManager.tryCheckpoint(recoveryManager.getMinFirstLSN());

                // Validate initialLowWaterMarkFileId was deleted
                for (Long fileId : logManager.getLogFileIds()) {
                    Assert.assertNotEquals(initialLowWaterMarkFileId, fileId.longValue());
                }

                if (tupleAppender.getTupleCount() > 0) {
                    tupleAppender.write(insertOp, true);
                }
                insertOp.close();
                nc.getTransactionManager().commitTransaction(txnCtx.getTxnId());
            } finally {
                nc.deInit();
            }
        } catch (Throwable e) {
            e.printStackTrace();
            Assert.fail(e.getMessage());
        }
    }

    @Test
    public void testCorruptedCheckpointFiles() {
        try {
            TestNodeController nc = new TestNodeController(new File(TEST_CONFIG_FILE_PATH).getAbsolutePath(), false);
            nc.init();
            try {
                final ITransactionSubsystem txnSubsystem = nc.getTransactionSubsystem();
                final AbstractCheckpointManager checkpointManager =
                        (AbstractCheckpointManager) txnSubsystem.getCheckpointManager();
                // Make a checkpoint with the current minFirstLSN
                final long minFirstLSN = txnSubsystem.getRecoveryManager().getMinFirstLSN();
                checkpointManager.tryCheckpoint(minFirstLSN);
                // Get the just created checkpoint
                final Checkpoint validCheckpoint = checkpointManager.getLatest();
                // Make sure the valid checkout wouldn't force full recovery
                Assert.assertTrue(validCheckpoint.getMinMCTFirstLsn() >= minFirstLSN);
                // Add a corrupted (empty) checkpoint file with a timestamp > than current checkpoint
                Path corruptedCheckpointPath = checkpointManager.getCheckpointPath(validCheckpoint.getTimeStamp() + 1);
                File corruptedCheckpoint = corruptedCheckpointPath.toFile();
                corruptedCheckpoint.createNewFile();
                // Make sure the corrupted checkpoint file was created
                Assert.assertTrue(corruptedCheckpoint.exists());
                // Try to get the latest checkpoint again
                Checkpoint cpAfterCorruption = checkpointManager.getLatest();
                // Make sure the valid checkpoint was returned
                Assert.assertEquals(validCheckpoint.getTimeStamp(), cpAfterCorruption.getTimeStamp());
                // Make sure the corrupted checkpoint file was deleted
                Assert.assertFalse(corruptedCheckpoint.exists());
                // Corrupt the valid checkpoint by replacing its content
                final Path validCheckpointPath = checkpointManager.getCheckpointPath(validCheckpoint.getTimeStamp());
                File validCheckpointFile = validCheckpointPath.toFile();
                Assert.assertTrue(validCheckpointFile.exists());
                // Delete the valid checkpoint file and create it as an empty file
                validCheckpointFile.delete();
                validCheckpointFile.createNewFile();
                // Make sure the returned checkpoint (the forged checkpoint) will enforce full recovery
                Checkpoint forgedCheckpoint = checkpointManager.getLatest();
                Assert.assertTrue(forgedCheckpoint.getMinMCTFirstLsn() < minFirstLSN);
                // Make sure the forged checkpoint recovery will start from the first available log
                final long readableSmallestLSN = txnSubsystem.getLogManager().getReadableSmallestLSN();
                Assert.assertTrue(forgedCheckpoint.getMinMCTFirstLsn() <= readableSmallestLSN);
            } finally {
                nc.deInit();
            }
        } catch (Throwable e) {
            e.printStackTrace();
            Assert.fail(e.getMessage());
        }
    }
}