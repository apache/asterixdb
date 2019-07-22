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

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.spy;

import java.io.File;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.apache.asterix.app.bootstrap.TestNodeController;
import org.apache.asterix.app.data.gen.RecordTupleGenerator;
import org.apache.asterix.app.data.gen.RecordTupleGenerator.GenerationFunction;
import org.apache.asterix.app.nc.RecoveryManager;
import org.apache.asterix.common.exceptions.ACIDException;
import org.apache.asterix.common.transactions.Checkpoint;
import org.apache.asterix.common.transactions.ICheckpointManager;
import org.apache.asterix.common.transactions.ITransactionContext;
import org.apache.asterix.common.transactions.ITransactionManager;
import org.apache.asterix.common.transactions.ITransactionSubsystem;
import org.apache.asterix.common.transactions.LogRecord;
import org.apache.asterix.common.transactions.TransactionOptions;
import org.apache.asterix.common.transactions.TxnId;
import org.apache.asterix.common.utils.TransactionUtil;
import org.apache.asterix.external.util.DataflowUtils;
import org.apache.asterix.file.StorageComponentProvider;
import org.apache.asterix.metadata.entities.Index;
import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.asterix.om.types.IAType;
import org.apache.asterix.runtime.operators.LSMPrimaryInsertOperatorNodePushable;
import org.apache.asterix.test.common.TestHelper;
import org.apache.asterix.test.dataflow.StorageTestUtils;
import org.apache.asterix.transaction.management.service.logging.LogManager;
import org.apache.asterix.transaction.management.service.recovery.AbstractCheckpointManager;
import org.apache.asterix.transaction.management.service.transaction.TransactionManager;
import org.apache.hyracks.api.comm.VSizeFrame;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.job.JobId;
import org.apache.hyracks.dataflow.common.comm.io.FrameTupleAppender;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class CheckpointingTest {

    private static final String TEST_CONFIG_FILE_NAME = "cc-small-txn-log-partition.conf";
    private static final String TEST_CONFIG_PATH = System.getProperty("user.dir") + File.separator + "src"
            + File.separator + "test" + File.separator + "resources";
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
    private volatile boolean threadException = false;
    private Throwable exception = null;

    @Before
    public void setUp() throws Exception {
        System.out.println("SetUp: ");
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
            try {
                nc.createPrimaryIndex(StorageTestUtils.DATASET, KEY_TYPES, RECORD_TYPE, META_TYPE, null, storageManager,
                        KEY_INDEXES, KEY_INDICATOR_LIST, 0);
                JobId jobId = nc.newJobId();
                IHyracksTaskContext ctx = nc.createTestContext(jobId, 0, false);
                ITransactionContext txnCtx = nc.getTransactionManager().beginTransaction(nc.getTxnJobId(ctx),
                        new TransactionOptions(ITransactionManager.AtomicityLevel.ENTITY_LEVEL));
                // Prepare insert operation
                LSMPrimaryInsertOperatorNodePushable insertOp =
                        nc.getInsertPipeline(ctx, StorageTestUtils.DATASET, KEY_TYPES, RECORD_TYPE, META_TYPE, null,
                                KEY_INDEXES, KEY_INDICATOR_LIST, storageManager, null, null).getLeft();
                insertOp.open();
                RecordTupleGenerator tupleGenerator =
                        new RecordTupleGenerator(RECORD_TYPE, META_TYPE, KEY_INDEXES, KEY_INDICATOR,
                                RECORD_GEN_FUNCTION, UNIQUE_RECORD_FIELDS, META_GEN_FUNCTION, UNIQUE_META_FIELDS);
                VSizeFrame frame = new VSizeFrame(ctx);
                FrameTupleAppender tupleAppender = new FrameTupleAppender(frame);

                RecoveryManager recoveryManager = (RecoveryManager) nc.getTransactionSubsystem().getRecoveryManager();
                ICheckpointManager checkpointManager = nc.getTransactionSubsystem().getCheckpointManager();
                LogManager logManager = (LogManager) nc.getTransactionSubsystem().getLogManager();
                // Number of log files after node startup should be one
                int numberOfLogFiles = logManager.getOrderedLogFileIds().size();
                Assert.assertEquals(1, numberOfLogFiles);

                // Low-water mark LSN
                long lowWaterMarkLSN = recoveryManager.getMinFirstLSN();
                // Low-water mark log file id
                long initialLowWaterMarkFileId = logManager.getLogFileId(lowWaterMarkLSN);
                // Initial Low-water mark should be in the only available log file
                Assert.assertEquals(initialLowWaterMarkFileId, logManager.getOrderedLogFileIds().get(0).longValue());

                // Insert records until a new log file is created
                while (logManager.getOrderedLogFileIds().size() == 1) {
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
                    int numberOfLogFilesBeforeCheckpoint = logManager.getOrderedLogFileIds().size();
                    checkpointManager.tryCheckpoint(logManager.getAppendLSN());
                    int numberOfLogFilesAfterCheckpoint = logManager.getOrderedLogFileIds().size();
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
                 * a checkpoint should delete it. We will also start a second
                  * job to ensure that the checkpointing coexists peacefully
                  * with other concurrent readers of the log that request
                  * deletions to be witheld
                 */

                JobId jobId2 = nc.newJobId();
                IHyracksTaskContext ctx2 = nc.createTestContext(jobId2, 0, false);
                nc.getTransactionManager().beginTransaction(nc.getTxnJobId(ctx2),
                        new TransactionOptions(ITransactionManager.AtomicityLevel.ENTITY_LEVEL));
                // Prepare insert operation
                LSMPrimaryInsertOperatorNodePushable insertOp2 =
                        nc.getInsertPipeline(ctx2, StorageTestUtils.DATASET, KEY_TYPES, RECORD_TYPE, META_TYPE, null,
                                KEY_INDEXES, KEY_INDICATOR_LIST, storageManager, null, null).getLeft();
                insertOp2.open();
                VSizeFrame frame2 = new VSizeFrame(ctx2);
                FrameTupleAppender tupleAppender2 = new FrameTupleAppender(frame2);
                for (int i = 0; i < 4; i++) {
                    long lastCkpoint = recoveryManager.getMinFirstLSN();
                    long lastFileId = logManager.getLogFileId(lastCkpoint);

                    checkpointManager.tryCheckpoint(lowWaterMarkLSN);
                    // Validate initialLowWaterMarkFileId was deleted
                    for (Long fileId : logManager.getOrderedLogFileIds()) {
                        Assert.assertNotEquals(initialLowWaterMarkFileId, fileId.longValue());
                    }

                    while (currentLowWaterMarkLogFileId == lastFileId) {
                        ITupleReference tuple = tupleGenerator.next();
                        DataflowUtils.addTupleToFrame(tupleAppender2, tuple, insertOp2);
                        lowWaterMarkLSN = recoveryManager.getMinFirstLSN();
                        currentLowWaterMarkLogFileId = logManager.getLogFileId(lowWaterMarkLSN);
                    }
                }
                Thread.UncaughtExceptionHandler h = new Thread.UncaughtExceptionHandler() {
                    @Override
                    public void uncaughtException(Thread th, Throwable ex) {
                        threadException = true;
                        exception = ex;
                    }
                };

                Thread t = new Thread(() -> {
                    TransactionManager spyTxnMgr = spy((TransactionManager) nc.getTransactionManager());
                    doAnswer(i -> {
                        stallAbortTxn(Thread.currentThread(), txnCtx, nc.getTransactionSubsystem(),
                                (TxnId) i.getArguments()[0]);
                        return null;
                    }).when(spyTxnMgr).abortTransaction(any(TxnId.class));

                    spyTxnMgr.abortTransaction(txnCtx.getTxnId());
                });
                t.setUncaughtExceptionHandler(h);
                synchronized (t) {
                    t.start();
                    t.wait();
                }
                long lockedLSN = recoveryManager.getMinFirstLSN();
                checkpointManager.tryCheckpoint(lockedLSN);
                synchronized (t) {
                    t.notifyAll();
                }
                t.join();
                if (threadException) {
                    throw exception;
                }
            } finally {
                nc.deInit();
            }
        } catch (Throwable e) {
            e.printStackTrace();
            Assert.fail(e.getMessage());
        }
    }

    private void stallAbortTxn(Thread t, ITransactionContext txnCtx, ITransactionSubsystem txnSubsystem, TxnId txnId)
            throws InterruptedException, HyracksDataException {

        try {
            if (txnCtx.isWriteTxn()) {
                LogRecord logRecord = new LogRecord();
                TransactionUtil.formJobTerminateLogRecord(txnCtx, logRecord, false);
                txnSubsystem.getLogManager().log(logRecord);
                txnSubsystem.getCheckpointManager().secure(txnId);
                synchronized (t) {
                    t.notifyAll();
                    t.wait();
                }
                txnSubsystem.getRecoveryManager().rollbackTransaction(txnCtx);
                txnCtx.setTxnState(ITransactionManager.ABORTED);
            }
        } catch (ACIDException | HyracksDataException e) {
            String msg = "Could not complete rollback! System is in an inconsistent state";
            throw new ACIDException(msg, e);
        } finally {
            txnCtx.complete();
            txnSubsystem.getLockManager().releaseLocks(txnCtx);
            txnSubsystem.getCheckpointManager().completed(txnId);
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
                Path corruptedCheckpointPath = checkpointManager.getCheckpointPath(validCheckpoint.getId() + 1);
                File corruptedCheckpoint = corruptedCheckpointPath.toFile();
                corruptedCheckpoint.createNewFile();
                // Make sure the corrupted checkpoint file was created
                Assert.assertTrue(corruptedCheckpoint.exists());
                // Try to get the latest checkpoint again
                Checkpoint cpAfterCorruption = checkpointManager.getLatest();
                // Make sure the valid checkpoint was returned
                Assert.assertEquals(validCheckpoint.getId(), cpAfterCorruption.getId());
                // Make sure the corrupted checkpoint file was not deleted
                Assert.assertTrue(corruptedCheckpoint.exists());
                // Corrupt the valid checkpoint by replacing its content
                final Path validCheckpointPath = checkpointManager.getCheckpointPath(validCheckpoint.getId());
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
                // another call should still give us the forged checkpoint and the corrupted one should still be there
                forgedCheckpoint = checkpointManager.getLatest();
                Assert.assertTrue(forgedCheckpoint.getMinMCTFirstLsn() < minFirstLSN);
                Assert.assertTrue(corruptedCheckpoint.exists());
                // do a succesful checkpoint and ensure now the corrupted file was deleted
                checkpointManager.doSharpCheckpoint();
                Assert.assertFalse(corruptedCheckpoint.exists());
            } finally {
                nc.deInit();
            }
        } catch (Throwable e) {
            e.printStackTrace();
            Assert.fail(e.getMessage());
        }
    }
}
