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

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.asterix.api.common.AsterixHyracksIntegrationUtil;
import org.apache.asterix.common.TestDataUtil;
import org.apache.asterix.common.api.IDatasetLifecycleManager;
import org.apache.asterix.common.api.INcApplicationContext;
import org.apache.asterix.common.config.GlobalConfig;
import org.apache.asterix.common.dataflow.DatasetLocalResource;
import org.apache.asterix.common.exceptions.ACIDException;
import org.apache.asterix.common.exceptions.ExceptionUtils;
import org.apache.asterix.common.transactions.DatasetId;
import org.apache.asterix.common.transactions.ILockManager;
import org.apache.asterix.common.transactions.ILogManager;
import org.apache.asterix.common.transactions.ITransactionContext;
import org.apache.asterix.common.transactions.ITransactionManager;
import org.apache.asterix.common.transactions.LogRecord;
import org.apache.asterix.common.transactions.LogType;
import org.apache.asterix.common.transactions.TransactionOptions;
import org.apache.asterix.common.transactions.TxnId;
import org.apache.asterix.common.utils.TransactionUtil;
import org.apache.asterix.metadata.entities.Dataset;
import org.apache.asterix.test.common.TestTupleReference;
import org.apache.asterix.transaction.management.service.logging.LogManager;
import org.apache.asterix.transaction.management.service.transaction.TransactionManagementConstants.LockManagerConstants;
import org.apache.hyracks.api.io.FileSplit;
import org.apache.hyracks.storage.am.common.impls.NoOpOperationCallback;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIndex;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class LogManagerTest {

    protected static final String TEST_CONFIG_FILE_NAME = "src/main/resources/cc.conf";
    private static final AsterixHyracksIntegrationUtil integrationUtil = new AsterixHyracksIntegrationUtil();
    private static final String PREPARE_NEXT_LOG_FILE_METHOD = "prepareNextLogFile";

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
    public void interruptedLogPageSwitch() throws Exception {
        final INcApplicationContext ncAppCtx = (INcApplicationContext) integrationUtil.ncs[0].getApplicationContext();
        final String nodeId = ncAppCtx.getServiceContext().getNodeId();

        final String datasetName = "ds";
        TestDataUtil.createIdOnlyDataset(datasetName);
        final Dataset dataset = TestDataUtil.getDataset(integrationUtil, datasetName);
        final String indexPath = TestDataUtil.getIndexPath(integrationUtil, dataset, nodeId);
        final IDatasetLifecycleManager dclm = ncAppCtx.getDatasetLifecycleManager();
        dclm.open(indexPath);
        final ILSMIndex index = (ILSMIndex) dclm.get(indexPath);
        final long resourceId = ncAppCtx.getLocalResourceRepository().get(indexPath).getId();
        final DatasetLocalResource datasetLocalResource =
                (DatasetLocalResource) ncAppCtx.getLocalResourceRepository().get(indexPath).getResource();
        final ITransactionContext txnCtx = beingTransaction(ncAppCtx, index, resourceId);
        final ILogManager logManager = ncAppCtx.getTransactionSubsystem().getLogManager();
        final ILockManager lockManager = ncAppCtx.getTransactionSubsystem().getLockManager();
        final DatasetId datasetId = new DatasetId(dataset.getDatasetId());
        final int[] pkFields = dataset.getPrimaryBloomFilterFields();
        final int fieldsLength = pkFields.length;
        final TestTupleReference tuple = new TestTupleReference(fieldsLength);
        tuple.getFields()[0].getDataOutput().write(1);
        final int partition = datasetLocalResource.getPartition();

        // ensure interrupted thread will be interrupted on allocating next log page
        final AtomicBoolean interrupted = new AtomicBoolean(false);
        Thread interruptedTransactor = new Thread(() -> {
            Thread.currentThread().interrupt();
            try {
                for (int i = 0; i < 10000; i++) {
                    lockManager.lock(datasetId, i, LockManagerConstants.LockMode.S, txnCtx);
                    LogRecord logRecord = new LogRecord();
                    TransactionUtil.formEntityCommitLogRecord(logRecord, txnCtx, datasetId.getId(), i, tuple, pkFields,
                            partition, LogType.ENTITY_COMMIT);
                    logManager.log(logRecord);
                }
            } catch (ACIDException e) {
                Throwable rootCause = ExceptionUtils.getRootCause(e);
                if (rootCause instanceof java.lang.InterruptedException) {
                    interrupted.set(true);
                }
            }
        });
        interruptedTransactor.start();
        interruptedTransactor.join();
        Assert.assertTrue(interrupted.get());

        // ensure next thread will be able to allocate next page
        final AtomicInteger failCount = new AtomicInteger(0);
        Thread transactor = new Thread(() -> {
            try {
                for (int i = 0; i < 10000; i++) {
                    lockManager.lock(datasetId, i, LockManagerConstants.LockMode.S, txnCtx);
                    LogRecord logRecord = new LogRecord();
                    TransactionUtil.formEntityCommitLogRecord(logRecord, txnCtx, datasetId.getId(), i, tuple, pkFields,
                            partition, LogType.ENTITY_COMMIT);
                    logManager.log(logRecord);
                }
            } catch (Exception e) {
                failCount.incrementAndGet();
            }
        });
        transactor.start();
        transactor.join();
        Assert.assertEquals(0, failCount.get());
    }

    @Test
    public void interruptedLogFileSwitch() throws Exception {
        final INcApplicationContext ncAppCtx = (INcApplicationContext) integrationUtil.ncs[0].getApplicationContext();
        final LogManager logManager = (LogManager) ncAppCtx.getTransactionSubsystem().getLogManager();
        int logFileCountBeforeInterrupt = logManager.getLogFileIds().size();

        // ensure an interrupted transactor will create next log file but will fail to position the log channel
        final AtomicBoolean interrupted = new AtomicBoolean(false);
        Thread interruptedTransactor = new Thread(() -> {
            Thread.currentThread().interrupt();
            try {
                prepareNextLogFile(logManager);
            } catch (Exception e) {
                Throwable rootCause = ExceptionUtils.getRootCause(e);
                if (rootCause.getCause() instanceof java.nio.channels.ClosedByInterruptException) {
                    interrupted.set(true);
                }
            }
        });
        interruptedTransactor.start();
        interruptedTransactor.join();
        // ensure a new log file was created but the thread was interrupt
        int logFileCountAfterInterrupt = logManager.getLogFileIds().size();
        Assert.assertEquals(logFileCountBeforeInterrupt + 1, logFileCountAfterInterrupt);
        Assert.assertTrue(interrupted.get());

        // ensure next transactor will not create another file
        final AtomicBoolean failed = new AtomicBoolean(false);
        Thread transactor = new Thread(() -> {
            try {
                prepareNextLogFile(logManager);
            } catch (Exception e) {
                failed.set(true);
            }
        });
        transactor.start();
        transactor.join();
        // make sure no new files were created and the operation was successful
        int countAfterTransactor = logManager.getLogFileIds().size();
        Assert.assertEquals(logFileCountAfterInterrupt, countAfterTransactor);
        Assert.assertFalse(failed.get());

        // make sure we can still log to the new file
        interruptedLogPageSwitch();
    }

    private static ITransactionContext beingTransaction(INcApplicationContext ncAppCtx, ILSMIndex index,
            long resourceId) {
        final TxnId txnId = new TxnId(1);
        final TransactionOptions options = new TransactionOptions(ITransactionManager.AtomicityLevel.ENTITY_LEVEL);
        final ITransactionManager transactionManager = ncAppCtx.getTransactionSubsystem().getTransactionManager();
        final ITransactionContext txnCtx = transactionManager.beginTransaction(txnId, options);
        txnCtx.register(resourceId, index, NoOpOperationCallback.INSTANCE, true);
        return txnCtx;
    }

    private static void prepareNextLogFile(LogManager logManager) throws Exception {
        Method method;
        try {
            method = LogManager.class.getDeclaredMethod(PREPARE_NEXT_LOG_FILE_METHOD, null);
        } catch (Exception e) {
            throw new IllegalStateException(
                    "Couldn't find " + PREPARE_NEXT_LOG_FILE_METHOD + " in LogManager. Was it renamed?");
        }
        method.setAccessible(true);
        method.invoke(logManager, null);
    }
}