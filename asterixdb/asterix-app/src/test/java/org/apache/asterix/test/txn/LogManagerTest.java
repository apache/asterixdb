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
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.asterix.api.common.AsterixHyracksIntegrationUtil;
import org.apache.asterix.common.TestDataUtil;
import org.apache.asterix.common.api.IDatasetLifecycleManager;
import org.apache.asterix.common.api.INcApplicationContext;
import org.apache.asterix.common.config.GlobalConfig;
import org.apache.asterix.common.config.TransactionProperties;
import org.apache.asterix.common.dataflow.DatasetLocalResource;
import org.apache.asterix.common.exceptions.ACIDException;
import org.apache.asterix.common.exceptions.ExceptionUtils;
import org.apache.asterix.common.transactions.DatasetId;
import org.apache.asterix.common.transactions.ILockManager;
import org.apache.asterix.common.transactions.ILogManager;
import org.apache.asterix.common.transactions.ITransactionContext;
import org.apache.asterix.common.transactions.ITransactionManager;
import org.apache.asterix.common.transactions.LogRecord;
import org.apache.asterix.common.transactions.LogSource;
import org.apache.asterix.common.transactions.LogType;
import org.apache.asterix.common.transactions.TransactionOptions;
import org.apache.asterix.common.transactions.TxnId;
import org.apache.asterix.common.utils.TransactionUtil;
import org.apache.asterix.metadata.entities.Dataset;
import org.apache.asterix.test.common.TestTupleReference;
import org.apache.asterix.transaction.management.service.logging.LogManager;
import org.apache.asterix.transaction.management.service.transaction.TransactionContextFactory;
import org.apache.asterix.transaction.management.service.transaction.TransactionManagementConstants.LockManagerConstants;
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
    private static final String ENSURE_LAST_PAGE_FLUSHED_METHOD = "ensureLastPageFlushed";

    @Before
    public void setUp() throws Exception {
        System.setProperty(GlobalConfig.CONFIG_FILE_PROPERTY, TEST_CONFIG_FILE_NAME);
        // use a small page size for test purpose
        integrationUtil.addOption(TransactionProperties.Option.TXN_LOG_BUFFER_PAGESIZE, 128 * 1024);
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
        int logFileCountBeforeInterrupt = logManager.getOrderedLogFileIds().size();

        // ensure an interrupted transactor will create next log file but will fail to position the log channel
        final AtomicBoolean failed = new AtomicBoolean(false);
        Thread interruptedTransactor = new Thread(() -> {
            Thread.currentThread().interrupt();
            try {
                prepareNextLogFile(logManager);
            } catch (Exception e) {
                failed.set(true);
            }
        });
        interruptedTransactor.start();
        interruptedTransactor.join();
        // ensure a new log file was created and survived interrupt
        int logFileCountAfterInterrupt = logManager.getOrderedLogFileIds().size();
        Assert.assertEquals(logFileCountBeforeInterrupt + 1, logFileCountAfterInterrupt);
        Assert.assertFalse(failed.get());

        // make sure we can still log to the new file
        interruptedLogPageSwitch();
    }

    @Test
    public void waitLogTest() throws Exception {
        final INcApplicationContext ncAppCtx = (INcApplicationContext) integrationUtil.ncs[0].getApplicationContext();
        LogRecord logRecord = new LogRecord();
        final long txnId = 1;
        logRecord.setTxnCtx(TransactionContextFactory.create(new TxnId(txnId),
                new TransactionOptions(ITransactionManager.AtomicityLevel.ENTITY_LEVEL)));
        logRecord.setLogSource(LogSource.LOCAL);
        logRecord.setLogType(LogType.WAIT);
        logRecord.setTxnId(txnId);
        logRecord.isFlushed(false);
        logRecord.computeAndSetLogSize();
        Thread transactor = new Thread(() -> {
            final LogManager logManager = (LogManager) ncAppCtx.getTransactionSubsystem().getLogManager();
            logManager.log(logRecord);
        });
        transactor.start();
        transactor.join(TimeUnit.SECONDS.toMillis(30));
        Assert.assertTrue(logRecord.isFlushed());
    }

    private static ITransactionContext beingTransaction(INcApplicationContext ncAppCtx, ILSMIndex index,
            long resourceId) {
        final TxnId txnId = new TxnId(1);
        final TransactionOptions options = new TransactionOptions(ITransactionManager.AtomicityLevel.ENTITY_LEVEL);
        final ITransactionManager transactionManager = ncAppCtx.getTransactionSubsystem().getTransactionManager();
        final ITransactionContext txnCtx = transactionManager.beginTransaction(txnId, options);
        txnCtx.register(resourceId, 0, index, NoOpOperationCallback.INSTANCE, true);
        return txnCtx;
    }

    private static void prepareNextLogFile(LogManager logManager) throws Exception {
        Method ensureLastPageFlushed;
        Method prepareNextLogFile;
        String targetMethod = null;
        try {
            targetMethod = ENSURE_LAST_PAGE_FLUSHED_METHOD;
            ensureLastPageFlushed = LogManager.class.getDeclaredMethod(targetMethod, null);
            targetMethod = PREPARE_NEXT_LOG_FILE_METHOD;
            prepareNextLogFile = LogManager.class.getDeclaredMethod(targetMethod, null);
        } catch (Exception e) {
            throw new IllegalStateException("Couldn't find " + targetMethod + " in LogManager. Was it renamed?");
        }
        ensureLastPageFlushed.setAccessible(true);
        ensureLastPageFlushed.invoke(logManager, null);
        prepareNextLogFile.setAccessible(true);
        prepareNextLogFile.invoke(logManager, null);
    }
}
