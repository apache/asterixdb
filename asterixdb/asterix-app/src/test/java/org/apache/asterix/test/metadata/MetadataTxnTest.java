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
package org.apache.asterix.test.metadata;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import org.apache.asterix.api.common.AsterixHyracksIntegrationUtil;
import org.apache.asterix.common.api.IDatasetLifecycleManager;
import org.apache.asterix.common.api.INcApplicationContext;
import org.apache.asterix.common.config.DatasetConfig;
import org.apache.asterix.common.config.GlobalConfig;
import org.apache.asterix.common.context.DatasetInfo;
import org.apache.asterix.common.context.PrimaryIndexOperationTracker;
import org.apache.asterix.common.dataflow.ICcApplicationContext;
import org.apache.asterix.metadata.MetadataManager;
import org.apache.asterix.metadata.MetadataTransactionContext;
import org.apache.asterix.metadata.api.IMetadataIndex;
import org.apache.asterix.metadata.bootstrap.MetadataBuiltinEntities;
import org.apache.asterix.metadata.bootstrap.MetadataPrimaryIndexes;
import org.apache.asterix.metadata.declared.MetadataProvider;
import org.apache.asterix.metadata.entities.Dataset;
import org.apache.asterix.metadata.entities.NodeGroup;
import org.apache.asterix.metadata.utils.DatasetUtil;
import org.apache.asterix.test.common.TestExecutor;
import org.apache.asterix.testframework.context.TestCaseContext;
import org.apache.hyracks.api.util.InvokeUtil;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIndex;
import org.apache.hyracks.storage.am.lsm.common.impls.AbstractLSMIndex;
import org.apache.hyracks.storage.am.lsm.common.impls.NoMergePolicyFactory;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class MetadataTxnTest {

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
    public void abortMetadataTxn() throws Exception {
        ICcApplicationContext appCtx =
                (ICcApplicationContext) integrationUtil.getClusterControllerService().getApplicationContext();
        final MetadataProvider metadataProvider = new MetadataProvider(appCtx, null);
        final MetadataTransactionContext mdTxn = MetadataManager.INSTANCE.beginTransaction();
        metadataProvider.setMetadataTxnContext(mdTxn);
        final String nodeGroupName = "ng";
        try {
            final List<String> ngNodes = Arrays.asList("asterix_nc1");
            MetadataManager.INSTANCE.addNodegroup(mdTxn, new NodeGroup(nodeGroupName, ngNodes));
            MetadataManager.INSTANCE.abortTransaction(mdTxn);
        } finally {
            metadataProvider.getLocks().unlock();
        }

        // ensure that the node group was not added
        final MetadataTransactionContext readMdTxn = MetadataManager.INSTANCE.beginTransaction();
        try {
            final NodeGroup nodegroup = MetadataManager.INSTANCE.getNodegroup(readMdTxn, nodeGroupName);
            if (nodegroup != null) {
                throw new AssertionError("nodegroup was found after metadata txn was aborted");
            }
        } finally {
            MetadataManager.INSTANCE.commitTransaction(readMdTxn);
        }
    }

    @Test
    public void rebalanceFailureMetadataTxn() throws Exception {
        ICcApplicationContext appCtx =
                (ICcApplicationContext) integrationUtil.getClusterControllerService().getApplicationContext();
        String nodeGroup = "ng";
        String datasetName = "dataset1";
        final TestCaseContext.OutputFormat format = TestCaseContext.OutputFormat.CLEAN_JSON;
        // create original node group
        testExecutor.executeSqlppUpdateOrDdl("CREATE nodegroup " + nodeGroup + " on asterix_nc2;", format);
        // create original dataset
        testExecutor.executeSqlppUpdateOrDdl("CREATE TYPE KeyType AS { id: int };", format);
        testExecutor.executeSqlppUpdateOrDdl(
                "CREATE DATASET " + datasetName + "(KeyType) PRIMARY KEY id on " + nodeGroup + ";", format);
        // find source dataset
        Dataset sourceDataset;
        MetadataProvider metadataProvider = new MetadataProvider(appCtx, null);
        final MetadataTransactionContext mdTxnCtx = MetadataManager.INSTANCE.beginTransaction();
        metadataProvider.setMetadataTxnContext(mdTxnCtx);
        try {
            sourceDataset = metadataProvider.findDataset(MetadataBuiltinEntities.DEFAULT_DATAVERSE_NAME, datasetName);
            MetadataManager.INSTANCE.commitTransaction(mdTxnCtx);
        } finally {
            metadataProvider.getLocks().unlock();
        }

        // create rebalance metadata provider and metadata txn
        metadataProvider = new MetadataProvider(appCtx, null);
        final MetadataTransactionContext rebalanceTxn = MetadataManager.INSTANCE.beginTransaction();
        metadataProvider.setMetadataTxnContext(rebalanceTxn);
        try {
            final Set<String> rebalanceToNodes = Stream.of("asterix_nc1").collect(Collectors.toSet());
            DatasetUtil.createNodeGroupForNewDataset(sourceDataset.getDataverseName(), sourceDataset.getDatasetName(),
                    sourceDataset.getRebalanceCount() + 1, rebalanceToNodes, metadataProvider);
            // rebalance failed --> abort txn
            MetadataManager.INSTANCE.abortTransaction(rebalanceTxn);
        } finally {
            metadataProvider.getLocks().unlock();
        }
        // ensure original dataset can be dropped after rebalance failure
        testExecutor.executeSqlppUpdateOrDdl("DROP DATASET " + datasetName + ";", format);

        // ensure the node group was dropped too since its only dataset was dropped
        final MetadataTransactionContext readMdTxn = MetadataManager.INSTANCE.beginTransaction();
        try {
            final NodeGroup nodegroup = MetadataManager.INSTANCE.getNodegroup(readMdTxn, nodeGroup);
            if (nodegroup != null) {
                throw new AssertionError("nodegroup was found after its only dataset was dropped");
            }
        } finally {
            MetadataManager.INSTANCE.commitTransaction(readMdTxn);
        }
    }

    @Test
    public void concurrentMetadataTxn() throws Exception {
        // get create type and dataset
        String datasetName = "dataset1";
        final TestCaseContext.OutputFormat format = TestCaseContext.OutputFormat.CLEAN_JSON;
        testExecutor.executeSqlppUpdateOrDdl("CREATE TYPE KeyType AS { id: int };", format);
        testExecutor.executeSqlppUpdateOrDdl("CREATE DATASET " + datasetName + "(KeyType) PRIMARY KEY id;", format);

        // get created dataset
        ICcApplicationContext appCtx =
                (ICcApplicationContext) integrationUtil.getClusterControllerService().getApplicationContext();
        MetadataProvider metadataProvider = new MetadataProvider(appCtx, null);
        final MetadataTransactionContext mdTxnCtx = MetadataManager.INSTANCE.beginTransaction();
        metadataProvider.setMetadataTxnContext(mdTxnCtx);
        Dataset sourceDataset;
        try {
            sourceDataset = metadataProvider.findDataset(MetadataBuiltinEntities.DEFAULT_DATAVERSE_NAME, datasetName);
            MetadataManager.INSTANCE.commitTransaction(mdTxnCtx);
        } finally {
            metadataProvider.getLocks().unlock();
        }

        /*
         * Concurrently insert copies of the created dataset with
         * different names and either commit or abort the transaction.
         */
        final AtomicInteger failCount = new AtomicInteger(0);
        Thread transactor1 = new Thread(() -> IntStream.range(1, 100).forEach(x -> {
            try {
                addDataset(appCtx, sourceDataset, x, x % 2 == 0);
            } catch (Exception e) {
                e.printStackTrace();
                failCount.incrementAndGet();
            }
        }));

        Thread transactor2 = new Thread(() -> IntStream.range(101, 200).forEach(x -> {
            try {
                addDataset(appCtx, sourceDataset, x, x % 3 == 0);
            } catch (Exception e) {
                e.printStackTrace();
                failCount.incrementAndGet();
            }
        }));

        transactor1.start();
        transactor2.start();
        transactor1.join();
        transactor2.join();

        Assert.assertEquals(0, failCount.get());

        // make sure all metadata indexes have no pending operations after all txns committed/aborted
        final IDatasetLifecycleManager datasetLifecycleManager =
                ((INcApplicationContext) integrationUtil.ncs[0].getApplicationContext()).getDatasetLifecycleManager();
        int maxMetadatasetId = 14;
        for (int i = 1; i <= maxMetadatasetId; i++) {
            ILSMIndex index = (ILSMIndex) datasetLifecycleManager.getIndex(i, i);
            if (index != null) {
                final PrimaryIndexOperationTracker opTracker =
                        (PrimaryIndexOperationTracker) index.getOperationTracker();
                Assert.assertEquals(0, opTracker.getNumActiveOperations());
            }
        }
    }

    @Test
    public void surviveInterruptOnMetadataTxnCommit() throws Exception {
        ICcApplicationContext appCtx =
                (ICcApplicationContext) integrationUtil.getClusterControllerService().getApplicationContext();
        final MetadataProvider metadataProvider = new MetadataProvider(appCtx, null);
        final MetadataTransactionContext mdTxn = MetadataManager.INSTANCE.beginTransaction();
        metadataProvider.setMetadataTxnContext(mdTxn);
        final String nodeGroupName = "ng";
        Thread transactor = new Thread(() -> {
            final List<String> ngNodes = Arrays.asList("asterix_nc1");
            try {
                MetadataManager.INSTANCE.addNodegroup(mdTxn, new NodeGroup(nodeGroupName, ngNodes));
                Thread.currentThread().interrupt();
                MetadataManager.INSTANCE.commitTransaction(mdTxn);
            } catch (Exception e) {
                e.printStackTrace();
            }
        });
        transactor.start();
        transactor.join();
        // ensure that the node group was added
        final MetadataTransactionContext readMdTxn = MetadataManager.INSTANCE.beginTransaction();
        try {
            final NodeGroup nodegroup = MetadataManager.INSTANCE.getNodegroup(readMdTxn, nodeGroupName);
            if (nodegroup == null) {
                throw new AssertionError("nodegroup was found after metadata txn was aborted");
            }
        } finally {
            MetadataManager.INSTANCE.commitTransaction(readMdTxn);
        }
    }

    @Test
    public void failedFlushOnUncommittedMetadataTxn() throws Exception {
        ICcApplicationContext ccAppCtx =
                (ICcApplicationContext) integrationUtil.getClusterControllerService().getApplicationContext();
        final MetadataProvider metadataProvider = new MetadataProvider(ccAppCtx, null);
        final MetadataTransactionContext mdTxn = MetadataManager.INSTANCE.beginTransaction();
        metadataProvider.setMetadataTxnContext(mdTxn);
        final String nodeGroupName = "ng";
        final String committedNodeGroup = "committed_ng";
        final List<String> ngNodes = Collections.singletonList("asterix_nc1");
        try {
            MetadataManager.INSTANCE.addNodegroup(mdTxn, new NodeGroup(nodeGroupName, ngNodes));
            MetadataManager.INSTANCE.commitTransaction(mdTxn);
        } finally {
            metadataProvider.getLocks().unlock();
        }
        INcApplicationContext appCtx = (INcApplicationContext) integrationUtil.ncs[0].getApplicationContext();
        IDatasetLifecycleManager dlcm = appCtx.getDatasetLifecycleManager();
        dlcm.flushAllDatasets();
        IMetadataIndex idx = MetadataPrimaryIndexes.NODEGROUP_DATASET;
        DatasetInfo datasetInfo = dlcm.getDatasetInfo(idx.getDatasetId().getId());
        AbstractLSMIndex index = (AbstractLSMIndex) appCtx.getDatasetLifecycleManager()
                .getIndex(idx.getDatasetId().getId(), idx.getResourceId());
        PrimaryIndexOperationTracker opTracker = (PrimaryIndexOperationTracker) index.getOperationTracker();
        final MetadataTransactionContext mdTxn2 = MetadataManager.INSTANCE.beginTransaction();
        int mutableComponentBeforeFlush = index.getCurrentMemoryComponentIndex();
        int diskComponentsBeforeFlush = index.getDiskComponents().size();
        // lock opTracker to prevent log flusher from triggering flush
        synchronized (opTracker) {
            final MetadataTransactionContext committedMdTxn = MetadataManager.INSTANCE.beginTransaction();
            MetadataManager.INSTANCE.addNodegroup(committedMdTxn, new NodeGroup(committedNodeGroup, ngNodes));
            MetadataManager.INSTANCE.commitTransaction(committedMdTxn);
            opTracker.setFlushOnExit(true);
            opTracker.flushIfNeeded();
            Assert.assertTrue(opTracker.isFlushLogCreated());
            metadataProvider.setMetadataTxnContext(mdTxn2);
            // make sure force operation will processed
            MetadataManager.INSTANCE.dropNodegroup(mdTxn2, nodeGroupName, false);
            Assert.assertEquals(1, opTracker.getNumActiveOperations());
            // release opTracker lock now to allow log flusher to schedule the flush
            InvokeUtil.runWithTimeout(() -> {
                synchronized (opTracker) {
                    opTracker.wait(1000);
                }
            }, () -> !opTracker.isFlushLogCreated(), 10, TimeUnit.SECONDS);
        }
        // ensure flush failed to be scheduled
        datasetInfo.waitForIO();
        Assert.assertEquals(mutableComponentBeforeFlush, index.getCurrentMemoryComponentIndex());
        Assert.assertEquals(diskComponentsBeforeFlush, index.getDiskComponents().size());
        // after committing, the flush should be scheduled successfully
        opTracker.setFlushOnExit(true);
        MetadataManager.INSTANCE.commitTransaction(mdTxn2);
        metadataProvider.getLocks().unlock();
        InvokeUtil.runWithTimeout(() -> {
            synchronized (opTracker) {
                opTracker.wait(1000);
            }
        }, () -> !opTracker.isFlushLogCreated(), 10, TimeUnit.SECONDS);
        // ensure flush completed successfully and the component was switched
        datasetInfo.waitForIO();
        Assert.assertNotEquals(mutableComponentBeforeFlush, index.getCurrentMemoryComponentIndex());
        Assert.assertNotEquals(diskComponentsBeforeFlush, index.getDiskComponents().size());
    }

    private void addDataset(ICcApplicationContext appCtx, Dataset source, int datasetPostfix, boolean abort)
            throws Exception {
        Dataset dataset = new Dataset(source.getDataverseName(), "ds_" + datasetPostfix, source.getDataverseName(),
                source.getDatasetType().name(), source.getNodeGroupName(), NoMergePolicyFactory.NAME, null,
                source.getDatasetDetails(), source.getHints(), DatasetConfig.DatasetType.INTERNAL, datasetPostfix, 0);
        MetadataProvider metadataProvider = new MetadataProvider(appCtx, null);
        final MetadataTransactionContext writeTxn = MetadataManager.INSTANCE.beginTransaction();
        metadataProvider.setMetadataTxnContext(writeTxn);
        try {
            MetadataManager.INSTANCE.addDataset(writeTxn, dataset);
            if (abort) {
                MetadataManager.INSTANCE.abortTransaction(writeTxn);
            } else {
                MetadataManager.INSTANCE.commitTransaction(writeTxn);
            }
        } finally {
            metadataProvider.getLocks().unlock();
        }
    }
}
