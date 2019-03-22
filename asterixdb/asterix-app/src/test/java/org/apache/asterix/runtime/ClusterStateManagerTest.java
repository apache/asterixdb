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
package org.apache.asterix.runtime;

import java.util.Collections;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.asterix.app.replication.NcLifecycleCoordinator;
import org.apache.asterix.app.replication.message.NCLifecycleTaskReportMessage;
import org.apache.asterix.common.api.IClusterManagementWork.ClusterState;
import org.apache.asterix.common.cluster.ClusterPartition;
import org.apache.asterix.common.cluster.IGlobalRecoveryManager;
import org.apache.asterix.common.config.MetadataProperties;
import org.apache.asterix.common.metadata.IMetadataBootstrap;
import org.apache.asterix.common.utils.NcLocalCounters;
import org.apache.asterix.runtime.transaction.ResourceIdManager;
import org.apache.asterix.runtime.utils.BulkTxnIdFactory;
import org.apache.asterix.runtime.utils.CcApplicationContext;
import org.apache.asterix.runtime.utils.ClusterStateManager;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.api.application.ICCServiceContext;
import org.apache.hyracks.api.config.IApplicationConfig;
import org.apache.hyracks.api.control.CcId;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.exceptions.HyracksException;
import org.apache.hyracks.api.job.JobIdFactory;
import org.apache.hyracks.control.cc.ClusterControllerService;
import org.apache.hyracks.control.cc.application.CCServiceContext;
import org.apache.hyracks.control.common.application.ConfigManagerApplicationConfig;
import org.apache.hyracks.control.common.config.ConfigManager;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

public class ClusterStateManagerTest {

    private static final String NC1 = "NC1";
    private static final String NC2 = "NC2";
    private static final String NC3 = "NC3";
    private static final String METADATA_NODE = NC1;

    /**
     * Ensures that a cluster with a fixed topology will not be active until
     * all partitions are active.
     *
     * @throws Exception
     */
    @Test
    public void fixedTopologyState() throws Exception {
        ClusterStateManager csm = new ClusterStateManager();
        CcApplicationContext ccAppCtx = ccAppContext(csm);
        // prepare fixed topology
        ccAppCtx.getMetadataProperties().getClusterPartitions().put(0, new ClusterPartition(0, NC1, 0));
        ccAppCtx.getMetadataProperties().getClusterPartitions().put(1, new ClusterPartition(1, NC2, 0));
        ccAppCtx.getMetadataProperties().getClusterPartitions().put(2, new ClusterPartition(2, NC3, 0));
        for (ClusterPartition cp : ccAppCtx.getMetadataProperties().getClusterPartitions().values()) {
            ccAppCtx.getMetadataProperties().getNodePartitions().put(cp.getNodeId(), new ClusterPartition[] { cp });
        }
        csm.setCcAppCtx(ccAppCtx);

        // notify NC1 joined and completed startup
        notifyNodeJoined(csm, NC1, 0, false);
        notifyNodeStartupCompletion(ccAppCtx, NC1);
        // cluster should be unusable
        Assert.assertTrue(!csm.isClusterActive());
        // notify NC2 joined
        notifyNodeJoined(csm, NC2, 1, false);
        // notify NC3 joined
        notifyNodeJoined(csm, NC3, 2, false);
        // notify NC2 completed startup
        notifyNodeStartupCompletion(ccAppCtx, NC2);
        // cluster should still be unusable
        Assert.assertTrue(!csm.isClusterActive());
        // notify NC3 completed startup
        notifyNodeStartupCompletion(ccAppCtx, NC3);
        // cluster should now be active
        Assert.assertTrue(csm.isClusterActive());
        // NC2 failed
        csm.notifyNodeFailure(NC2);
        // cluster should now be unusable
        Assert.assertTrue(!csm.isClusterActive());
    }

    /**
     * Ensures that a cluster with a dynamic topology will not go into unusable state while
     * new partitions are dynamically added.
     *
     * @throws Exception
     */
    @Test
    public void dynamicTopologyState() throws Exception {
        ClusterStateManager csm = new ClusterStateManager();
        CcApplicationContext ccApplicationContext = ccAppContext(csm);
        csm.setCcAppCtx(ccApplicationContext);

        // notify NC1 joined and completed startup
        notifyNodeJoined(csm, NC1, 0, true);
        notifyNodeStartupCompletion(ccApplicationContext, NC1);
        // cluster should now be active
        Assert.assertTrue(csm.isClusterActive());
        // notify NC2 joined
        notifyNodeJoined(csm, NC2, 1, true);
        // notify NC3 joined
        notifyNodeJoined(csm, NC3, 2, true);
        // cluster should still be active
        Assert.assertTrue(csm.isClusterActive());
        //  notify NC2 completed startup
        notifyNodeStartupCompletion(ccApplicationContext, NC2);
        // cluster should still be active
        Assert.assertTrue(csm.isClusterActive());
        //  notify NC3 completed startup
        notifyNodeStartupCompletion(ccApplicationContext, NC3);
        // cluster should still be active
        Assert.assertTrue(csm.isClusterActive());
        // NC2 failed
        csm.notifyNodeFailure(NC2);
        // cluster should now be unusable
        Assert.assertTrue(!csm.isClusterActive());
    }

    /**
     * Ensures that a cluster with a dynamic topology will not go into unusable state if
     * a newly added node fails before completing its startup
     *
     * @throws Exception
     */
    @Test
    public void dynamicTopologyNodeFailure() throws Exception {
        ClusterStateManager csm = new ClusterStateManager();
        CcApplicationContext ccApplicationContext = ccAppContext(csm);
        csm.setCcAppCtx(ccApplicationContext);

        // notify NC1 joined and completed startup
        notifyNodeJoined(csm, NC1, 0, true);
        notifyNodeStartupCompletion(ccApplicationContext, NC1);
        // cluster should now be active
        Assert.assertTrue(csm.isClusterActive());
        // notify NC2 joined
        notifyNodeJoined(csm, NC2, 1, true);
        // notify NC3 joined
        notifyNodeJoined(csm, NC3, 2, true);
        // cluster should still be active
        Assert.assertTrue(csm.isClusterActive());
        //  notify NC2 completed startup
        notifyNodeStartupCompletion(ccApplicationContext, NC2);
        // cluster should still be active
        Assert.assertTrue(csm.isClusterActive());
        // NC3 failed before completing startup
        csm.notifyNodeFailure(NC3);
        // cluster should still be active
        Assert.assertTrue(csm.isClusterActive());
    }

    /**
     * Ensures that a cluster with a dynamic topology will be in an unusable state
     * if all partitions are pending activation
     *
     * @throws Exception
     */
    @Test
    public void dynamicTopologyNoActivePartitions() throws Exception {
        ClusterStateManager csm = new ClusterStateManager();
        CcApplicationContext ccApplicationContext = ccAppContext(csm);
        csm.setCcAppCtx(ccApplicationContext);

        // notify NC1 joined
        notifyNodeJoined(csm, NC1, 0, true);
        // notify NC1 failed before completing startup
        csm.notifyNodeFailure(NC1);
        Assert.assertTrue(csm.getState() == ClusterState.UNUSABLE);
    }

    private void notifyNodeJoined(ClusterStateManager csm, String nodeId, int partitionId, boolean registerPartitions)
            throws HyracksException, AlgebricksException {
        csm.notifyNodeJoin(nodeId, Collections.emptyMap());
        if (registerPartitions) {
            csm.registerNodePartitions(nodeId, new ClusterPartition[] { new ClusterPartition(partitionId, nodeId, 0) });
        }
    }

    private void notifyNodeStartupCompletion(CcApplicationContext applicationContext, String nodeId)
            throws HyracksDataException {
        NCLifecycleTaskReportMessage msg = new NCLifecycleTaskReportMessage(nodeId, true, mockLocalCounters());
        applicationContext.getNcLifecycleCoordinator().process(msg);
    }

    private CcApplicationContext ccAppContext(ClusterStateManager csm) throws HyracksDataException {
        CcApplicationContext ccApplicationContext = Mockito.mock(CcApplicationContext.class);
        ConfigManager configManager = new ConfigManager(null);
        IApplicationConfig applicationConfig = new ConfigManagerApplicationConfig(configManager);
        ICCServiceContext iccServiceContext = Mockito.mock(CCServiceContext.class);
        final ClusterControllerService ccs = Mockito.mock(ClusterControllerService.class);
        JobIdFactory jobIdFactory = new JobIdFactory(CcId.valueOf(0));
        Mockito.when(ccs.getJobIdFactory()).thenReturn(jobIdFactory);
        Mockito.when(iccServiceContext.getAppConfig()).thenReturn(applicationConfig);
        Mockito.when(iccServiceContext.getControllerService()).thenReturn(ccs);

        Mockito.when(ccApplicationContext.getServiceContext()).thenReturn(iccServiceContext);

        NcLifecycleCoordinator coordinator =
                new NcLifecycleCoordinator(ccApplicationContext.getServiceContext(), false);
        coordinator.bindTo(csm);
        Mockito.when(ccApplicationContext.getNcLifecycleCoordinator()).thenReturn(coordinator);

        MetadataProperties metadataProperties = mockMetadataProperties();
        Mockito.when(ccApplicationContext.getMetadataProperties()).thenReturn(metadataProperties);

        ResourceIdManager resourceIdManager = new ResourceIdManager(csm);
        Mockito.when(ccApplicationContext.getResourceIdManager()).thenReturn(resourceIdManager);

        IMetadataBootstrap metadataBootstrap = Mockito.mock(IMetadataBootstrap.class);
        Mockito.doNothing().when(metadataBootstrap).init();
        Mockito.when(ccApplicationContext.getMetadataBootstrap()).thenReturn(metadataBootstrap);

        IGlobalRecoveryManager globalRecoveryManager = Mockito.mock(IGlobalRecoveryManager.class);
        Mockito.when(globalRecoveryManager.isRecoveryCompleted()).thenReturn(true);
        Mockito.when(ccApplicationContext.getGlobalRecoveryManager()).thenReturn(globalRecoveryManager);

        BulkTxnIdFactory bulkTxnIdFactory = new BulkTxnIdFactory();
        Mockito.when(ccApplicationContext.getTxnIdFactory()).thenReturn(bulkTxnIdFactory);
        return ccApplicationContext;
    }

    private MetadataProperties mockMetadataProperties() {
        SortedMap<Integer, ClusterPartition> clusterPartitions = Collections.synchronizedSortedMap(new TreeMap<>());
        Map<String, ClusterPartition[]> nodePartitionsMap = new ConcurrentHashMap<>();
        nodePartitionsMap.put(METADATA_NODE, new ClusterPartition[] { new ClusterPartition(0, METADATA_NODE, 0) });
        MetadataProperties metadataProperties = Mockito.mock(MetadataProperties.class);
        Mockito.when(metadataProperties.getMetadataNodeName()).thenReturn(METADATA_NODE);
        Mockito.when(metadataProperties.getClusterPartitions()).thenReturn(clusterPartitions);
        Mockito.when(metadataProperties.getNodePartitions()).thenReturn(nodePartitionsMap);
        return metadataProperties;
    }

    private NcLocalCounters mockLocalCounters() {
        final NcLocalCounters localCounters = Mockito.mock(NcLocalCounters.class);
        Mockito.when(localCounters.getMaxJobId()).thenReturn(1000L);
        Mockito.when(localCounters.getMaxResourceId()).thenReturn(1000L);
        Mockito.when(localCounters.getMaxTxnId()).thenReturn(1000L);
        return localCounters;
    }
}
