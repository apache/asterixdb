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
package org.apache.hyracks.tests.integration;

import static org.apache.hyracks.util.file.FileUtil.joinPath;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.verify;

import java.io.File;
import java.lang.reflect.Field;
import java.util.HashMap;

import org.apache.commons.io.FileUtils;
import org.apache.hyracks.api.client.IHyracksClientConnection;
import org.apache.hyracks.api.job.DeployedJobSpecId;
import org.apache.hyracks.api.job.JobId;
import org.apache.hyracks.api.job.JobSpecification;
import org.apache.hyracks.control.cc.ClusterControllerService;
import org.apache.hyracks.control.cc.cluster.INodeManager;
import org.apache.hyracks.control.cc.cluster.NodeManager;
import org.apache.hyracks.control.common.controllers.CCConfig;
import org.apache.hyracks.control.common.controllers.NCConfig;
import org.apache.hyracks.control.nc.NodeControllerService;
import org.apache.hyracks.ipc.impl.HyracksConnection;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mockito;

public class DeployedJobSpecsTest {
    private static final Logger LOGGER = LogManager.getLogger();

    private static final String NC1_ID = "nc1";
    private static final String NC2_ID = "nc2";
    private static final int TIME_THRESHOLD = 5000;

    private static ClusterControllerService cc;
    private static NodeControllerService nc1;
    private static NodeControllerService nc2;
    private static IHyracksClientConnection hcc;

    @BeforeClass
    public static void init() throws Exception {
        CCConfig ccConfig = new CCConfig();
        ccConfig.setClientListenAddress("127.0.0.1");
        ccConfig.setClientListenPort(39000);
        ccConfig.setClusterListenAddress("127.0.0.1");
        ccConfig.setClusterListenPort(39001);
        ccConfig.setProfileDumpPeriod(10000);
        FileUtils.deleteQuietly(new File(joinPath("target", "data")));
        FileUtils.copyDirectory(new File("data"), new File(joinPath("target", "data")));
        File outDir = new File("target" + File.separator + "ClusterController");
        outDir.mkdirs();
        File ccRoot = File.createTempFile(AbstractIntegrationTest.class.getName(), ".data", outDir);
        ccRoot.delete();
        ccRoot.mkdir();
        ccConfig.setRootDir(ccRoot.getAbsolutePath());
        ClusterControllerService ccBase = new ClusterControllerService(ccConfig);
        // The spying below is dangerous since it replaces the ClusterControllerService already referenced by many
        // objects created in the constructor above
        cc = Mockito.spy(ccBase);
        cc.start();

        // The following code partially fixes the problem created by the spying
        INodeManager nodeManager = cc.getNodeManager();
        Field ccsInNodeManager = NodeManager.class.getDeclaredField("ccs");
        ccsInNodeManager.setAccessible(true);
        ccsInNodeManager.set(nodeManager, cc);

        NCConfig ncConfig1 = new NCConfig(NC1_ID);
        ncConfig1.setClusterAddress("localhost");
        ncConfig1.setClusterPort(39001);
        ncConfig1.setClusterListenAddress("127.0.0.1");
        ncConfig1.setDataListenAddress("127.0.0.1");
        ncConfig1.setResultListenAddress("127.0.0.1");
        ncConfig1.setResultSweepThreshold(TIME_THRESHOLD);
        ncConfig1.setIODevices(new String[] { joinPath(System.getProperty("user.dir"), "target", "data", "device0") });
        NodeControllerService nc1Base = new NodeControllerService(ncConfig1);
        nc1 = Mockito.spy(nc1Base);
        nc1.start();

        NCConfig ncConfig2 = new NCConfig(NC2_ID);
        ncConfig2.setClusterAddress("localhost");
        ncConfig2.setClusterPort(39001);
        ncConfig2.setClusterListenAddress("127.0.0.1");
        ncConfig2.setDataListenAddress("127.0.0.1");
        ncConfig2.setResultListenAddress("127.0.0.1");
        ncConfig2.setResultSweepThreshold(TIME_THRESHOLD);
        ncConfig2.setIODevices(new String[] { joinPath(System.getProperty("user.dir"), "target", "data", "device1") });
        NodeControllerService nc2Base = new NodeControllerService(ncConfig2);
        nc2 = Mockito.spy(nc2Base);
        nc2.start();

        hcc = new HyracksConnection(ccConfig.getClientListenAddress(), ccConfig.getClientListenPort());
        if (LOGGER.isInfoEnabled()) {
            LOGGER.info("Starting CC in " + ccRoot.getAbsolutePath());
        }
    }

    @Test
    public void DistributedTest() throws Exception {
        JobSpecification spec1 = UnionTest.createUnionJobSpec();
        JobSpecification spec2 = HeapSortMergeTest.createSortMergeJobSpec();

        //distribute both jobs
        DeployedJobSpecId distributedId1 = hcc.deployJobSpec(spec1);
        DeployedJobSpecId distributedId2 = hcc.deployJobSpec(spec2);

        //make sure it finished
        //cc will get the store once to check for duplicate insertion and once to insert per job
        verify(cc, Mockito.timeout(TIME_THRESHOLD).times(4)).getDeployedJobSpecStore();
        verify(nc1, Mockito.timeout(TIME_THRESHOLD).times(2)).storeActivityClusterGraph(any(), any());
        verify(nc2, Mockito.timeout(TIME_THRESHOLD).times(2)).storeActivityClusterGraph(any(), any());
        verify(nc1, Mockito.timeout(TIME_THRESHOLD).times(2)).checkForDuplicateDeployedJobSpec(any());
        verify(nc2, Mockito.timeout(TIME_THRESHOLD).times(2)).checkForDuplicateDeployedJobSpec(any());

        //confirm that both jobs are distributed
        Assert.assertTrue(nc1.getActivityClusterGraph(distributedId1) != null
                && nc2.getActivityClusterGraph(distributedId1) != null);
        Assert.assertTrue(nc1.getActivityClusterGraph(distributedId2) != null
                && nc2.getActivityClusterGraph(distributedId2) != null);
        Assert.assertTrue(cc.getDeployedJobSpecStore().getDeployedJobSpecDescriptor(distributedId1) != null);
        Assert.assertTrue(cc.getDeployedJobSpecStore().getDeployedJobSpecDescriptor(distributedId2) != null);

        //run the first job
        JobId jobRunId1 = hcc.startJob(distributedId1, new HashMap<>());
        hcc.waitForCompletion(jobRunId1);

        //Make sure the job parameter map was removed
        verify(cc, Mockito.timeout(TIME_THRESHOLD).times(1)).removeJobParameterByteStore(any());
        verify(nc1, Mockito.timeout(TIME_THRESHOLD).times(1)).removeJobParameterByteStore(any());
        verify(nc2, Mockito.timeout(TIME_THRESHOLD).times(1)).removeJobParameterByteStore(any());

        //destroy the first job
        hcc.undeployJobSpec(distributedId1);

        //make sure it finished
        verify(cc, Mockito.timeout(TIME_THRESHOLD).times(8)).getDeployedJobSpecStore();
        verify(nc1, Mockito.timeout(TIME_THRESHOLD).times(1)).removeActivityClusterGraph(any());
        verify(nc2, Mockito.timeout(TIME_THRESHOLD).times(1)).removeActivityClusterGraph(any());

        //confirm the first job is destroyed
        Assert.assertTrue(nc1.getActivityClusterGraph(distributedId1) == null
                && nc2.getActivityClusterGraph(distributedId1) == null);
        cc.getDeployedJobSpecStore().checkForExistingDeployedJobSpecDescriptor(distributedId1);

        //run the second job
        JobId jobRunId2 = hcc.startJob(distributedId2, new HashMap<>());
        hcc.waitForCompletion(jobRunId2);

        //Make sure the job parameter map was removed
        verify(cc, Mockito.timeout(TIME_THRESHOLD).times(2)).removeJobParameterByteStore(any());
        verify(nc1, Mockito.timeout(TIME_THRESHOLD).times(2)).removeJobParameterByteStore(any());
        verify(nc2, Mockito.timeout(TIME_THRESHOLD).times(2)).removeJobParameterByteStore(any());

        //run the second job again
        JobId jobRunId3 = hcc.startJob(distributedId2, new HashMap<>());
        hcc.waitForCompletion(jobRunId3);

        //Make sure the job parameter map was removed
        verify(cc, Mockito.timeout(TIME_THRESHOLD).times(3)).removeJobParameterByteStore(any());
        verify(nc1, Mockito.timeout(TIME_THRESHOLD).times(3)).removeJobParameterByteStore(any());
        verify(nc2, Mockito.timeout(TIME_THRESHOLD).times(3)).removeJobParameterByteStore(any());

        //destroy the second job
        hcc.undeployJobSpec(distributedId2);

        //make sure it finished
        verify(cc, Mockito.timeout(TIME_THRESHOLD).times(12)).getDeployedJobSpecStore();
        verify(nc1, Mockito.timeout(TIME_THRESHOLD).times(2)).removeActivityClusterGraph(any());
        verify(nc2, Mockito.timeout(TIME_THRESHOLD).times(2)).removeActivityClusterGraph(any());

        //confirm the second job is destroyed
        Assert.assertTrue(nc1.getActivityClusterGraph(distributedId2) == null
                && nc2.getActivityClusterGraph(distributedId2) == null);
        cc.getDeployedJobSpecStore().checkForExistingDeployedJobSpecDescriptor(distributedId2);

        //run the second job 100 times in parallel
        distributedId2 = hcc.deployJobSpec(spec2);
        for (int i = 0; i < 100; i++) {
            hcc.startJob(distributedId2, new HashMap<>());
        }

        //Change the second job into the first job and see whether it runs
        hcc.redeployJobSpec(distributedId2, spec1);
        JobId jobRunId4 = hcc.startJob(distributedId2, new HashMap<>());
        hcc.waitForCompletion(jobRunId4);

        //Run it one more time
        JobId jobRunId5 = hcc.startJob(distributedId2, new HashMap<>());
        hcc.waitForCompletion(jobRunId5);

    }

    @AfterClass
    public static void deinit() throws Exception {
        nc2.stop();
        nc1.stop();
        cc.stop();
    }
}
