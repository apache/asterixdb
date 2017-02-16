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

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.verify;

import java.io.File;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.commons.io.FileUtils;
import org.apache.hyracks.api.client.HyracksConnection;
import org.apache.hyracks.api.client.IHyracksClientConnection;
import org.apache.hyracks.api.job.JobId;
import org.apache.hyracks.api.job.JobSpecification;
import org.apache.hyracks.control.cc.ClusterControllerService;
import org.apache.hyracks.control.common.controllers.CCConfig;
import org.apache.hyracks.control.common.controllers.NCConfig;
import org.apache.hyracks.control.nc.NodeControllerService;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mockito;

public class PredistributedJobsTest {
    private static final Logger LOGGER = Logger.getLogger(PredistributedJobsTest.class.getName());

    private static final String NC1_ID = "nc1";
    private static final String NC2_ID = "nc2";

    private static ClusterControllerService cc;
    private static NodeControllerService nc1;
    private static NodeControllerService nc2;
    private static IHyracksClientConnection hcc;

    @BeforeClass
    public static void init() throws Exception {
        CCConfig ccConfig = new CCConfig();
        ccConfig.clientNetIpAddress = "127.0.0.1";
        ccConfig.clientNetPort = 39000;
        ccConfig.clusterNetIpAddress = "127.0.0.1";
        ccConfig.clusterNetPort = 39001;
        ccConfig.profileDumpPeriod = 10000;
        FileUtils.deleteQuietly(new File("target" + File.separator + "data"));
        FileUtils.copyDirectory(new File("data"), new File("target" + File.separator + "data"));
        File outDir = new File("target" + File.separator + "ClusterController");
        outDir.mkdirs();
        File ccRoot = File.createTempFile(AbstractIntegrationTest.class.getName(), ".data", outDir);
        ccRoot.delete();
        ccRoot.mkdir();
        ccConfig.ccRoot = ccRoot.getAbsolutePath();
        ClusterControllerService ccBase = new ClusterControllerService(ccConfig);
        cc = Mockito.spy(ccBase);
        cc.start();

        NCConfig ncConfig1 = new NCConfig();
        ncConfig1.ccHost = "localhost";
        ncConfig1.ccPort = 39001;
        ncConfig1.clusterNetIPAddress = "127.0.0.1";
        ncConfig1.dataIPAddress = "127.0.0.1";
        ncConfig1.resultIPAddress = "127.0.0.1";
        ncConfig1.nodeId = NC1_ID;
        ncConfig1.ioDevices = System.getProperty("user.dir") + File.separator + "target" + File.separator + "data"
                + File.separator + "device0";
        NodeControllerService nc1Base = new NodeControllerService(ncConfig1);
        nc1 = Mockito.spy(nc1Base);
        nc1.start();

        NCConfig ncConfig2 = new NCConfig();
        ncConfig2.ccHost = "localhost";
        ncConfig2.ccPort = 39001;
        ncConfig2.clusterNetIPAddress = "127.0.0.1";
        ncConfig2.dataIPAddress = "127.0.0.1";
        ncConfig2.resultIPAddress = "127.0.0.1";
        ncConfig2.nodeId = NC2_ID;
        ncConfig2.ioDevices = System.getProperty("user.dir") + File.separator + "target" + File.separator + "data"
                + File.separator + "device1";
        NodeControllerService nc2Base = new NodeControllerService(ncConfig2);
        nc2 = Mockito.spy(nc2Base);
        nc2.start();

        hcc = new HyracksConnection(ccConfig.clientNetIpAddress, ccConfig.clientNetPort);
        if (LOGGER.isLoggable(Level.INFO)) {
            LOGGER.info("Starting CC in " + ccRoot.getAbsolutePath());
        }
    }

    @Test
    public void DistributedTest() throws Exception {
        JobSpecification spec1 = UnionTest.createUnionJobSpec();
        JobSpecification spec2 = HeapSortMergeTest.createSortMergeJobSpec();

        //distribute both jobs
        JobId jobId1 = hcc.distributeJob(spec1);
        JobId jobId2 = hcc.distributeJob(spec2);

        //make sure it finished
        //cc will get the store once to check for duplicate insertion and once to insert per job
        verify(cc, Mockito.timeout(5000).times(4)).getPreDistributedJobStore();
        verify(nc1, Mockito.timeout(5000).times(2)).storeActivityClusterGraph(any(), any());
        verify(nc2, Mockito.timeout(5000).times(2)).storeActivityClusterGraph(any(), any());
        verify(nc1, Mockito.timeout(5000).times(2)).checkForDuplicateDistributedJob(any());
        verify(nc2, Mockito.timeout(5000).times(2)).checkForDuplicateDistributedJob(any());

        //confirm that both jobs are distributed
        Assert.assertTrue(nc1.getActivityClusterGraph(jobId1) != null && nc2.getActivityClusterGraph(jobId1) != null);
        Assert.assertTrue(nc1.getActivityClusterGraph(jobId2) != null && nc2.getActivityClusterGraph(jobId2) != null);
        Assert.assertTrue(cc.getPreDistributedJobStore().getDistributedJobDescriptor(jobId1) != null);
        Assert.assertTrue(cc.getPreDistributedJobStore().getDistributedJobDescriptor(jobId2) != null);

        //run the first job
        hcc.startJob(jobId1);
        hcc.waitForCompletion(jobId1);

        //destroy the first job
        hcc.destroyJob(jobId1);

        //make sure it finished
        verify(cc, Mockito.timeout(5000).times(8)).getPreDistributedJobStore();
        verify(nc1, Mockito.timeout(5000).times(1)).removeActivityClusterGraph(any());
        verify(nc2, Mockito.timeout(5000).times(1)).removeActivityClusterGraph(any());

        //confirm the first job is destroyed
        Assert.assertTrue(nc1.getActivityClusterGraph(jobId1) == null && nc2.getActivityClusterGraph(jobId1) == null);
        cc.getPreDistributedJobStore().checkForExistingDistributedJobDescriptor(jobId1);

        //run the second job
        hcc.startJob(jobId2);
        hcc.waitForCompletion(jobId2);

        //run the second job again
        hcc.startJob(jobId2);
        hcc.waitForCompletion(jobId2);

        //destroy the second job
        hcc.destroyJob(jobId2);

        //make sure it finished
        verify(cc, Mockito.timeout(5000).times(12)).getPreDistributedJobStore();
        verify(nc1, Mockito.timeout(5000).times(2)).removeActivityClusterGraph(any());
        verify(nc2, Mockito.timeout(5000).times(2)).removeActivityClusterGraph(any());

        //confirm the second job is destroyed
        Assert.assertTrue(nc1.getActivityClusterGraph(jobId2) == null && nc2.getActivityClusterGraph(jobId2) == null);
        cc.getPreDistributedJobStore().checkForExistingDistributedJobDescriptor(jobId2);
    }

    @AfterClass
    public static void deinit() throws Exception {
        nc2.stop();
        nc1.stop();
        cc.stop();
    }
}
