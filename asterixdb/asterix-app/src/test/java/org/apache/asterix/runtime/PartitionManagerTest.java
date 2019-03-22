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

import java.net.InetSocketAddress;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.asterix.api.common.AsterixHyracksIntegrationUtil;
import org.apache.asterix.common.config.GlobalConfig;
import org.apache.hyracks.api.comm.FixedSizeFrame;
import org.apache.hyracks.api.comm.NetworkAddress;
import org.apache.hyracks.api.context.IHyracksCommonContext;
import org.apache.hyracks.api.dataflow.ConnectorDescriptorId;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.job.JobId;
import org.apache.hyracks.api.job.JobStatus;
import org.apache.hyracks.api.partitions.PartitionId;
import org.apache.hyracks.comm.channels.NetworkInputChannel;
import org.apache.hyracks.control.nc.NodeControllerService;
import org.apache.hyracks.dataflow.std.collectors.InputChannelFrameReader;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

public class PartitionManagerTest {

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
    public void failedJobPartitionRequestTest() throws Exception {
        final NodeControllerService nc1 = integrationUtil.ncs[0];
        final NodeControllerService nc2 = integrationUtil.ncs[1];
        final JobId failedJob = new JobId(-1);
        nc2.getPartitionManager().jobCompleted(failedJob, JobStatus.FAILURE);
        final NetworkAddress localNetworkAddress = nc2.getNetworkManager().getPublicNetworkAddress();
        final InetSocketAddress nc2Address =
                new InetSocketAddress(localNetworkAddress.getAddress(), localNetworkAddress.getPort());
        PartitionId id = new PartitionId(failedJob, new ConnectorDescriptorId(1), 0, 1);
        NetworkInputChannel inputChannel = new NetworkInputChannel(nc1.getNetworkManager(), nc2Address, id, 1);
        InputChannelFrameReader frameReader = new InputChannelFrameReader(inputChannel);
        inputChannel.registerMonitor(frameReader);
        AtomicBoolean failed = new AtomicBoolean(false);
        Thread reader = new Thread(() -> {
            try {
                failed.set(!frameReader.nextFrame(new FixedSizeFrame()));
            } catch (HyracksDataException e) {
                e.printStackTrace();
            }
        });
        reader.start();
        final IHyracksCommonContext context = Mockito.mock(IHyracksCommonContext.class);
        Mockito.when(context.getInitialFrameSize()).thenReturn(2000);
        inputChannel.open(context);
        reader.join(5000);
        Assert.assertTrue(failed.get());
    }
}
