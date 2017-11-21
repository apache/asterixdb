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

import java.nio.ByteBuffer;
import java.util.concurrent.TimeUnit;

import org.apache.asterix.app.bootstrap.TestNodeController;
import org.apache.asterix.replication.management.NetworkingUtil;
import org.apache.asterix.test.common.TestHelper;
import org.apache.asterix.test.runtime.LangExecutionUtil;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.ConnectorDescriptorId;
import org.apache.hyracks.api.dataflow.TaskAttemptId;
import org.apache.hyracks.api.job.JobId;
import org.apache.hyracks.api.partitions.PartitionId;
import org.apache.hyracks.comm.channels.NetworkOutputChannel;
import org.apache.hyracks.control.nc.NodeControllerService;
import org.apache.hyracks.control.nc.partitions.MaterializingPipelinedPartition;
import org.apache.hyracks.net.protocols.muxdemux.ChannelControlBlock;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

public class DeallocatableTest {

    @Before
    public void setUp() throws Exception {
        TestHelper.deleteExistingInstanceFiles();
    }

    @After
    public void tearDown() throws Exception {
        TestHelper.deleteExistingInstanceFiles();
    }

    @Test
    public void deallocateBeforeConsumerStart() throws Exception {
        TestNodeController nc = new TestNodeController(null, false);
        try {
            nc.init();
            final NodeControllerService ncs =
                    (NodeControllerService) nc.getAppRuntimeContext().getServiceContext().getControllerService();
            final TaskAttemptId taId = Mockito.mock(TaskAttemptId.class);
            JobId jobId = nc.newJobId();
            final IHyracksTaskContext ctx = nc.createTestContext(jobId, 0, true);
            final ConnectorDescriptorId codId = new ConnectorDescriptorId(1);
            final PartitionId pid = new PartitionId(ctx.getJobletContext().getJobId(), codId, 1, 1);
            final ChannelControlBlock ccb = ncs.getNetworkManager()
                    .connect(NetworkingUtil.getSocketAddress(ncs.getNetworkManager().getLocalNetworkAddress()));
            final NetworkOutputChannel networkOutputChannel = new NetworkOutputChannel(ccb, 0);
            final MaterializingPipelinedPartition mpp =
                    new MaterializingPipelinedPartition(ctx, ncs.getPartitionManager(), pid, taId, ncs.getExecutor());
            mpp.open();
            // fill and write frame
            final ByteBuffer frame = ctx.allocateFrame();
            while (frame.hasRemaining()) {
                frame.put((byte) 0);
            }
            frame.flip();
            mpp.nextFrame(frame);
            // close and deallocate before consumer thread starts
            mpp.close();
            mpp.deallocate();
            // start the consumer thread after deallocate
            mpp.writeTo(networkOutputChannel);
            // give consumer thread chance to exit
            TimeUnit.MILLISECONDS.sleep(100);
            LangExecutionUtil.checkThreadLeaks();
            LangExecutionUtil.checkOpenRunFileLeaks();
        } finally {
            nc.deInit();
        }
    }
}
