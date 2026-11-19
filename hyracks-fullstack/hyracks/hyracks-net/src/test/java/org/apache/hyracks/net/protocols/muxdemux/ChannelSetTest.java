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
package org.apache.hyracks.net.protocols.muxdemux;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.apache.hyracks.api.comm.IChannelInterfaceFactory;
import org.apache.hyracks.api.comm.IChannelReadInterface;
import org.apache.hyracks.api.comm.IChannelWriteInterface;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class ChannelSetTest {

    private ChannelSet cSet;

    @Before
    public void setUp() {
        IChannelInterfaceFactory factory = mock(IChannelInterfaceFactory.class);
        when(factory.createReadInterface(any())).thenReturn(mock(IChannelReadInterface.class));
        when(factory.createWriteInterface(any())).thenReturn(mock(IChannelWriteInterface.class));
        MultiplexedConnection mConn = mock(MultiplexedConnection.class);
        when(mConn.getChannelInterfaceFactory()).thenReturn(factory);
        cSet = new ChannelSet(mConn, new IEventCounter() {
            @Override
            public void increment() {
            }

            @Override
            public void decrement() {
            }
        });
    }

    @Test
    public void slotIsRecycledOnlyOnceBothAcksAreSeen() throws Exception {
        ChannelControlBlock c0 = cSet.allocateChannel();
        ChannelControlBlock c1 = cSet.allocateChannel();
        Assert.assertEquals(0, c0.getChannelId());
        Assert.assertEquals(1, c1.getChannelId());

        c0.reportLocalEOSAck();
        Assert.assertEquals("half-closed channel must keep its slot", 2, cSet.allocateChannel().getChannelId());

        c0.reportRemoteEOSAck();
        Assert.assertSame(c0, cSet.getCCB(0));
        ChannelControlBlock reused = cSet.allocateChannel();
        Assert.assertEquals(0, reused.getChannelId());
        Assert.assertNotSame(c0, reused);
        Assert.assertEquals(3, cSet.getOpenChannelCount());
    }

    @Test
    public void acksInEitherOrderMarkTheSlot() throws Exception {
        ChannelControlBlock c0 = cSet.allocateChannel();
        cSet.allocateChannel();
        c0.reportRemoteEOSAck();
        c0.reportLocalEOSAck();
        Assert.assertEquals(0, cSet.allocateChannel().getChannelId());
    }

    @Test
    public void remotelyOpenedChannelReusesAClosedSlot() throws Exception {
        ChannelControlBlock c0 = cSet.allocateChannel();
        c0.reportLocalEOSAck();
        c0.reportRemoteEOSAck();
        ChannelControlBlock remote = cSet.registerChannel(0);
        Assert.assertNotSame(c0, remote);
        Assert.assertEquals(1, cSet.getOpenChannelCount());
        // the slot is live again and must not be reclaimed by the next allocation
        Assert.assertEquals(1, cSet.allocateChannel().getChannelId());
        Assert.assertSame(remote, cSet.getCCB(0));
    }

    @Test
    public void readerSideAckByIdMarksTheSlot() throws Exception {
        ChannelControlBlock c0 = cSet.allocateChannel();
        cSet.allocateChannel();
        c0.reportRemoteEOSAck();
        Assert.assertSame(c0, cSet.reportLocalEOSAck(0));
        Assert.assertEquals(0, cSet.allocateChannel().getChannelId());
    }

    @Test
    public void supersededBlockDoesNotMarkItsSuccessor() throws Exception {
        ChannelControlBlock c0 = cSet.allocateChannel();
        c0.reportLocalEOSAck();
        c0.reportRemoteEOSAck();
        // the peer reopens slot 0 before we next allocate, replacing c0 there
        ChannelControlBlock remote = cSet.registerChannel(0);
        Assert.assertNotSame(c0, remote);
        // a late ack still reaching the superseded block must not mark the slot its successor now holds
        c0.reportLocalEOSAck();
        c0.reportRemoteEOSAck();
        Assert.assertEquals(1, cSet.allocateChannel().getChannelId());
        Assert.assertSame(remote, cSet.getCCB(0));
        Assert.assertEquals(2, cSet.getOpenChannelCount());
    }

    @Test
    public void growsPastInitialCapacity() throws Exception {
        for (int i = 0; i < 100; i++) {
            Assert.assertEquals(i, cSet.allocateChannel().getChannelId());
        }
        ChannelControlBlock remote = cSet.registerChannel(100);
        Assert.assertSame(remote, cSet.getCCB(100));
    }
}
