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
package org.apache.hyracks.net.tests;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hyracks.api.comm.IBufferFactory;
import org.apache.hyracks.api.comm.IChannelControlBlock;
import org.apache.hyracks.api.comm.ICloseableBufferAcceptor;
import org.apache.hyracks.api.network.ISocketChannel;
import org.apache.hyracks.net.protocols.muxdemux.ChannelControlBlock;
import org.apache.hyracks.net.protocols.muxdemux.FullFrameChannelReadInterface;
import org.apache.hyracks.util.StorageUtil;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.mockito.Mockito;

@RunWith(Parameterized.class)
public class FullFrameChannelReadInterfaceTest {

    private static final int TEST_RUNS = 100;
    private static final int RECEIVER_BUFFER_COUNT = 50;
    private static int FRAMES_TO_READ_COUNT = 10000;
    private static final int FRAME_SIZE = StorageUtil.getIntSizeInBytes(32, StorageUtil.StorageUnit.KILOBYTE);
    private static final int EXPECTED_CHANNEL_CREDIT = FRAME_SIZE * RECEIVER_BUFFER_COUNT;

    @Parameterized.Parameters
    public static Object[][] data() {
        return new Object[TEST_RUNS][0];
    }

    @Test
    public void bufferRecycleTest() throws Exception {
        final AtomicInteger channelCredit = new AtomicInteger();
        final IChannelControlBlock ccb = mockChannelControlBlock(channelCredit);
        final ReadBufferFactory bufferFactory = new ReadBufferFactory(RECEIVER_BUFFER_COUNT, FRAME_SIZE);
        final FullFrameChannelReadInterface readInterface = new FullFrameChannelReadInterface(ccb);
        final LinkedBlockingDeque<ByteBuffer> fullBufferQ = new LinkedBlockingDeque<>();
        readInterface.setFullBufferAcceptor(new ReadFullBufferAcceptor(fullBufferQ));
        readInterface.setBufferFactory(bufferFactory, RECEIVER_BUFFER_COUNT, FRAME_SIZE);
        Assert.assertEquals(EXPECTED_CHANNEL_CREDIT, channelCredit.get());
        final ISocketChannel socketChannel = mockSocketChannel(ccb);
        final Thread networkFrameReader = new Thread(() -> {
            try {
                int framesRead = FRAMES_TO_READ_COUNT;
                while (framesRead > 0) {
                    while (channelCredit.get() == 0) {
                        synchronized (channelCredit) {
                            channelCredit.wait(10000);
                            if (channelCredit.get() == 0) {
                                System.err.println("Sender doesn't have any write credit");
                                System.exit(1);
                            }
                        }
                    }
                    readInterface.read(socketChannel, FRAME_SIZE);
                    framesRead--;
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        });

        final Thread frameProcessor = new Thread(() -> {
            int framesProcessed = 0;
            try {
                while (true) {
                    final ByteBuffer fullFrame = fullBufferQ.take();
                    fullFrame.clear();
                    readInterface.getEmptyBufferAcceptor().accept(fullFrame);
                    framesProcessed++;
                    if (framesProcessed == FRAMES_TO_READ_COUNT) {
                        return;
                    }
                }
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        });
        networkFrameReader.start();
        frameProcessor.start();
        networkFrameReader.join();
        frameProcessor.join();
        if (channelCredit.get() != EXPECTED_CHANNEL_CREDIT) {
            System.err
                    .println("Expected channel credit " + EXPECTED_CHANNEL_CREDIT + " , found " + channelCredit.get());
            System.exit(1);
        }
    }

    private IChannelControlBlock mockChannelControlBlock(AtomicInteger credit) {
        final ChannelControlBlock ccb = Mockito.mock(ChannelControlBlock.class);
        Mockito.when(ccb.isRemotelyClosed()).thenReturn(false);
        Mockito.doAnswer(invocation -> {
            final Integer delta = invocation.getArgumentAt(0, Integer.class);
            credit.addAndGet(delta);
            synchronized (credit) {
                credit.notifyAll();
            }
            return null;
        }).when(ccb).addPendingCredits(Mockito.anyInt());
        return ccb;
    }

    private ISocketChannel mockSocketChannel(IChannelControlBlock ccb) throws IOException {
        final ISocketChannel sc = Mockito.mock(ISocketChannel.class);
        Mockito.when(sc.read(Mockito.any(ByteBuffer.class))).thenAnswer(invocation -> {
            ccb.addPendingCredits(-FRAME_SIZE);
            final ByteBuffer buffer = invocation.getArgumentAt(0, ByteBuffer.class);
            while (buffer.hasRemaining()) {
                buffer.put((byte) 0);
            }
            return FRAME_SIZE;
        });
        return sc;
    }

    private class ReadFullBufferAcceptor implements ICloseableBufferAcceptor {
        private final BlockingQueue<ByteBuffer> fullBufferQ;

        ReadFullBufferAcceptor(BlockingQueue<ByteBuffer> fullBuffer) {
            this.fullBufferQ = fullBuffer;
        }

        @Override
        public void accept(ByteBuffer buffer) {
            fullBufferQ.add(buffer);
        }

        @Override
        public void close() {
        }

        @Override
        public void error(int ecode) {
        }
    }

    public class ReadBufferFactory implements IBufferFactory {
        private final int limit;
        private final int frameSize;
        private int counter = 0;

        ReadBufferFactory(int limit, int frameSize) {
            this.limit = limit;
            this.frameSize = frameSize;
        }

        @Override
        public ByteBuffer createBuffer() {
            if (counter >= limit) {
                throw new IllegalStateException("Buffer limit exceeded");
            }
            counter++;
            return ByteBuffer.allocate(frameSize);
        }
    }
}
