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

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.LinkedList;
import java.util.Queue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.hyracks.api.comm.IBufferAcceptor;
import org.apache.hyracks.api.comm.ICloseableBufferAcceptor;
import org.apache.hyracks.ipc.sockets.PlainSocketChannelFactory;
import org.apache.hyracks.net.protocols.muxdemux.ChannelControlBlock;
import org.apache.hyracks.net.protocols.muxdemux.FullFrameChannelInterfaceFactory;
import org.apache.hyracks.net.protocols.muxdemux.IChannelOpenListener;
import org.apache.hyracks.net.protocols.muxdemux.MultiplexedConnection;
import org.apache.hyracks.net.protocols.muxdemux.MuxDemux;
import org.junit.Assert;
import org.junit.Test;

public class NetTest {
    @Test
    public void test() throws Exception {
        AtomicBoolean failFlag = new AtomicBoolean();

        MuxDemux md1 = createMuxDemux("md1", failFlag);
        md1.start();
        MuxDemux md2 = createMuxDemux("md2", failFlag);
        md2.start();
        InetSocketAddress md2Address = md2.getLocalAddress();

        MultiplexedConnection md1md2 = md1.connect(md2Address);

        Thread t1 = createThread(md1md2, 1);
        Thread t2 = createThread(md1md2, -1);
        t1.start();
        t2.start();

        t1.join();
        t2.join();

        Assert.assertFalse("Failure flag was set to true", failFlag.get());
    }

    private Thread createThread(final MultiplexedConnection md1md2, final int factor) {
        return new Thread() {
            @Override
            public void run() {
                try {
                    ChannelControlBlock md1md2c1 = md1md2.openChannel();

                    final Semaphore sem = new Semaphore(1);
                    sem.acquire();
                    md1md2c1.getWriteInterface().setEmptyBufferAcceptor(new IBufferAcceptor() {
                        @Override
                        public void accept(ByteBuffer buffer) {
                        }
                    });

                    md1md2c1.getReadInterface().setFullBufferAcceptor(new ICloseableBufferAcceptor() {
                        @Override
                        public void accept(ByteBuffer buffer) {
                        }

                        @Override
                        public void error(int ecode) {
                        }

                        @Override
                        public void close() {
                            sem.release();
                        }
                    });

                    ICloseableBufferAcceptor fba = md1md2c1.getWriteInterface().getFullBufferAcceptor();
                    for (int i = 0; i < 10000; ++i) {
                        ByteBuffer buffer = ByteBuffer.allocate(1024);
                        for (int j = 0; j < 256; ++j) {
                            buffer.putInt(factor * (i + j));
                        }
                        buffer.flip();
                        fba.accept(buffer);
                    }
                    fba.close();
                    sem.acquire();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        };

    }

    private MuxDemux createMuxDemux(final String label, final AtomicBoolean failFlag) {
        IChannelOpenListener md1OpenListener = new IChannelOpenListener() {
            @Override
            public void channelOpened(final ChannelControlBlock channel) {
                final ChannelIO cio = new ChannelIO(label, channel);
                channel.getReadInterface().setFullBufferAcceptor(cio.rifba);
                channel.getWriteInterface().setEmptyBufferAcceptor(cio.wieba);

                final IBufferAcceptor rieba = channel.getReadInterface().getEmptyBufferAcceptor();
                for (int i = 0; i < 50; ++i) {
                    rieba.accept(ByteBuffer.allocate(1024));
                }
                new Thread() {
                    private int prevTotal = 0;

                    @Override
                    public void run() {
                        while (true) {
                            ByteBuffer fbuf = null;
                            synchronized (channel) {
                                while (!cio.eos && cio.ecode == 0 && cio.rifq.isEmpty()) {
                                    try {
                                        channel.wait();
                                    } catch (InterruptedException e) {
                                        e.printStackTrace();
                                    }
                                }
                                if (!cio.rifq.isEmpty()) {
                                    fbuf = cio.rifq.poll();
                                } else if (cio.ecode != 0) {
                                    throw new RuntimeException("Error: " + cio.ecode);
                                } else if (cio.eos) {
                                    channel.getWriteInterface().getFullBufferAcceptor().close();
                                    return;
                                }
                            }
                            int counter = 0;
                            while (fbuf.remaining() > 0) {
                                counter += fbuf.getInt();
                            }
                            if (prevTotal != 0) {
                                if (Math.abs(counter - prevTotal) != 256) {
                                    failFlag.set(true);
                                }
                            }
                            prevTotal = counter;
                            fbuf.compact();
                            rieba.accept(fbuf);
                        }
                    }
                }.start();
            }
        };
        return new MuxDemux(new InetSocketAddress("127.0.0.1", 0), md1OpenListener, 1, 5,
                FullFrameChannelInterfaceFactory.INSTANCE, PlainSocketChannelFactory.INSTANCE);
    }

    private class ChannelIO {
        private ChannelControlBlock channel;

        private Queue<ByteBuffer> rifq;

        private Queue<ByteBuffer> wieq;

        private boolean eos;

        private int ecode;

        private ICloseableBufferAcceptor rifba;

        private IBufferAcceptor wieba;

        public ChannelIO(final String label, ChannelControlBlock channel) {
            this.channel = channel;
            this.rifq = new LinkedList<ByteBuffer>();
            this.wieq = new LinkedList<ByteBuffer>();

            rifba = new ICloseableBufferAcceptor() {
                @Override
                public void accept(ByteBuffer buffer) {
                    rifq.add(buffer);
                    ChannelIO.this.channel.notifyAll();
                }

                @Override
                public void error(int ecode) {
                    ChannelIO.this.ecode = ecode;
                    ChannelIO.this.channel.notifyAll();
                }

                @Override
                public void close() {
                    eos = true;
                    ChannelIO.this.channel.notifyAll();
                }
            };

            wieba = new IBufferAcceptor() {
                @Override
                public void accept(ByteBuffer buffer) {
                    wieq.add(buffer);
                    ChannelIO.this.channel.notifyAll();
                }
            };
        }
    }
}
