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
package org.apache.hyracks.ipc.tests;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

import org.apache.hyracks.ipc.api.IIPCHandle;
import org.apache.hyracks.ipc.api.IIPCI;
import org.apache.hyracks.ipc.api.RPCInterface;
import org.apache.hyracks.ipc.exceptions.IPCException;
import org.apache.hyracks.ipc.impl.IPCSystem;
import org.apache.hyracks.ipc.impl.JavaSerializationBasedPayloadSerializerDeserializer;
import org.apache.hyracks.ipc.sockets.PlainSocketChannelFactory;
import org.junit.Assert;
import org.junit.Test;

public class IPCTest {
    @Test
    public void test() throws Exception {
        IPCSystem server = createServerIPCSystem();
        server.start();
        InetSocketAddress serverAddr = server.getSocketAddress();

        RPCInterface rpci = new RPCInterface();
        IPCSystem client = createClientIPCSystem(rpci);
        client.start();

        IIPCHandle handle = client.getHandle(serverAddr, 0);

        for (int i = 0; i < 100; ++i) {
            Assert.assertEquals(rpci.call(handle, Integer.valueOf(i)), Integer.valueOf(2 * i));
        }

        try {
            rpci.call(handle, "Foo");
            Assert.assertTrue(false);
        } catch (Exception e) {
            Assert.assertTrue(true);
        }
    }

    private IPCSystem createServerIPCSystem() throws IOException {
        final Executor executor = Executors.newCachedThreadPool();
        IIPCI ipci = new IIPCI() {
            @Override
            public void deliverIncomingMessage(final IIPCHandle handle, final long mid, long rmid, final Object payload,
                    Exception exception) {
                executor.execute(new Runnable() {
                    @Override
                    public void run() {
                        Object result = null;
                        Exception exception = null;
                        try {
                            Integer i = (Integer) payload;
                            result = i.intValue() * 2;
                        } catch (Exception e) {
                            exception = e;
                        }
                        try {
                            handle.send(mid, result, exception);
                        } catch (IPCException e) {
                            e.printStackTrace();
                        }
                    }
                });
            }
        };
        return new IPCSystem(new InetSocketAddress("127.0.0.1", 0), PlainSocketChannelFactory.INSTANCE, ipci,
                new JavaSerializationBasedPayloadSerializerDeserializer());
    }

    private IPCSystem createClientIPCSystem(RPCInterface rpci) throws IOException {
        return new IPCSystem(new InetSocketAddress("127.0.0.1", 0), PlainSocketChannelFactory.INSTANCE, rpci,
                new JavaSerializationBasedPayloadSerializerDeserializer());
    }
}
