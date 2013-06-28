/*
 * Copyright 2009-2013 by The Regents of the University of California
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * you may obtain a copy of the License from
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.uci.ics.hyracks.ipc.tests;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

import junit.framework.Assert;

import org.junit.Test;

import edu.uci.ics.hyracks.ipc.api.IIPCHandle;
import edu.uci.ics.hyracks.ipc.api.IIPCI;
import edu.uci.ics.hyracks.ipc.api.RPCInterface;
import edu.uci.ics.hyracks.ipc.exceptions.IPCException;
import edu.uci.ics.hyracks.ipc.impl.IPCSystem;
import edu.uci.ics.hyracks.ipc.impl.JavaSerializationBasedPayloadSerializerDeserializer;

public class IPCTest {
    @Test
    public void test() throws Exception {
        IPCSystem server = createServerIPCSystem();
        server.start();
        InetSocketAddress serverAddr = server.getSocketAddress();

        RPCInterface rpci = new RPCInterface();
        IPCSystem client = createClientIPCSystem(rpci);
        client.start();

        IIPCHandle handle = client.getHandle(serverAddr);

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
            public void deliverIncomingMessage(final IIPCHandle handle, final long mid, long rmid,
                    final Object payload, Exception exception) {
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
        return new IPCSystem(new InetSocketAddress("127.0.0.1", 0), ipci,
                new JavaSerializationBasedPayloadSerializerDeserializer());
    }

    private IPCSystem createClientIPCSystem(RPCInterface rpci) throws IOException {
        return new IPCSystem(new InetSocketAddress("127.0.0.1", 0), rpci,
                new JavaSerializationBasedPayloadSerializerDeserializer());
    }
}