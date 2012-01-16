/*
 * Copyright 2009-2010 by The Regents of the University of California
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
package edu.uci.ics.ipc.tests;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

import junit.framework.Assert;

import org.junit.Test;

import edu.uci.ics.hyracks.ipc.api.IIPCHandle;
import edu.uci.ics.hyracks.ipc.api.IIPCI;
import edu.uci.ics.hyracks.ipc.api.SyncRMI;
import edu.uci.ics.hyracks.ipc.impl.IPCSystem;

public class IPCTest {
    @Test
    public void test() throws Exception {
        IPCSystem server = createServerIPCSystem();
        server.start();
        InetSocketAddress serverAddr = server.getSocketAddress();

        IPCSystem client = createClientIPCSystem();
        client.start();

        IIPCHandle handle = client.getHandle(serverAddr);

        SyncRMI rmi = new SyncRMI();
        for (int i = 0; i < 100; ++i) {
            Assert.assertEquals(rmi.call(handle, Integer.valueOf(i)), Integer.valueOf(2 * i));
        }

        IIPCHandle rHandle = server.getHandle(client.getSocketAddress());

        try {
            rmi.call(rHandle, "Foo");
            Assert.assertTrue(false);
        } catch (Exception e) {
            Assert.assertTrue(true);
        }
    }

    private IPCSystem createServerIPCSystem() throws IOException {
        Executor executor = Executors.newCachedThreadPool();
        IIPCI ipci = new IIPCI() {
            @Override
            public Object call(IIPCHandle caller, Object req) throws Exception {
                Integer i = (Integer) req;
                return i.intValue() * 2;
            }
        };
        return new IPCSystem(new InetSocketAddress("127.0.0.1", 0), ipci, executor);
    }

    private IPCSystem createClientIPCSystem() throws IOException {
        Executor executor = Executors.newCachedThreadPool();
        IIPCI ipci = new IIPCI() {
            @Override
            public Object call(IIPCHandle caller, Object req) throws Exception {
                throw new IllegalStateException();
            }
        };
        return new IPCSystem(new InetSocketAddress("127.0.0.1", 0), ipci, executor);
    }
}