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
package org.apache.hyracks.examples.shutdown.test;

import java.net.ServerSocket;

import org.apache.hyracks.api.client.IHyracksClientConnection;
import org.apache.hyracks.ipc.exceptions.IPCException;
import org.apache.hyracks.ipc.impl.HyracksConnection;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

public class ClusterShutdownIT {
    private static Logger LOGGER = LogManager.getLogger();
    @Rule
    public ExpectedException closeTwice = ExpectedException.none();

    @Test
    public void runShutdown() throws Exception {
        IHyracksClientConnection hcc = new HyracksConnection("localhost", 1098);
        hcc.stopCluster(false);
        //what happens here...
        closeTwice.expect(IPCException.class);
        closeTwice.expectMessage("Connection failed to localhost/127.0.0.1:1098");
        hcc.stopCluster(false);
        ServerSocket c = null;
        ServerSocket s = null;
        try {
            c = new ServerSocket(1098); // we should be able to bind to this
            s = new ServerSocket(1099); // and we should be able to bind to this too
        } catch (Exception e) {
            LOGGER.error("Unexpected error", e);
            throw e;
        } finally {
            if (s != null) {
                s.close();
            }
            if (c != null) {
                c.close();
            }
        }
    }

}
