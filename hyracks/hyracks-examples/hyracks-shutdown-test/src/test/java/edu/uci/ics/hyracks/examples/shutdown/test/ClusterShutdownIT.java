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
package edu.uci.ics.hyracks.examples.shutdown.test;

import java.net.ServerSocket;
import java.util.logging.Logger;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import edu.uci.ics.hyracks.api.client.HyracksConnection;
import edu.uci.ics.hyracks.api.client.IHyracksClientConnection;
import edu.uci.ics.hyracks.ipc.exceptions.IPCException;

public class ClusterShutdownIT {
    private static Logger LOGGER = Logger.getLogger(ClusterShutdownIT.class.getName());
    @Rule
    public ExpectedException closeTwice = ExpectedException.none();
    @Test
    public void runShutdown() throws Exception {
        IHyracksClientConnection hcc = new HyracksConnection("localhost", 1098);
        hcc.stopCluster();
        //what happens here...
        closeTwice.expect(IPCException.class);
        closeTwice.expectMessage("Cannot send on a closed handle");
        hcc.stopCluster();
        ServerSocket c = null;
        ServerSocket s = null;
        try {
            c = new ServerSocket(1098);
            //we should be able to bind to this 
            s = new ServerSocket(1099);
            //and we should be able to bind to this too 
        } catch (Exception e) {
            LOGGER.severe(e.getMessage());
            throw e;
        } finally {
            s.close();
            c.close();
        }
    }

}
