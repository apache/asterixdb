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
package edu.uci.ics.hyracks.control.cc.work;

import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;

import edu.uci.ics.hyracks.api.messages.IMessage;
import edu.uci.ics.hyracks.control.cc.ClusterControllerService;
import edu.uci.ics.hyracks.control.common.application.ApplicationContext;
import edu.uci.ics.hyracks.control.common.work.AbstractWork;

/**
 * @author rico
 * 
 */
public class ApplicationMessageWork extends AbstractWork {

    private static final Logger LOGGER = Logger.getLogger(ApplicationMessageWork.class.getName());
    private byte[] message;
    private String nodeId;
    private ClusterControllerService ccs;
    private String appName;

    public ApplicationMessageWork(ClusterControllerService ccs, byte[] message, String appName, String nodeId) {
        this.ccs = ccs;
        this.nodeId = nodeId;
        this.message = message;
        this.appName = appName;
    }

    @Override
    public void run() {

        final ApplicationContext ctx = ccs.getApplicationMap().get(appName);
        try {
            final IMessage data = (IMessage) ctx.deserialize(message);
            (new Thread() {
                public void run() {
                    ctx.getMessageBroker().receivedMessage(data, nodeId);
                }
            }).start();
        } catch (IOException e) {
            LOGGER.log(Level.WARNING, "Error in stats reporting", e);
        } catch (ClassNotFoundException e) {
            Logger.getLogger(this.getClass().getName()).log(Level.WARNING, "Error in stats reporting", e);
        }
    }

    @Override
    public String toString() {
        return "nodeID: " + nodeId;
    }

}
