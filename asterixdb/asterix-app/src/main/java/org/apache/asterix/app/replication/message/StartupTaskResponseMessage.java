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
package org.apache.asterix.app.replication.message;

import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.asterix.common.api.INCLifecycleTask;
import org.apache.asterix.common.api.INcApplicationContext;
import org.apache.asterix.common.messaging.api.INCMessageBroker;
import org.apache.asterix.common.messaging.api.INcAddressedMessage;
import org.apache.asterix.common.replication.INCLifecycleMessage;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.service.IControllerService;
import org.apache.hyracks.control.nc.NCShutdownHook;
import org.apache.hyracks.util.ExitUtil;

public class StartupTaskResponseMessage implements INCLifecycleMessage, INcAddressedMessage {

    private static final Logger LOGGER = Logger.getLogger(StartupTaskResponseMessage.class.getName());
    private static final long serialVersionUID = 1L;
    private final String nodeId;
    private final List<INCLifecycleTask> tasks;

    public StartupTaskResponseMessage(String nodeId, List<INCLifecycleTask> tasks) {
        this.nodeId = nodeId;
        this.tasks = tasks;
    }

    @Override
    public void handle(INcApplicationContext appCtx) throws HyracksDataException, InterruptedException {
        INCMessageBroker broker = (INCMessageBroker) appCtx.getServiceContext().getMessageBroker();
        IControllerService cs = appCtx.getServiceContext().getControllerService();
        boolean success = true;
        try {
            Throwable exception = null;
            try {
                for (INCLifecycleTask task : tasks) {
                    if (LOGGER.isLoggable(Level.INFO)) {
                        LOGGER.log(Level.INFO, "Starting startup task: " + task);
                    }
                    task.perform(cs);
                    if (LOGGER.isLoggable(Level.INFO)) {
                        LOGGER.log(Level.INFO, "Completed startup task: " + task);
                    }
                }
            } catch (Throwable e) { //NOSONAR all startup failures should be reported to CC
                LOGGER.log(Level.SEVERE, "Failed during startup task", e);
                success = false;
                exception = e;
            }
            NCLifecycleTaskReportMessage result = new NCLifecycleTaskReportMessage(nodeId, success);
            result.setException(exception);
            try {
                broker.sendMessageToCC(result);
            } catch (Exception e) {
                success = false;
                LOGGER.log(Level.SEVERE, "Failed sending message to cc", e);
            }
        } finally {
            if (!success) {
                // stop NC so that it can be started again
                ExitUtil.exit(NCShutdownHook.FAILED_TO_STARTUP_EXIT_CODE);
            }
        }
    }

    public String getNodeId() {
        return nodeId;
    }

    @Override
    public MessageType getType() {
        return MessageType.STARTUP_TASK_RESPONSE;
    }
}