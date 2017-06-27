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

import org.apache.asterix.common.dataflow.ICcApplicationContext;
import org.apache.asterix.common.messaging.api.ICcAddressedMessage;
import org.apache.asterix.common.replication.INCLifecycleMessage;
import org.apache.hyracks.api.exceptions.HyracksDataException;

public class NCLifecycleTaskReportMessage implements INCLifecycleMessage, ICcAddressedMessage {

    private static final long serialVersionUID = 1L;
    private final String nodeId;
    private final boolean success;
    private Exception exception;

    public NCLifecycleTaskReportMessage(String nodeId, boolean success) {
        this.nodeId = nodeId;
        this.success = success;
    }

    @Override
    public void handle(ICcApplicationContext appCtx) throws HyracksDataException, InterruptedException {
        appCtx.getFaultToleranceStrategy().process(this);
    }

    public String getNodeId() {
        return nodeId;
    }

    public boolean isSuccess() {
        return success;
    }

    public Exception getException() {
        return exception;
    }

    public void setException(Exception exception) {
        this.exception = exception;
    }

    @Override
    public MessageType getType() {
        return MessageType.STARTUP_TASK_RESULT;
    }
}