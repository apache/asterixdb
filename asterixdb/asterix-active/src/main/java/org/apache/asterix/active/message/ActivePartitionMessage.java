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
package org.apache.asterix.active.message;

import java.io.Serializable;

import org.apache.asterix.active.ActiveRuntimeId;
import org.apache.asterix.common.messaging.AbstractApplicationMessage;
import org.apache.hyracks.api.job.JobId;

public class ActivePartitionMessage extends AbstractApplicationMessage {

    public static final byte ACTIVE_RUNTIME_REGISTERED = 0x00;
    public static final byte ACTIVE_RUNTIME_DEREGISTERED = 0x01;
    private static final long serialVersionUID = 1L;
    private final ActiveRuntimeId activeRuntimeId;
    private final JobId jobId;
    private final Serializable payload;
    private final byte event;

    public ActivePartitionMessage(ActiveRuntimeId activeRuntimeId, JobId jobId, byte event) {
        this(activeRuntimeId, jobId, event, null);
    }

    public ActivePartitionMessage(ActiveRuntimeId activeRuntimeId, JobId jobId, byte event, Serializable payload) {
        this.activeRuntimeId = activeRuntimeId;
        this.jobId = jobId;
        this.event = event;
        this.payload = payload;
    }

    @Override
    public ApplicationMessageType getMessageType() {
        return ApplicationMessageType.ACTIVE_ENTITY_TO_CC_MESSAGE;
    }

    public ActiveRuntimeId getActiveRuntimeId() {
        return activeRuntimeId;
    }

    public JobId getJobId() {
        return jobId;
    }

    public Serializable getPayload() {
        return payload;
    }

    public byte getEvent() {
        return event;
    }
}
