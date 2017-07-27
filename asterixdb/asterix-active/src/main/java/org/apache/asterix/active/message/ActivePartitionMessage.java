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
import java.util.Objects;

import org.apache.asterix.active.ActiveRuntimeId;
import org.apache.asterix.active.IActiveNotificationHandler;
import org.apache.asterix.common.dataflow.ICcApplicationContext;
import org.apache.asterix.common.messaging.api.ICcAddressedMessage;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.job.JobId;

public class ActivePartitionMessage implements ICcAddressedMessage {

    private static final long serialVersionUID = 1L;
    public static final byte ACTIVE_RUNTIME_REGISTERED = 0x00;
    public static final byte ACTIVE_RUNTIME_DEREGISTERED = 0x01;
    public static final byte GENERIC_EVENT = 0x02;
    private final ActiveRuntimeId activeRuntimeId;
    private final JobId jobId;
    private final Serializable payload;
    private final byte event;

    public ActivePartitionMessage(ActiveRuntimeId activeRuntimeId, JobId jobId, byte event, Serializable payload) {
        this.activeRuntimeId = activeRuntimeId;
        this.jobId = jobId;
        this.event = event;
        this.payload = payload;
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

    @Override
    public void handle(ICcApplicationContext appCtx) throws HyracksDataException, InterruptedException {
        IActiveNotificationHandler activeListener = (IActiveNotificationHandler) appCtx.getActiveNotificationHandler();
        activeListener.receive(this);
    }

    @Override
    public String toString() {
        return ActivePartitionMessage.class.getSimpleName() + event;
    }

    @Override
    public int hashCode() {
        return Objects.hash(activeRuntimeId, jobId, payload, event);
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || !(o instanceof ActivePartitionMessage)) {
            return false;
        }
        if (this == o) {
            return true;
        }
        ActivePartitionMessage other = (ActivePartitionMessage) o;
        return Objects.equals(other.activeRuntimeId, activeRuntimeId) && Objects.equals(other.jobId, jobId)
                && Objects.equals(other.payload, payload);
    }
}
