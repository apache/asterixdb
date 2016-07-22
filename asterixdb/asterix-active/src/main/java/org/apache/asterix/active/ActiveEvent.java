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
package org.apache.asterix.active;

import java.io.Serializable;

import org.apache.hyracks.api.job.JobId;

public class ActiveEvent {

    private final JobId jobId;
    private final EntityId entityId;
    private final Serializable payload;
    private final EventKind eventKind;

    public enum EventKind {
        JOB_START,
        JOB_FINISH,
        PARTITION_EVENT
    }

    public ActiveEvent(JobId jobId, ActiveEvent.EventKind eventKind) {
        this(jobId, eventKind, null, null);
    }

    public ActiveEvent(JobId jobId, ActiveEvent.EventKind eventKind, EntityId feedId) {
        this(jobId, eventKind, feedId, null);
    }

    public ActiveEvent(JobId jobId, ActiveEvent.EventKind eventKind, EntityId feedId, Serializable payload) {
        this.jobId = jobId;
        this.eventKind = eventKind;
        this.entityId = feedId;
        this.payload = payload;
    }

    public JobId getJobId() {
        return jobId;
    }

    public EntityId getFeedId() {
        return entityId;
    }

    public Serializable getPayload() {
        return payload;
    }

    public EventKind getEventKind() {
        return eventKind;
    }
}