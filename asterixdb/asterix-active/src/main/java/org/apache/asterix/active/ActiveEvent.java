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

import java.util.Objects;

import org.apache.hyracks.api.job.JobId;

public class ActiveEvent {

    public enum Kind {
        JOB_CREATED,
        JOB_STARTED,
        JOB_FINISHED,
        PARTITION_EVENT,
        EXTENSION_EVENT,
        STATS_UPDATED,
        STATE_CHANGED,
        FAILURE
    }

    private final JobId jobId;
    private final EntityId entityId;
    private final Kind eventKind;
    private final Object eventObject;

    public ActiveEvent(JobId jobId, Kind eventKind, EntityId entityId, Object eventObject) {
        this.jobId = jobId;
        this.entityId = entityId;
        this.eventKind = eventKind;
        this.eventObject = eventObject;
    }

    public JobId getJobId() {
        return jobId;
    }

    public EntityId getEntityId() {
        return entityId;
    }

    public Kind getEventKind() {
        return eventKind;
    }

    public Object getEventObject() {
        return eventObject;
    }

    @Override
    public String toString() {
        return "JobId:" + jobId + "," + "EntityId:" + entityId + ", " + "Kind" + eventKind;
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof ActiveEvent)) {
            return false;
        }
        if (this == o) {
            return true;
        }
        ActiveEvent other = (ActiveEvent) o;
        return Objects.equals(entityId, other.entityId) && Objects.equals(eventKind, other.eventKind)
                && Objects.equals(eventObject, other.eventObject);
    }

    @Override
    public int hashCode() {
        return Objects.hash(jobId, entityId, eventKind, eventObject);
    }
}
