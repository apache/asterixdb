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
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.hyracks.api.job.JobId;
import org.apache.hyracks.api.job.JobSpecification;

public class ActiveJob implements Serializable {
    private static final long serialVersionUID = 1L;
    private static final Logger LOGGER = Logger.getLogger(ActiveJob.class.getName());
    protected final EntityId entityId;
    protected JobId jobId;
    protected final Serializable jobObject;
    protected ActivityState state;
    protected JobSpecification spec;

    public ActiveJob(EntityId entityId, JobId jobId, ActivityState state, Serializable jobInfo, JobSpecification spec) {
        this.entityId = entityId;
        this.state = state;
        this.jobId = jobId;
        this.jobObject = jobInfo;
        this.spec = spec;
    }

    public ActiveJob(EntityId entityId, ActivityState state, Serializable jobInfo, JobSpecification spec) {
        this.entityId = entityId;
        this.state = state;
        this.jobObject = jobInfo;
        this.spec = spec;
    }

    public JobId getJobId() {
        return jobId;
    }

    public void setJobId(JobId jobId) {
        this.jobId = jobId;
    }

    public ActivityState getState() {
        return state;
    }

    public void setState(ActivityState state) {
        this.state = state;
        if (LOGGER.isLoggable(Level.INFO)) {
            LOGGER.info(this + " is in " + state + " state.");
        }
    }

    public Object getJobObject() {
        return jobObject;
    }

    public JobSpecification getSpec() {
        return spec;
    }

    public void setSpec(JobSpecification spec) {
        this.spec = spec;
    }

    @Override
    public String toString() {
        return jobId + " [" + jobObject + "]";
    }

    public EntityId getEntityId() {
        return entityId;
    }

}
