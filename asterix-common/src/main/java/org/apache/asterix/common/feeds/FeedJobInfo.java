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
package org.apache.asterix.common.feeds;

import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.hyracks.api.job.JobId;
import org.apache.hyracks.api.job.JobSpecification;

public class FeedJobInfo {

    private static final Logger LOGGER = Logger.getLogger(FeedJobInfo.class.getName());

    public enum JobType {
        INTAKE,
        FEED_CONNECT
    }

    public enum FeedJobState {
        CREATED,
        ACTIVE,
        UNDER_RECOVERY,
        ENDED
    }

    protected final JobId jobId;
    protected final JobType jobType;
    protected FeedJobState state;
    protected JobSpecification spec;

    public FeedJobInfo(JobId jobId, FeedJobState state, JobType jobType, JobSpecification spec) {
        this.jobId = jobId;
        this.state = state;
        this.jobType = jobType;
        this.spec = spec;
    }

    public JobId getJobId() {
        return jobId;
    }

    public FeedJobState getState() {
        return state;
    }

    public void setState(FeedJobState state) {
        this.state = state;
        if (LOGGER.isLoggable(Level.INFO)) {
            LOGGER.info(this + " is in " + state + " state.");
        }
    }

    public JobType getJobType() {
        return jobType;
    }

    public JobSpecification getSpec() {
        return spec;
    }

    public void setSpec(JobSpecification spec) {
        this.spec = spec;
    }

    public String toString() {
        return jobId + " [" + jobType + "]";
    }

}
