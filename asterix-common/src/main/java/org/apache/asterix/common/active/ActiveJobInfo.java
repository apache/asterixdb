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
package org.apache.asterix.common.active;

import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.hyracks.api.job.JobId;
import org.apache.hyracks.api.job.JobSpecification;

public class ActiveJobInfo {

    private static final Logger LOGGER = Logger.getLogger(ActiveJobInfo.class.getName());

    public enum JobState {
        CREATED,
        INACTIVE,
        ACTIVE,
        UNDER_RECOVERY,
        ENDED
    }

    public enum ActiveJopType {
        FEED_INTAKE,
        FEED_COLLECT,
        FEED_CONNECT,
        CHANNEL_REPETITIVE,
        CHANNEL_CONTINUOUS,
        PROCEDURE
    }

    protected JobId jobId;
    protected JobState state;
    protected JobSpecification spec;
    protected ActiveJopType jobType;
    protected ActiveJobId activeJobId;

    public ActiveJobInfo(JobId jobId, JobState state, ActiveJopType jobType, JobSpecification spec,
            ActiveJobId activeJobId) {
        this.jobId = jobId;
        this.state = state;
        this.spec = spec;
        this.jobType = jobType;
        this.activeJobId = activeJobId;
    }

    public JobId getJobId() {
        return jobId;
    }

    public ActiveJopType getJobType() {
        return jobType;
    }

    public JobState getState() {
        return state;
    }

    public ActiveJobId getActiveJobId() {
        return activeJobId;
    }

    public void setState(JobState state) {
        this.state = state;
        if (LOGGER.isLoggable(Level.INFO)) {
            LOGGER.info(this + " is in " + state + " state.");
        }
    }

    public JobSpecification getSpec() {
        return spec;
    }

    public void setSpec(JobSpecification spec) {
        this.spec = spec;
    }

}
