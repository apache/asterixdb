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
package org.apache.asterix.common.channels;

import java.util.List;

import org.apache.asterix.common.active.ActiveJobInfo;
import org.apache.hyracks.api.job.JobId;
import org.apache.hyracks.api.job.JobSpecification;

public class ChannelJobInfo extends ActiveJobInfo {

    public enum ChannelJobType {
        REPETITIVE,
        CONTINUOUS
    }

    protected final ChannelJobType jobType;

    private final ChannelId channelId;
    private List<String> location;

    public ChannelJobInfo(JobId jobId, JobState state, ChannelJobType jobType, JobSpecification spec,
            ChannelId channelId) {
        super(jobId, state, spec);
        this.jobType = jobType;
        this.channelId = channelId;
    }

    public ChannelId getChannelId() {
        return channelId;
    }

    public ChannelJobType getJobType() {
        return jobType;
    }

    public String toString() {
        return jobId + " [" + jobType + "]";
    }

    public List<String> getLocation() {
        return location;
    }

    public void setLocation(List<String> location) {
        this.location = location;
    }

}
