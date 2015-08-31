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

import org.apache.asterix.common.active.ActiveJobInfo;
import org.apache.hyracks.api.job.JobId;
import org.apache.hyracks.api.job.JobSpecification;

public class FeedJobInfo extends ActiveJobInfo {

    public enum JobType {
        INTAKE,
        COLLECT,
        FEED_CONNECT
    }

    private final JobType jobType;

    public FeedJobInfo(JobId jobId, JobState state, JobType jobType, JobSpecification spec) {
        super(jobId, state, spec);
        this.jobType = jobType;
    }

    public JobType getJobType() {
        return jobType;
    }

    public String toString() {
        return jobId + " [" + jobType + "]";
    }

}
