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
package org.apache.asterix.feeds;

import org.apache.hyracks.api.job.JobId;
import org.apache.hyracks.api.job.JobInfo;
import org.apache.hyracks.api.job.JobSpecification;

public class FeedInfo {
    public JobSpecification jobSpec;
    public JobInfo jobInfo;
    public JobId jobId;
    public FeedInfoType infoType;
    public State state;

    public enum State {
        ACTIVE,
        INACTIVE
    }

    public enum FeedInfoType {
        INTAKE,
        COLLECT
    }

    public FeedInfo(JobSpecification jobSpec, JobId jobId, FeedInfoType infoType) {
        this.jobSpec = jobSpec;
        this.jobId = jobId;
        this.infoType = infoType;
        this.state = State.INACTIVE;
    }

    @Override
    public String toString() {
        return " job id " + jobId;
    }
}
