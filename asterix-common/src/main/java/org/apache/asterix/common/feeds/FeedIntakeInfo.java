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

import java.util.List;

import org.apache.asterix.common.active.ActiveId;
import org.apache.asterix.common.active.ActiveJobInfo;
import org.apache.asterix.common.feeds.api.IFeedJoint;
import org.apache.hyracks.api.job.JobId;
import org.apache.hyracks.api.job.JobSpecification;

public class FeedIntakeInfo extends ActiveJobInfo {

    private final ActiveId feedId;
    private final IFeedJoint intakeFeedJoint;
    private List<String> intakeLocation;

    public FeedIntakeInfo(JobId jobId, JobState state, ActiveId feedId, IFeedJoint intakeFeedJoint,
            JobSpecification spec) {
        super(jobId, state, ActiveJopType.FEED_INTAKE, spec, null);
        this.feedId = feedId;
        this.intakeFeedJoint = intakeFeedJoint;
    }

    public ActiveId getFeedId() {
        return feedId;
    }

    public IFeedJoint getIntakeFeedJoint() {
        return intakeFeedJoint;
    }

    public JobSpecification getSpec() {
        return spec;
    }

    public List<String> getIntakeLocation() {
        return intakeLocation;
    }

    public void setIntakeLocation(List<String> intakeLocation) {
        this.intakeLocation = intakeLocation;
    }

}
