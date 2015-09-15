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

import java.util.Map;

public class FeedActivity implements Comparable<FeedActivity> {

    private int activityId;

    private final String dataverseName;
    private final String datasetName;
    private final String feedName;
    private final Map<String, String> feedActivityDetails;

    public static class FeedActivityDetails {
        public static final String INTAKE_LOCATIONS = "intake-locations";
        public static final String COMPUTE_LOCATIONS = "compute-locations";
        public static final String STORAGE_LOCATIONS = "storage-locations";
        public static final String COLLECT_LOCATIONS = "collect-locations";
        public static final String FEED_POLICY_NAME = "feed-policy-name";
        public static final String FEED_CONNECT_TIMESTAMP = "feed-connect-timestamp";

    }

    public FeedActivity(String dataverseName, String feedName, String datasetName,
            Map<String, String> feedActivityDetails) {
        this.dataverseName = dataverseName;
        this.feedName = feedName;
        this.datasetName = datasetName;
        this.feedActivityDetails = feedActivityDetails;
    }

    public String getDataverseName() {
        return dataverseName;
    }

    public String getDatasetName() {
        return datasetName;
    }

    public String getFeedName() {
        return feedName;
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }
        if (!(other instanceof FeedActivity)) {
            return false;
        }

        if (!((FeedActivity) other).dataverseName.equals(dataverseName)) {
            return false;
        }
        if (!((FeedActivity) other).datasetName.equals(datasetName)) {
            return false;
        }
        if (!((FeedActivity) other).getFeedName().equals(feedName)) {
            return false;
        }
        if (((FeedActivity) other).getActivityId() != (activityId)) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode() {
        return toString().hashCode();
    }

    @Override
    public String toString() {
        return dataverseName + "." + feedName + " --> " + datasetName + " " + activityId;
    }

    public String getConnectTimestamp() {
        return feedActivityDetails.get(FeedActivityDetails.FEED_CONNECT_TIMESTAMP);
    }

    public int getActivityId() {
        return activityId;
    }

    public void setActivityId(int activityId) {
        this.activityId = activityId;
    }

    public Map<String, String> getFeedActivityDetails() {
        return feedActivityDetails;
    }

    @Override
    public int compareTo(FeedActivity o) {
        return o.getActivityId() - this.activityId;
    }

}