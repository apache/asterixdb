/*
 * Copyright 2009-2010 by The Regents of the University of California
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

package edu.uci.ics.asterix.metadata.entities;

import java.util.Map;

import edu.uci.ics.asterix.metadata.MetadataCache;
import edu.uci.ics.asterix.metadata.api.IMetadataEntity;

/**
 * Metadata describing a feed activity record.
 */
public class FeedActivity implements IMetadataEntity, Comparable<FeedActivity> {

    private static final long serialVersionUID = 1L;

    private int activityId;

    private final String dataverseName;
    private final String datasetName;
    private final String feedName;

    private String lastUpdatedTimestamp;
    private FeedActivityType activityType;
    private Map<String, String> feedActivityDetails;

    public static enum FeedActivityType {
        FEED_BEGIN,
        FEED_FAILURE,
        FEED_END
    }

    public static class FeedActivityDetails {
        public static final String COMPUTE_LOCATIONS = "compute-locations";
        public static final String INGEST_LOCATIONS = "ingest-locations";
        public static final String STORAGE_LOCATIONS = "storage-locations";
        public static final String TOTAL_INGESTED = "total-ingested";
        public static final String INGESTION_RATE = "ingestion-rate";
        public static final String EXCEPTION_LOCATION = "exception-location";
        public static final String EXCEPTION_MESSAGE = "exception-message";
        public static final String FEED_POLICY_NAME = "feed-policy-name";
        public static final String SUPER_FEED_MANAGER_HOST = "super-feed-manager-host";
        public static final String SUPER_FEED_MANAGER_PORT = "super-feed-manager-port";
        public static final String FEED_NODE_FAILURE = "feed-node-failure";

    }

    public FeedActivity(String dataverseName, String feedName, String datasetName, FeedActivityType feedActivityType,
            Map<String, String> feedActivityDetails) {
        this.dataverseName = dataverseName;
        this.feedName = feedName;
        this.datasetName = datasetName;
        this.activityType = feedActivityType;
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
    public Object addToCache(MetadataCache cache) {
        return cache.addFeedActivityIfNotExists(this);
    }

    @Override
    public Object dropFromCache(MetadataCache cache) {
        return cache.dropFeedActivity(this);
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
        if (!((FeedActivity) other).getFeedActivityType().equals(activityType)) {
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
        return dataverseName + "." + feedName + " --> " + datasetName + " " + activityType + " " + activityId;
    }

    public FeedActivityType getFeedActivityType() {
        return activityType;
    }

    public void setFeedActivityType(FeedActivityType feedActivityType) {
        this.activityType = feedActivityType;
    }

    public String getLastUpdatedTimestamp() {
        return lastUpdatedTimestamp;
    }

    public void setLastUpdatedTimestamp(String lastUpdatedTimestamp) {
        this.lastUpdatedTimestamp = lastUpdatedTimestamp;
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

    public void setFeedActivityDetails(Map<String, String> feedActivityDetails) {
        this.feedActivityDetails = feedActivityDetails;
    }

    public FeedActivityType getActivityType() {
        return activityType;
    }

    public void setActivityType(FeedActivityType activityType) {
        this.activityType = activityType;
    }

    @Override
    public int compareTo(FeedActivity o) {
        return o.getActivityId() - this.activityId;
    }

}