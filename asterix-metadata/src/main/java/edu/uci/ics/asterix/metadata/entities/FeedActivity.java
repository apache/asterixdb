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

import java.util.List;

import edu.uci.ics.asterix.metadata.IDatasetDetails;
import edu.uci.ics.asterix.metadata.MetadataCache;
import edu.uci.ics.asterix.metadata.api.IMetadataEntity;
import edu.uci.ics.asterix.metadata.entities.FeedDatasetDetails.FeedState;

/**
 * Metadata describing a feed activity record.
 */
public class FeedActivity implements IMetadataEntity {

    private static final long serialVersionUID = 1L;

    private final String dataverseName;
    // Enforced to be unique within a dataverse.
    private final String datasetName;

    private FeedState feedState;
    private List<String> ingestNodes;
    private List<String> computeNodes;
    private String lastUpdatedTimestamp;

    public FeedActivity(String dataverseName, String datasetName, String feedState, List<String> ingestNodes,
            List<String> computeNodes) {
        this.dataverseName = dataverseName;
        this.datasetName = datasetName;
        this.feedState = FeedState.valueOf(feedState);
        this.ingestNodes = ingestNodes;
        this.computeNodes = computeNodes;
    }

    public String getDataverseName() {
        return dataverseName;
    }

    public String getDatasetName() {
        return datasetName;
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
        FeedActivity otherDataset = (FeedActivity) other;
        if (!otherDataset.dataverseName.equals(dataverseName)) {
            return false;
        }
        if (!otherDataset.datasetName.equals(datasetName)) {
            return false;
        }
        return true;
    }

    public FeedState getFeedState() {
        return feedState;
    }

    public void setFeedState(FeedState feedState) {
        this.feedState = feedState;
    }

    public List<String> getIngestNodes() {
        return ingestNodes;
    }

    public void setIngestNodes(List<String> ingestNodes) {
        this.ingestNodes = ingestNodes;
    }

    public List<String> getComputeNodes() {
        return computeNodes;
    }

    public void setComputeNodes(List<String> computeNodes) {
        this.computeNodes = computeNodes;
    }

    public String getLastUpdatedTimestamp() {
        return lastUpdatedTimestamp;
    }

    public void setLastUpdatedTimestamp(String lastUpdatedTimestamp) {
        this.lastUpdatedTimestamp = lastUpdatedTimestamp;
    }
}