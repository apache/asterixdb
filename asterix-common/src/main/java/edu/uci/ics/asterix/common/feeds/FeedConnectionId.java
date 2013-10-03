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
package edu.uci.ics.asterix.common.feeds;

import java.io.Serializable;

/**
 * A unique identifier for a data feed flowing into a dataset.
 */
public class FeedConnectionId implements Serializable {

    private static final long serialVersionUID = 1L;

    private final String dataverse;
    private final String feedName;
    private final String datasetName;

    public FeedConnectionId(String dataverse, String feedName, String datasetName) {
        this.dataverse = dataverse;
        this.feedName = feedName;
        this.datasetName = datasetName;
    }

    public String getDataverse() {
        return dataverse;
    }

    public String getFeedName() {
        return feedName;
    }

    public String getDatasetName() {
        return datasetName;
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || !(o instanceof FeedConnectionId)) {
            return false;
        }
        if (((FeedConnectionId) o).getFeedName().equals(feedName)
                && ((FeedConnectionId) o).getDataverse().equals(dataverse)
                && ((FeedConnectionId) o).getDatasetName().equals(datasetName)) {
            return true;
        }
        return false;
    }

    @Override
    public int hashCode() {
        return toString().hashCode();
    }

    @Override
    public String toString() {
        return dataverse + "." + feedName + "-->" + datasetName;
    }
}
