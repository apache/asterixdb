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
 * A unique identifier for a feed connection. A feed connection is an instance of a data feed that is flowing into a dataset.
 */
public class FeedConnectionId implements Serializable {

    private static final long serialVersionUID = 1L;

    private final FeedId feedId;
    private final String datasetName;

    public FeedConnectionId(FeedId feedId, String datasetName) {
        this.feedId = feedId;
        this.datasetName = datasetName;
    }

    public FeedConnectionId(String dataverse, String feedName, String datasetName) {
        this.feedId = new FeedId(dataverse, feedName);
        this.datasetName = datasetName;
    }

    public FeedId getFeedId() {
        return feedId;
    }

    public String getDatasetName() {
        return datasetName;
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || !(o instanceof FeedConnectionId)) {
            return false;
        }

        if (this == o
                || (((FeedConnectionId) o).getFeedId().equals(feedId) && ((FeedConnectionId) o).getDatasetName()
                        .equals(datasetName))) {
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
        return feedId.toString() + "-->" + datasetName;
    }
}
