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
package org.apache.asterix.external.feed.management;

import java.io.Serializable;

import org.apache.asterix.active.EntityId;

/**
 * A unique identifier for a feed connection. A feed connection is an instance of a data feed that is flowing into a
 * dataset.
 */
public class FeedConnectionId implements Serializable {

    private static final long serialVersionUID = 1L;
    private static final String FEED_EXTENSION_NAME = "Feed";

    private final EntityId feedId; // Dataverse - Feed
    private final String datasetName; // Dataset <Dataset is empty in case of no target dataset>
    private final int hash;

    public FeedConnectionId(EntityId feedId, String datasetName) {
        this.feedId = feedId;
        this.datasetName = datasetName;
        this.hash = toString().hashCode();
    }

    public FeedConnectionId(String dataverse, String feedName, String datasetName) {
        this(new EntityId(FEED_EXTENSION_NAME, dataverse, feedName), datasetName);
    }

    public EntityId getFeedId() {
        return feedId;
    }

    public String getDatasetName() {
        return datasetName;
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof FeedConnectionId)) {
            return false;
        }

        if (this == o || (((FeedConnectionId) o).getFeedId().equals(feedId)
                && ((FeedConnectionId) o).getDatasetName().equals(datasetName))) {
            return true;
        }
        return false;
    }

    @Override
    public int hashCode() {
        return hash;
    }

    @Override
    public String toString() {
        return feedId.toString() + "-->" + datasetName;
    }
}
