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

package org.apache.asterix.metadata.entities;

import java.util.Map;

import org.apache.asterix.active.EntityId;
import org.apache.asterix.external.feed.api.IFeed;
import org.apache.asterix.metadata.MetadataCache;
import org.apache.asterix.metadata.api.IMetadataEntity;

/**
 * Feed POJO
 */
public class Feed implements IMetadataEntity<Feed>, IFeed {
    private static final long serialVersionUID = 1L;
    public static final String EXTENSION_NAME = "Feed";

    /** A unique identifier for the feed */
    private EntityId feedId;
    /** A string representation of the instance **/
    private String displayName;
    /** Feed configurations */
    private Map<String, String> feedConfiguration;

    public Feed(String dataverseName, String feedName, Map<String, String> feedConfiguration) {
        this.feedId = new EntityId(EXTENSION_NAME, dataverseName, feedName);
        this.displayName = "(" + feedId + ")";
        this.feedConfiguration = feedConfiguration;
    }

    @Override
    public EntityId getFeedId() {
        return feedId;
    }

    @Override
    public String getDataverseName() {
        return feedId.getDataverse();
    }

    @Override
    public String getFeedName() {
        return feedId.getEntityName();
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }
        if (!(other instanceof Feed)) {
            return false;
        }
        Feed otherFeed = (Feed) other;
        return otherFeed.getFeedId().equals(feedId);
    }

    @Override
    public int hashCode() {
        return displayName.hashCode();
    }

    @Override
    public String toString() {
        return feedId.toString();
    }

    @Override
    public Feed addToCache(MetadataCache cache) {
        return cache.addFeedIfNotExists(this);
    }

    @Override
    public Feed dropFromCache(MetadataCache cache) {
        return cache.dropFeedIfExists(this);
    }

    @Override
    public Map<String, String> getConfiguration() {
        return feedConfiguration;
    }
}
