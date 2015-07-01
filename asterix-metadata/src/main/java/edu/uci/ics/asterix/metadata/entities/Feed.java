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

package edu.uci.ics.asterix.metadata.entities;

import edu.uci.ics.asterix.common.feeds.FeedId;
import edu.uci.ics.asterix.common.functions.FunctionSignature;
import edu.uci.ics.asterix.metadata.MetadataCache;
import edu.uci.ics.asterix.metadata.api.IMetadataEntity;

/**
 * Feed POJO
 */
public class Feed implements IMetadataEntity {

    private static final long serialVersionUID = 1L;

    /** A unique identifier for the feed */
    protected final FeedId feedId;

    /** The function that is to be applied on each incoming feed tuple **/
    protected final FunctionSignature appliedFunction;

    /** The type {@code FeedType} associated with the feed. **/
    protected final FeedType feedType;

    /** A string representation of the instance **/
    protected final String displayName;

    public enum FeedType {
        /**
         * A feed that derives its data from an external source.
         */
        PRIMARY,

        /**
         * A feed that derives its data from another primary or secondary feed.
         */
        SECONDARY
    }

    public Feed(String dataverseName, String datasetName, FunctionSignature appliedFunction, FeedType feedType) {
        this.feedId = new FeedId(dataverseName, datasetName);
        this.appliedFunction = appliedFunction;
        this.feedType = feedType;
        this.displayName = feedType + "(" + feedId + ")";
    }

    public FeedId getFeedId() {
        return feedId;
    }

    public String getDataverseName() {
        return feedId.getDataverse();
    }

    public String getFeedName() {
        return feedId.getFeedName();
    }

    public FunctionSignature getAppliedFunction() {
        return appliedFunction;
    }

    public FeedType getFeedType() {
        return feedType;
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
        return feedType + "(" + feedId + ")";
    }

    @Override
    public Object addToCache(MetadataCache cache) {
        return cache.addFeedIfNotExists(this);
    }

    @Override
    public Object dropFromCache(MetadataCache cache) {
        return cache.dropFeed(this);
    }
}