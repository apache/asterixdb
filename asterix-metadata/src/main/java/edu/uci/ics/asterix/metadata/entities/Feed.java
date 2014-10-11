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

import java.util.Map;

import edu.uci.ics.asterix.common.functions.FunctionSignature;
import edu.uci.ics.asterix.metadata.MetadataCache;
import edu.uci.ics.asterix.metadata.api.IMetadataEntity;

/**
 * Metadata describing a feed.
 */
public class Feed implements IMetadataEntity {

    private static final long serialVersionUID = 1L;

    private final String dataverseName;
    private final String feedName;
    private final String adapterName;
    private final Map<String, String> adapterConfiguration;
    private final FunctionSignature appliedFunction;

    public Feed(String dataverseName, String datasetName, String adapterName, Map<String, String> adapterConfiguration,
            FunctionSignature appliedFunction) {
        this.dataverseName = dataverseName;
        this.feedName = datasetName;
        this.adapterName = adapterName;
        this.adapterConfiguration = adapterConfiguration;
        this.appliedFunction = appliedFunction;
    }

    public String getDataverseName() {
        return dataverseName;
    }

    public String getFeedName() {
        return feedName;
    }

    public String getAdapterName() {
        return adapterName;
    }

    public Map<String, String> getAdapterConfiguration() {
        return adapterConfiguration;
    }

    public FunctionSignature getAppliedFunction() {
        return appliedFunction;
    }

    @Override
    public Object addToCache(MetadataCache cache) {
        return cache.addFeedIfNotExists(this);
    }

    @Override
    public Object dropFromCache(MetadataCache cache) {
        return cache.dropFeed(this);
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }
        if (!(other instanceof Feed)) {
            return false;
        }
        Feed otherDataset = (Feed) other;
        if (!otherDataset.dataverseName.equals(dataverseName)) {
            return false;
        }
        if (!otherDataset.feedName.equals(feedName)) {
            return false;
        }
        return true;
    }
}