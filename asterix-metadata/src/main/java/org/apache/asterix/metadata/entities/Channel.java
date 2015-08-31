/*
 * Copyright 2009-2015 by The Regents of the University of California
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

package org.apache.asterix.metadata.entities;

import org.apache.asterix.common.functions.FunctionSignature;
import org.apache.asterix.metadata.MetadataCache;
import org.apache.asterix.metadata.api.IMetadataEntity;

/**
 * Metadata describing a channel.
 */
public class Channel implements IMetadataEntity {

    private static final long serialVersionUID = 1L;

    private final String dataverseName;
    private final String channelName;
    private final String subscriptionsDatasetName;
    private final String resultsDatasetName;
    private final String duration;
    private final FunctionSignature function;

    public Channel(String dataverseName, String channelName, String subscriptionsDataset, String resultsDataset,
            FunctionSignature function, String duration) {
        this.dataverseName = dataverseName;
        this.channelName = channelName;
        this.function = function;
        this.duration = duration;
        this.resultsDatasetName = resultsDataset;
        this.subscriptionsDatasetName = subscriptionsDataset;
    }

    public String getDataverseName() {
        return dataverseName;
    }

    public String getChannelName() {
        return channelName;
    }

    public String getSubscriptionsDataset() {
        return subscriptionsDatasetName;
    }

    public String getResultsDatasetName() {
        return resultsDatasetName;
    }

    public String getDuration() {
        return duration;
    }

    public FunctionSignature getFunction() {
        return function;
    }

    @Override
    public Object addToCache(MetadataCache cache) {
        return cache.addChannelIfNotExists(this);
    }

    @Override
    public Object dropFromCache(MetadataCache cache) {
        return cache.dropChannel(this);
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }
        if (!(other instanceof Channel)) {
            return false;
        }
        Channel otherDataset = (Channel) other;
        if (!otherDataset.dataverseName.equals(dataverseName)) {
            return false;
        }
        if (!otherDataset.channelName.equals(channelName)) {
            return false;
        }
        return true;
    }
}