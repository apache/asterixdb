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

import org.apache.asterix.metadata.MetadataCache;
import org.apache.asterix.metadata.api.IMetadataEntity;

/**
 * Metadata describing a broker.
 */
public class Broker implements IMetadataEntity {

    private static final long serialVersionUID = 1L;

    private final String brokerName;
    private final String endPointName;

    public Broker(String brokerName, String endPointName) {
        this.endPointName = endPointName;
        this.brokerName = brokerName;
    }

    public String getBrokerName() {
        return brokerName;
    }

    public String getEndPointName() {
        return endPointName;
    }

    @Override
    public Object addToCache(MetadataCache cache) {
        return cache.addBrokerIfNotExists(this);
    }

    @Override
    public Object dropFromCache(MetadataCache cache) {
        return cache.dropBroker(this);
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }
        if (!(other instanceof Broker)) {
            return false;
        }
        Broker otherDataset = (Broker) other;
        if (!otherDataset.brokerName.equals(brokerName)) {
            return false;
        }
        return true;
    }
}