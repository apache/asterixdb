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
import java.util.Map.Entry;

import org.apache.asterix.common.functions.FunctionSignature;
import org.apache.asterix.metadata.api.IMetadataEntity;

/**
 * A primary feed is one that derives its data from an external source via an adaptor.
 * This class is a holder object for the metadata associated with a primary feed.
 */
public class PrimaryFeed extends Feed implements IMetadataEntity {

    private static final long serialVersionUID = 1L;

    private final String adaptorName;
    private final Map<String, String> adaptorConfiguration;

    public PrimaryFeed(String dataverseName, String datasetName, String adaptorName,
            Map<String, String> adaptorConfiguration, FunctionSignature appliedFunction) {
        super(dataverseName, datasetName, appliedFunction, FeedType.PRIMARY);
        this.adaptorName = adaptorName;
        this.adaptorConfiguration = adaptorConfiguration;
    }

    public String getAdaptorName() {
        return adaptorName;
    }

    public Map<String, String> getAdaptorConfiguration() {
        return adaptorConfiguration;
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }
        if (!super.equals(other) || !(other instanceof PrimaryFeed)) {
            return false;
        }

        PrimaryFeed otherFeed = (PrimaryFeed) other;
        if (!otherFeed.getAdaptorName().equals(adaptorName)) {
            return false;
        }

        for (Entry<String, String> entry : adaptorConfiguration.entrySet()) {
            if (!(entry.getValue().equals(otherFeed.getAdaptorConfiguration().get(entry.getKey())))) {
                return false;
            }
        }
        return true;
    }

    @Override
    public String toString() {
        return "PrimaryFeed (" + adaptorName + ")";
    }
}