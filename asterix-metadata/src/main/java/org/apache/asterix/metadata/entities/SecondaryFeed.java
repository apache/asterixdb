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

import org.apache.asterix.common.functions.FunctionSignature;
import org.apache.asterix.metadata.api.IMetadataEntity;

/**
 * A secondary feed is one that derives its data from another (primary/secondary) feed.
 * This class is a holder object for the metadata associated with a secondary feed.
 */
public class SecondaryFeed extends Feed implements IMetadataEntity {

    private static final long serialVersionUID = 1L;

    private final String sourceFeedName;

    public SecondaryFeed(String dataverseName, String feedName, String sourceFeedName, FunctionSignature appliedFunction) {
        super(dataverseName, feedName, appliedFunction, FeedType.SECONDARY);
        this.sourceFeedName = sourceFeedName;
    }

    public String getSourceFeedName() {
        return sourceFeedName;
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }
        if (!super.equals(other) || !(other instanceof SecondaryFeed)) {
            return false;
        }

        SecondaryFeed otherFeed = (SecondaryFeed) other;
        if (!otherFeed.getSourceFeedName().equals(sourceFeedName)) {
            return false;
        }
        return true;
    }

    @Override
    public String toString() {
        return "SecondaryFeed (" + feedId + ")" + "<--" + "(" + sourceFeedName + ")";
    }
}