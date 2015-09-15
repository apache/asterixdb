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
package org.apache.asterix.common.feeds;

import java.util.List;

import org.apache.commons.lang3.StringUtils;

/**
 * Represents a unique identifier for a Feed Joint. A Feed joint is a logical entity located
 * along a feed ingestion pipeline at a point where the tuples moving as part of data flow
 * constitute the feed. The feed joint acts as a network tap and allows the flowing data to be
 * routed to multiple paths.
 */
public class FeedJointKey {

    private final FeedId primaryFeedId;
    private final List<String> appliedFunctions;
    private final String stringRep;

    public FeedJointKey(FeedId feedId, List<String> appliedFunctions) {
        this.primaryFeedId = feedId;
        this.appliedFunctions = appliedFunctions;
        StringBuilder builder = new StringBuilder();
        builder.append(feedId);
        builder.append(":");
        builder.append(StringUtils.join(appliedFunctions, ':'));
        stringRep = builder.toString();
    }

    public FeedId getFeedId() {
        return primaryFeedId;
    }

    public List<String> getAppliedFunctions() {
        return appliedFunctions;
    }

    public String getStringRep() {
        return stringRep;
    }

    @Override
    public final String toString() {
        return stringRep;
    }

    @Override
    public int hashCode() {
        return stringRep.hashCode();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || !(o instanceof FeedJointKey)) {
            return false;
        }
        return stringRep.equals(((FeedJointKey) o).stringRep);
    }

}