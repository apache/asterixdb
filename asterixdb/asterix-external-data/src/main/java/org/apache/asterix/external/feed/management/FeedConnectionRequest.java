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
import java.util.List;

import org.apache.asterix.active.EntityId;
import org.apache.asterix.common.functions.FunctionSignature;
import org.apache.asterix.external.util.FeedUtils.FeedRuntimeType;
import org.apache.commons.lang3.StringUtils;

/**
 * A request for connecting a feed to a dataset.
 */
public class FeedConnectionRequest implements Serializable {

    private static final long serialVersionUID = 1L;
    /** Location in the source feed pipeline from where feed tuples are received. **/
    private final FeedRuntimeType connectionLocation;
    /** List of functions that need to be applied in sequence after the data hand-off at the source feedPointKey. **/
    private final List<FunctionSignature> functionsToApply;
    /** Name of the policy that governs feed ingestion **/
    private final String policy;
    /** Target dataset associated with the connection request **/
    private final String targetDataset;
    private final EntityId receivingFeedId;

    public FeedConnectionRequest(FeedRuntimeType connectionLocation, List<FunctionSignature> functionsToApply,
            String targetDataset, String policy, EntityId receivingFeedId) {
        this.connectionLocation = connectionLocation;
        this.functionsToApply = functionsToApply;
        this.targetDataset = targetDataset;
        this.policy = policy;
        this.receivingFeedId = receivingFeedId;
    }

    public String getPolicy() {
        return policy;
    }

    public String getTargetDataset() {
        return targetDataset;
    }

    public FeedRuntimeType getSubscriptionLocation() {
        return connectionLocation;
    }

    public EntityId getReceivingFeedId() {
        return receivingFeedId;
    }

    public List<FunctionSignature> getFunctionsToApply() {
        return functionsToApply;
    }

    @Override
    public String toString() {
        return "Feed Connection Request " + receivingFeedId + " [" + connectionLocation + "]" + " Apply ("
                + StringUtils.join(functionsToApply, ",") + ")";
    }

}
