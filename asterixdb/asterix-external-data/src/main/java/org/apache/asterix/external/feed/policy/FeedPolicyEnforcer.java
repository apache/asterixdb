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
package org.apache.asterix.external.feed.policy;

import java.util.Map;

import org.apache.asterix.external.feed.management.FeedConnectionId;
import org.apache.hyracks.api.exceptions.HyracksDataException;

public class FeedPolicyEnforcer {

    private final FeedConnectionId connectionId;
    private final FeedPolicyAccessor policyAccessor;

    public FeedPolicyEnforcer(FeedConnectionId feedConnectionId, Map<String, String> feedPolicy) {
        this.connectionId = feedConnectionId;
        this.policyAccessor = new FeedPolicyAccessor(feedPolicy);
    }

    public boolean continueIngestionPostSoftwareFailure(HyracksDataException e) throws HyracksDataException {
        return policyAccessor.continueOnSoftFailure();
    }

    public FeedPolicyAccessor getFeedPolicyAccessor() {
        return policyAccessor;
    }

    public FeedConnectionId getFeedId() {
        return connectionId;
    }

}
