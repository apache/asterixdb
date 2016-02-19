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

import org.apache.asterix.external.feed.policy.FeedPolicy;
import org.apache.asterix.metadata.MetadataCache;
import org.apache.asterix.metadata.api.IMetadataEntity;

/**
 * Metadata describing a feed activity record.
 */
public class FeedPolicyEntity extends FeedPolicy implements IMetadataEntity<FeedPolicyEntity> {

    public FeedPolicyEntity(String dataverseName, String policyName, String description,
            Map<String, String> properties) {
        super(dataverseName, policyName, description, properties);
    }

    private static final long serialVersionUID = 1L;

    @Override
    public FeedPolicyEntity addToCache(MetadataCache cache) {
        return null;
    }

    @Override
    public FeedPolicyEntity dropFromCache(MetadataCache cache) {
        return null;
    }
}
