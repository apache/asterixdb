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
package org.apache.asterix.metadata.feeds;

import java.util.HashMap;
import java.util.Map;

import org.apache.asterix.external.feed.policy.FeedPolicyAccessor;
import org.apache.asterix.metadata.MetadataManager;
import org.apache.asterix.metadata.MetadataTransactionContext;
import org.apache.asterix.metadata.entities.FeedPolicyEntity;
import org.apache.asterix.metadata.utils.MetadataConstants;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class BuiltinFeedPolicies {

    private static final Logger LOGGER = LogManager.getLogger();

    public static final FeedPolicyEntity BASIC = initializeBasicPolicy();

    public static final FeedPolicyEntity ADVANCED_FT_DISCARD = initializeAdvancedFTDiscardPolicy();

    public static final FeedPolicyEntity ADVANCED_FT_SPILL = initializeAdvancedFTSpillPolicy();

    public static final FeedPolicyEntity ELASTIC = initializeAdvancedFTElasticPolicy();

    private static final FeedPolicyEntity[] POLICIES =
            new FeedPolicyEntity[] { BASIC, ADVANCED_FT_DISCARD, ADVANCED_FT_SPILL, ELASTIC };

    public static final FeedPolicyEntity DEFAULT_POLICY = BASIC;

    public static final String CONFIG_FEED_POLICY_KEY = "policy";

    private BuiltinFeedPolicies() {
    }

    public static FeedPolicyEntity getFeedPolicy(String policyName) {
        for (FeedPolicyEntity policy : POLICIES) {
            if (policy.getPolicyName().equalsIgnoreCase(policyName)) {
                return policy;
            }
        }
        return null;
    }

    // Basic
    private static FeedPolicyEntity initializeBasicPolicy() {
        Map<String, String> policyParams = new HashMap<>();
        policyParams.put(FeedPolicyAccessor.ELASTIC, Boolean.toString(false));

        String description = "Basic";
        return new FeedPolicyEntity(MetadataConstants.METADATA_DATAVERSE_NAME, "Basic", description, policyParams);
    }

    // Discard
    private static FeedPolicyEntity initializeAdvancedFTDiscardPolicy() {
        Map<String, String> policyParams = new HashMap<>();
        policyParams.put(FeedPolicyAccessor.ELASTIC, Boolean.toString(false));
        policyParams.put(FeedPolicyAccessor.FLOWCONTROL_ENABLED, Boolean.toString(true));
        policyParams.put(FeedPolicyAccessor.MAX_SPILL_SIZE_ON_DISK, Boolean.toString(false));
        policyParams.put(FeedPolicyAccessor.MAX_FRACTION_DISCARD, "100");
        policyParams.put(FeedPolicyAccessor.LOGGING_STATISTICS, Boolean.toString(true));

        String description = "FlowControl 100% Discard during congestion";
        return new FeedPolicyEntity(MetadataConstants.METADATA_DATAVERSE_NAME, "Discard", description, policyParams);
    }

    // Spill
    private static FeedPolicyEntity initializeAdvancedFTSpillPolicy() {
        Map<String, String> policyParams = new HashMap<>();
        policyParams.put(FeedPolicyAccessor.ELASTIC, Boolean.toString(false));
        policyParams.put(FeedPolicyAccessor.FLOWCONTROL_ENABLED, Boolean.toString(true));
        policyParams.put(FeedPolicyAccessor.SPILL_TO_DISK_ON_CONGESTION, "" + Boolean.TRUE);
        policyParams.put(FeedPolicyAccessor.MAX_SPILL_SIZE_ON_DISK, "" + Long.toString(FeedPolicyAccessor.NO_LIMIT));

        String description = "FlowControl 100% Spill during congestion";
        return new FeedPolicyEntity(MetadataConstants.METADATA_DATAVERSE_NAME, "Spill", description, policyParams);
    }

    // AdvancedFT_Elastic
    private static FeedPolicyEntity initializeAdvancedFTElasticPolicy() {
        Map<String, String> policyParams = new HashMap<>();
        policyParams.put(FeedPolicyAccessor.ELASTIC, Boolean.toString(true));
        policyParams.put(FeedPolicyAccessor.FLOWCONTROL_ENABLED, Boolean.toString(true));
        policyParams.put(FeedPolicyAccessor.LOGGING_STATISTICS, Boolean.toString(true));
        String description = "Basic Monitored Fault-Tolerant Elastic";
        return new FeedPolicyEntity(MetadataConstants.METADATA_DATAVERSE_NAME, "AdvancedFT_Elastic", description,
                policyParams);
    }

    public static void insertInitialFeedPolicies(MetadataTransactionContext mdTxnCtx) throws AlgebricksException {
        for (FeedPolicyEntity feedPolicy : BuiltinFeedPolicies.POLICIES) {
            MetadataManager.INSTANCE.addFeedPolicy(mdTxnCtx, feedPolicy);
        }
        LOGGER.info("Finished adding built-in feed policies.");
    }

}
