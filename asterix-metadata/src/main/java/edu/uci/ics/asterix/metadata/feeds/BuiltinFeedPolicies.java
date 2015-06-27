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
package edu.uci.ics.asterix.metadata.feeds;

import java.util.HashMap;
import java.util.Map;

import edu.uci.ics.asterix.common.feeds.FeedPolicyAccessor;
import edu.uci.ics.asterix.metadata.bootstrap.MetadataConstants;
import edu.uci.ics.asterix.metadata.entities.FeedPolicy;

public class BuiltinFeedPolicies {

    public static final FeedPolicy BRITTLE = initializeBrittlePolicy();

    public static final FeedPolicy BASIC = initializeBasicPolicy();

    public static final FeedPolicy BASIC_FT = initializeBasicFTPolicy();

    public static final FeedPolicy ADVANCED_FT = initializeAdvancedFTPolicy();

    public static final FeedPolicy ADVANCED_FT_DISCARD = initializeAdvancedFTDiscardPolicy();

    public static final FeedPolicy ADVANCED_FT_SPILL = initializeAdvancedFTSpillPolicy();

    public static final FeedPolicy ADVANCED_FT_THROTTLE = initializeAdvancedFTThrottlePolicy();

    public static final FeedPolicy ELASTIC = initializeAdvancedFTElasticPolicy();

    public static final FeedPolicy[] policies = new FeedPolicy[] { BRITTLE, BASIC, BASIC_FT, ADVANCED_FT,
            ADVANCED_FT_DISCARD, ADVANCED_FT_SPILL, ADVANCED_FT_THROTTLE, ELASTIC };

    public static final FeedPolicy DEFAULT_POLICY = BASIC_FT;

    public static final String CONFIG_FEED_POLICY_KEY = "policy";

    public static FeedPolicy getFeedPolicy(String policyName) {
        for (FeedPolicy policy : policies) {
            if (policy.getPolicyName().equalsIgnoreCase(policyName)) {
                return policy;
            }
        }
        return null;
    }

    //Brittle
    private static FeedPolicy initializeBrittlePolicy() {
        Map<String, String> policyParams = new HashMap<String, String>();
        policyParams.put(FeedPolicyAccessor.SOFT_FAILURE_CONTINUE, "false");
        policyParams.put(FeedPolicyAccessor.SOFT_FAILURE_LOG_DATA, "false");
        policyParams.put(FeedPolicyAccessor.HARDWARE_FAILURE_CONTINUE, "false");
        policyParams.put(FeedPolicyAccessor.CLUSTER_REBOOT_AUTO_RESTART, "false");
        policyParams.put(FeedPolicyAccessor.ELASTIC, "false");
        policyParams.put(FeedPolicyAccessor.TIME_TRACKING, "false");
        policyParams.put(FeedPolicyAccessor.AT_LEAST_ONE_SEMANTICS, "false");

        String description = "Brittle";
        return new FeedPolicy(MetadataConstants.METADATA_DATAVERSE_NAME, "Brittle", description, policyParams);
    }

    //Basic
    private static FeedPolicy initializeBasicPolicy() {
        Map<String, String> policyParams = new HashMap<String, String>();
        policyParams.put(FeedPolicyAccessor.SOFT_FAILURE_CONTINUE, "false");
        policyParams.put(FeedPolicyAccessor.SOFT_FAILURE_LOG_DATA, "true");
        policyParams.put(FeedPolicyAccessor.CLUSTER_REBOOT_AUTO_RESTART, "true");
        policyParams.put(FeedPolicyAccessor.ELASTIC, "false");
        policyParams.put(FeedPolicyAccessor.TIME_TRACKING, "false");
        policyParams.put(FeedPolicyAccessor.AT_LEAST_ONE_SEMANTICS, "false");

        String description = "Basic";
        return new FeedPolicy(MetadataConstants.METADATA_DATAVERSE_NAME, "Basic", description, policyParams);
    }

    // BasicFT
    private static FeedPolicy initializeBasicFTPolicy() {
        Map<String, String> policyParams = new HashMap<String, String>();
        policyParams.put(FeedPolicyAccessor.SOFT_FAILURE_CONTINUE, "true");
        policyParams.put(FeedPolicyAccessor.SOFT_FAILURE_LOG_DATA, "true");
        policyParams.put(FeedPolicyAccessor.HARDWARE_FAILURE_CONTINUE, "false");
        policyParams.put(FeedPolicyAccessor.CLUSTER_REBOOT_AUTO_RESTART, "true");
        policyParams.put(FeedPolicyAccessor.ELASTIC, "false");
        policyParams.put(FeedPolicyAccessor.SPILL_TO_DISK_ON_CONGESTION, "false");
        policyParams.put(FeedPolicyAccessor.MAX_FRACTION_DISCARD, "1");
        policyParams.put(FeedPolicyAccessor.TIME_TRACKING, "false");
        policyParams.put(FeedPolicyAccessor.AT_LEAST_ONE_SEMANTICS, "false");
        policyParams.put(FeedPolicyAccessor.THROTTLING_ENABLED, "false");

        String description = "Basic Monitored Fault-Tolerant";
        return new FeedPolicy(MetadataConstants.METADATA_DATAVERSE_NAME, "BasicFT", description, policyParams);
    }

    // AdvancedFT
    private static FeedPolicy initializeAdvancedFTPolicy() {
        Map<String, String> policyParams = new HashMap<String, String>();
        policyParams.put(FeedPolicyAccessor.SOFT_FAILURE_CONTINUE, "true");
        policyParams.put(FeedPolicyAccessor.SOFT_FAILURE_LOG_DATA, "true");
        policyParams.put(FeedPolicyAccessor.HARDWARE_FAILURE_CONTINUE, "true");
        policyParams.put(FeedPolicyAccessor.CLUSTER_REBOOT_AUTO_RESTART, "true");
        policyParams.put(FeedPolicyAccessor.ELASTIC, "false");
        policyParams.put(FeedPolicyAccessor.TIME_TRACKING, "true");
        policyParams.put(FeedPolicyAccessor.AT_LEAST_ONE_SEMANTICS, "true");

        String description = "Basic Monitored Fault-Tolerant with at least once semantics";
        return new FeedPolicy(MetadataConstants.METADATA_DATAVERSE_NAME, "AdvancedFT", description, policyParams);
    }

    // AdvancedFT_Discard
    private static FeedPolicy initializeAdvancedFTDiscardPolicy() {
        Map<String, String> policyParams = new HashMap<String, String>();
        policyParams.put(FeedPolicyAccessor.SOFT_FAILURE_CONTINUE, "true");
        policyParams.put(FeedPolicyAccessor.SOFT_FAILURE_LOG_DATA, "true");
        policyParams.put(FeedPolicyAccessor.HARDWARE_FAILURE_CONTINUE, "true");
        policyParams.put(FeedPolicyAccessor.CLUSTER_REBOOT_AUTO_RESTART, "true");
        policyParams.put(FeedPolicyAccessor.ELASTIC, "false");
        policyParams.put(FeedPolicyAccessor.MAX_SPILL_SIZE_ON_DISK, "false");
        policyParams.put(FeedPolicyAccessor.MAX_FRACTION_DISCARD, "100");
        policyParams.put(FeedPolicyAccessor.TIME_TRACKING, "false");
        policyParams.put(FeedPolicyAccessor.LOGGING_STATISTICS, "true");
       
        String description = "AdvancedFT 100% Discard during congestion";
        return new FeedPolicy(MetadataConstants.METADATA_DATAVERSE_NAME, "AdvancedFT_Discard", description,
                policyParams);
    }

    // AdvancedFT_Spill
    private static FeedPolicy initializeAdvancedFTSpillPolicy() {
        Map<String, String> policyParams = new HashMap<String, String>();
        policyParams.put(FeedPolicyAccessor.SOFT_FAILURE_CONTINUE, "true");
        policyParams.put(FeedPolicyAccessor.SOFT_FAILURE_LOG_DATA, "true");
        policyParams.put(FeedPolicyAccessor.HARDWARE_FAILURE_CONTINUE, "true");
        policyParams.put(FeedPolicyAccessor.CLUSTER_REBOOT_AUTO_RESTART, "true");
        policyParams.put(FeedPolicyAccessor.ELASTIC, "false");
        policyParams.put(FeedPolicyAccessor.SPILL_TO_DISK_ON_CONGESTION, "" + Boolean.TRUE);
        policyParams.put(FeedPolicyAccessor.MAX_SPILL_SIZE_ON_DISK, "" + FeedPolicyAccessor.NO_LIMIT);
        policyParams.put(FeedPolicyAccessor.TIME_TRACKING, "true");

        String description = "AdvancedFT 100% Discard during congestion";
        return new FeedPolicy(MetadataConstants.METADATA_DATAVERSE_NAME, "AdvancedFT_Spill", description, policyParams);
    }

    // AdvancedFT_Spill
    private static FeedPolicy initializeAdvancedFTThrottlePolicy() {
        Map<String, String> policyParams = new HashMap<String, String>();
        policyParams.put(FeedPolicyAccessor.SOFT_FAILURE_CONTINUE, "true");
        policyParams.put(FeedPolicyAccessor.SOFT_FAILURE_LOG_DATA, "true");
        policyParams.put(FeedPolicyAccessor.HARDWARE_FAILURE_CONTINUE, "true");
        policyParams.put(FeedPolicyAccessor.CLUSTER_REBOOT_AUTO_RESTART, "true");
        policyParams.put(FeedPolicyAccessor.ELASTIC, "false");
        policyParams.put(FeedPolicyAccessor.SPILL_TO_DISK_ON_CONGESTION, "" + Boolean.FALSE);
        policyParams.put(FeedPolicyAccessor.MAX_FRACTION_DISCARD, "" + 0);
        policyParams.put(FeedPolicyAccessor.TIME_TRACKING, "false");
        policyParams.put(FeedPolicyAccessor.THROTTLING_ENABLED, "true");

        String description = "AdvancedFT Throttle during congestion";
        return new FeedPolicy(MetadataConstants.METADATA_DATAVERSE_NAME, "AdvancedFT_Throttle", description,
                policyParams);
    }

    // AdvancedFT_Elastic
    private static FeedPolicy initializeAdvancedFTElasticPolicy() {
        Map<String, String> policyParams = new HashMap<String, String>();
        policyParams.put(FeedPolicyAccessor.SOFT_FAILURE_CONTINUE, "true");
        policyParams.put(FeedPolicyAccessor.SOFT_FAILURE_LOG_DATA, "true");
        policyParams.put(FeedPolicyAccessor.HARDWARE_FAILURE_CONTINUE, "true");
        policyParams.put(FeedPolicyAccessor.CLUSTER_REBOOT_AUTO_RESTART, "true");
        policyParams.put(FeedPolicyAccessor.ELASTIC, "true");
        policyParams.put(FeedPolicyAccessor.TIME_TRACKING, "false");
        policyParams.put(FeedPolicyAccessor.LOGGING_STATISTICS, "true");

        String description = "Basic Monitored Fault-Tolerant Elastic";
        return new FeedPolicy(MetadataConstants.METADATA_DATAVERSE_NAME, "AdvancedFT_Elastic", description,
                policyParams);
    }

}
