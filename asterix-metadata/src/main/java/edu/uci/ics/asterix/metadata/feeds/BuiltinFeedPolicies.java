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

import edu.uci.ics.asterix.metadata.bootstrap.MetadataConstants;
import edu.uci.ics.asterix.metadata.entities.FeedPolicy;

public class BuiltinFeedPolicies {

    public static final FeedPolicy BRITTLE = initializeBrittlePolicy();

    public static final FeedPolicy BASIC = initializeBasicPolicy();

    public static final FeedPolicy BASIC_MONITORED = initializeBasicMonitoredPolicy();

    public static final FeedPolicy FAULT_TOLERANT_BASIC_MONITORED = initializeFaultTolerantBasicMonitoredPolicy();

    public static final FeedPolicy ELASTIC = initializeFaultTolerantBasicMonitoredElasticPolicy();

    public static final FeedPolicy[] policies = new FeedPolicy[] { BRITTLE, BASIC, BASIC_MONITORED,
            FAULT_TOLERANT_BASIC_MONITORED, ELASTIC };

    public static final FeedPolicy DEFAULT_POLICY = BASIC;

    public static final String CONFIG_FEED_POLICY_KEY = "policy";

    public static FeedPolicy getFeedPolicy(String policyName) {
        for (FeedPolicy policy : policies) {
            if (policy.getPolicyName().equalsIgnoreCase(policyName)) {
                return policy;
            }
        }
        return null;
    }

    // BMFE
    private static FeedPolicy initializeFaultTolerantBasicMonitoredElasticPolicy() {
        Map<String, String> policyParams = new HashMap<String, String>();
        policyParams.put(FeedPolicyAccessor.FAILURE_LOG_ERROR, "true");
        policyParams.put(FeedPolicyAccessor.APPLICATION_FAILURE_CONTINUE, "true");
        policyParams.put(FeedPolicyAccessor.APPLICATION_FAILURE_LOG_DATA, "true");
        policyParams.put(FeedPolicyAccessor.HARDWARE_FAILURE_CONTINUE, "true");
        policyParams.put(FeedPolicyAccessor.CLUSTER_REBOOT_AUTO_RESTART, "true");
        policyParams.put(FeedPolicyAccessor.COLLECT_STATISTICS, "true");
        policyParams.put(FeedPolicyAccessor.COLLECT_STATISTICS_PERIOD, "60");
        policyParams.put(FeedPolicyAccessor.COLLECT_STATISTICS_PERIOD_UNIT, FeedPolicyAccessor.TimeUnit.SEC.name());
        policyParams.put(FeedPolicyAccessor.ELASTIC, "true");
        String description = "Basic Monitored Fault-Tolerant Elastic";
        return new FeedPolicy(MetadataConstants.METADATA_DATAVERSE_NAME, "BMFE", description, policyParams);
    }

    //BMF
    private static FeedPolicy initializeFaultTolerantBasicMonitoredPolicy() {
        Map<String, String> policyParams = new HashMap<String, String>();
        policyParams.put(FeedPolicyAccessor.FAILURE_LOG_ERROR, "true");
        policyParams.put(FeedPolicyAccessor.APPLICATION_FAILURE_CONTINUE, "true");
        policyParams.put(FeedPolicyAccessor.APPLICATION_FAILURE_LOG_DATA, "true");
        policyParams.put(FeedPolicyAccessor.HARDWARE_FAILURE_CONTINUE, "true");
        policyParams.put(FeedPolicyAccessor.CLUSTER_REBOOT_AUTO_RESTART, "true");
        policyParams.put(FeedPolicyAccessor.COLLECT_STATISTICS, "true");
        policyParams.put(FeedPolicyAccessor.COLLECT_STATISTICS_PERIOD, "60");
        policyParams.put(FeedPolicyAccessor.COLLECT_STATISTICS_PERIOD_UNIT, FeedPolicyAccessor.TimeUnit.SEC.name());
        policyParams.put(FeedPolicyAccessor.ELASTIC, "false");
        String description = "Basic Monitored Fault-Tolerant";
        return new FeedPolicy(MetadataConstants.METADATA_DATAVERSE_NAME, "BMF", description, policyParams);
    }

    //BM
    private static FeedPolicy initializeBasicMonitoredPolicy() {
        Map<String, String> policyParams = new HashMap<String, String>();
        policyParams.put(FeedPolicyAccessor.FAILURE_LOG_ERROR, "false");
        policyParams.put(FeedPolicyAccessor.APPLICATION_FAILURE_CONTINUE, "true");
        policyParams.put(FeedPolicyAccessor.APPLICATION_FAILURE_LOG_DATA, "true");
        policyParams.put(FeedPolicyAccessor.HARDWARE_FAILURE_CONTINUE, "false");
        policyParams.put(FeedPolicyAccessor.CLUSTER_REBOOT_AUTO_RESTART, "true");
        policyParams.put(FeedPolicyAccessor.COLLECT_STATISTICS, "true");
        policyParams.put(FeedPolicyAccessor.COLLECT_STATISTICS_PERIOD, "60");
        policyParams.put(FeedPolicyAccessor.COLLECT_STATISTICS_PERIOD_UNIT, FeedPolicyAccessor.TimeUnit.SEC.name());
        policyParams.put(FeedPolicyAccessor.ELASTIC, "false");
        String description = "Basic Monitored Fault-Tolerant";
        return new FeedPolicy(MetadataConstants.METADATA_DATAVERSE_NAME, "BM", description, policyParams);
    }

    //B
    private static FeedPolicy initializeBasicPolicy() {
        Map<String, String> policyParams = new HashMap<String, String>();
        policyParams.put(FeedPolicyAccessor.FAILURE_LOG_ERROR, "true");
        policyParams.put(FeedPolicyAccessor.APPLICATION_FAILURE_CONTINUE, "true");
        policyParams.put(FeedPolicyAccessor.APPLICATION_FAILURE_LOG_DATA, "false");
        policyParams.put(FeedPolicyAccessor.CLUSTER_REBOOT_AUTO_RESTART, "true");
        policyParams.put(FeedPolicyAccessor.COLLECT_STATISTICS, "false");
        policyParams.put(FeedPolicyAccessor.ELASTIC, "false");
        String description = "Basic";
        return new FeedPolicy(MetadataConstants.METADATA_DATAVERSE_NAME, "B", description, policyParams);
    }

    //Br
    private static FeedPolicy initializeBrittlePolicy() {
        Map<String, String> policyParams = new HashMap<String, String>();
        policyParams.put(FeedPolicyAccessor.FAILURE_LOG_ERROR, "false");
        policyParams.put(FeedPolicyAccessor.APPLICATION_FAILURE_CONTINUE, "false");
        policyParams.put(FeedPolicyAccessor.APPLICATION_FAILURE_LOG_DATA, "false");
        policyParams.put(FeedPolicyAccessor.HARDWARE_FAILURE_CONTINUE, "false");
        policyParams.put(FeedPolicyAccessor.CLUSTER_REBOOT_AUTO_RESTART, "false");
        policyParams.put(FeedPolicyAccessor.COLLECT_STATISTICS, "false");
        policyParams.put(FeedPolicyAccessor.ELASTIC, "false");
        String description = "Brittle";
        return new FeedPolicy(MetadataConstants.METADATA_DATAVERSE_NAME, "Br", description, policyParams);
    }

}
