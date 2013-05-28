package edu.uci.ics.asterix.metadata.feeds;

import java.util.HashMap;
import java.util.Map;

import edu.uci.ics.asterix.metadata.bootstrap.MetadataConstants;
import edu.uci.ics.asterix.metadata.entities.FeedPolicy;

public class BuiltinFeedPolicies {

    public static final FeedPolicy MISSTION_CRITICAL = initializeMissionCriticalFeedPolicy();

    public static final FeedPolicy ADVANCED = initializeAdvancedFeedPolicy();

    public static final FeedPolicy BASIC_MONITORED = initializeBasicMonitoredPolicy();

    public static final FeedPolicy BASIC = initializeBasicPolicy();

    public static final FeedPolicy[] policies = new FeedPolicy[] { MISSTION_CRITICAL, ADVANCED, BASIC_MONITORED, BASIC };

    public static final FeedPolicy DEFAULT_POLICY = BASIC_MONITORED;

    public static final String CONFIG_FEED_POLICY_KEY = "policy";

    public static FeedPolicy getFeedPolicy(String policyName) {
        for (FeedPolicy policy : policies) {
            if (policy.getPolicyName().equalsIgnoreCase(policyName)) {
                return policy;
            }
        }
        return null;
    }

    private static FeedPolicy initializeMissionCriticalFeedPolicy() {
        Map<String, String> policyParams = new HashMap<String, String>();
        policyParams.put(FeedPolicyAccessor.APPLICATION_FAILURE_PERSIST_EXCEPTION, "true");
        policyParams.put(FeedPolicyAccessor.APPLICATION_FAILURE_CONTINUE_ON_EXCEPTION, "true");
        policyParams.put(FeedPolicyAccessor.HARDWARE_FAILURE_AUTO_RESTART, "true");
        policyParams.put(FeedPolicyAccessor.STATISTICS_COLLECT, "true");
        policyParams.put(FeedPolicyAccessor.STATISTICS_COLLECT_PERIOD, "60");
        policyParams.put(FeedPolicyAccessor.STATISTICS_COLLECT_PERIOD_UNIT, FeedPolicyAccessor.TimeUnit.SEC.name());
        policyParams.put(FeedPolicyAccessor.ELASTIC, "false");
        String description = "MissionCritical";
        return new FeedPolicy(MetadataConstants.METADATA_DATAVERSE_NAME, "MissionCritical", description, policyParams);
    }

    private static FeedPolicy initializeBasicPolicy() {
        Map<String, String> policyParams = new HashMap<String, String>();
        policyParams.put(FeedPolicyAccessor.APPLICATION_FAILURE_PERSIST_EXCEPTION, "false");
        policyParams.put(FeedPolicyAccessor.APPLICATION_FAILURE_CONTINUE_ON_EXCEPTION, "false");
        policyParams.put(FeedPolicyAccessor.HARDWARE_FAILURE_AUTO_RESTART, "false");
        policyParams.put(FeedPolicyAccessor.STATISTICS_COLLECT, "false");
        policyParams.put(FeedPolicyAccessor.ELASTIC, "false");
        String description = "Basic";
        return new FeedPolicy(MetadataConstants.METADATA_DATAVERSE_NAME, "Basic", description, policyParams);
    }

    private static FeedPolicy initializeBasicMonitoredPolicy() {
        Map<String, String> policyParams = new HashMap<String, String>();
        policyParams.put(FeedPolicyAccessor.APPLICATION_FAILURE_PERSIST_EXCEPTION, "true");
        policyParams.put(FeedPolicyAccessor.APPLICATION_FAILURE_CONTINUE_ON_EXCEPTION, "false");
        policyParams.put(FeedPolicyAccessor.HARDWARE_FAILURE_AUTO_RESTART, "false");
        policyParams.put(FeedPolicyAccessor.STATISTICS_COLLECT, "true");
        policyParams.put(FeedPolicyAccessor.STATISTICS_COLLECT_PERIOD, "5");
        policyParams.put(FeedPolicyAccessor.STATISTICS_COLLECT_PERIOD_UNIT, FeedPolicyAccessor.TimeUnit.SEC.name());
        policyParams.put(FeedPolicyAccessor.ELASTIC, "false");
        String description = "BasicMonitored";
        return new FeedPolicy(MetadataConstants.METADATA_DATAVERSE_NAME, "BasicMonitored", description, policyParams);
    }

    private static FeedPolicy initializeAdvancedFeedPolicy() {
        Map<String, String> policyParams = new HashMap<String, String>();
        policyParams.put(FeedPolicyAccessor.APPLICATION_FAILURE_PERSIST_EXCEPTION, "true");
        policyParams.put(FeedPolicyAccessor.APPLICATION_FAILURE_CONTINUE_ON_EXCEPTION, "true");
        policyParams.put(FeedPolicyAccessor.HARDWARE_FAILURE_AUTO_RESTART, "false");
        policyParams.put(FeedPolicyAccessor.STATISTICS_COLLECT, "true");
        policyParams.put(FeedPolicyAccessor.STATISTICS_COLLECT_PERIOD, "60");
        policyParams.put(FeedPolicyAccessor.STATISTICS_COLLECT_PERIOD_UNIT, FeedPolicyAccessor.TimeUnit.SEC.name());
        policyParams.put(FeedPolicyAccessor.ELASTIC, "false");
        String description = "Advanced";
        return new FeedPolicy(MetadataConstants.METADATA_DATAVERSE_NAME, "Advanced", description, policyParams);
    }
}
