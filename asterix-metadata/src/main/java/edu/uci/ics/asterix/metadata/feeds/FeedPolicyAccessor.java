package edu.uci.ics.asterix.metadata.feeds;

import java.util.Map;

public class FeedPolicyAccessor {
    public static final String FAILURE_LOG_ERROR = "failure.log.error";
    public static final String APPLICATION_FAILURE_LOG_DATA = "application.failure.log.data";
    public static final String APPLICATION_FAILURE_CONTINUE = "application.failure.continue";
    public static final String HARDWARE_FAILURE_CONTINUE = "hardware.failure.continue";
    public static final String CLUSTER_REBOOT_AUTO_RESTART = "cluster.reboot.auto.restart";
    public static final String COLLECT_STATISTICS = "collect.statistics";
    public static final String COLLECT_STATISTICS_PERIOD = "collect.statistics.period";
    public static final String COLLECT_STATISTICS_PERIOD_UNIT = "collect.statistics.period.unit";
    public static final String ELASTIC = "elastic";

    public enum TimeUnit {
        SEC,
        MIN,
        HRS,
        DAYS
    }

    private Map<String, String> feedPolicy;

    public FeedPolicyAccessor(Map<String, String> feedPolicy) {
        this.feedPolicy = feedPolicy;
    }

    public boolean logErrorOnFailure() {
        return getBooleanPropertyValue(FAILURE_LOG_ERROR);
    }

    public boolean logDataOnApplicationFailure() {
        return getBooleanPropertyValue(APPLICATION_FAILURE_LOG_DATA);
    }

    public boolean continueOnApplicationFailure() {
        return getBooleanPropertyValue(APPLICATION_FAILURE_CONTINUE);
    }

    public boolean continueOnHardwareFailure() {
        return getBooleanPropertyValue(HARDWARE_FAILURE_CONTINUE);
    }

    public boolean autoRestartOnClusterReboot() {
        return getBooleanPropertyValue(CLUSTER_REBOOT_AUTO_RESTART);
    }

    public boolean collectStatistics() {
        return getBooleanPropertyValue(COLLECT_STATISTICS);
    }

    public long getStatisicsCollectionPeriodInSecs() {
        return getIntegerPropertyValue(COLLECT_STATISTICS_PERIOD) * getTimeUnitFactor();
    }

    public boolean isElastic() {
        return getBooleanPropertyValue(ELASTIC);
    }

    private int getTimeUnitFactor() {
        String v = feedPolicy.get(COLLECT_STATISTICS_PERIOD_UNIT);
        int factor = 1;
        switch (TimeUnit.valueOf(v)) {
            case SEC:
                factor = 1;
                break;
            case MIN:
                factor = 60;
                break;
            case HRS:
                factor = 3600;
                break;
            case DAYS:
                factor = 216000;
                break;

        }
        return factor;
    }

    private boolean getBooleanPropertyValue(String key) {
        String v = feedPolicy.get(key);
        return v == null ? false : Boolean.valueOf(v);
    }

    private int getIntegerPropertyValue(String key) {
        String v = feedPolicy.get(key);
        return Integer.parseInt(v);
    }
}