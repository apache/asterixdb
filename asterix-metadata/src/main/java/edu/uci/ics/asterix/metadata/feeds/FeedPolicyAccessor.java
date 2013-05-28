package edu.uci.ics.asterix.metadata.feeds;

import java.util.Map;

public class FeedPolicyAccessor {
    public static final String APPLICATION_FAILURE_PERSIST_EXCEPTION = "software.failure.persist.exception";
    public static final String APPLICATION_FAILURE_CONTINUE_ON_EXCEPTION = "software.failure.continue.on.exception";
    public static final String HARDWARE_FAILURE_AUTO_RESTART = "hardware.failure.auto.restart";
    public static final String STATISTICS_COLLECT = "statistics.collect";
    public static final String STATISTICS_COLLECT_PERIOD = "statistics.collect.period";
    public static final String STATISTICS_COLLECT_PERIOD_UNIT = "statistics.collect.period.unit";
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

    public boolean persistExceptionDetailsOnApplicationFailure() {
        return getBooleanPropertyValue(APPLICATION_FAILURE_PERSIST_EXCEPTION);
    }

    public boolean continueOnApplicationFailure() {
        return getBooleanPropertyValue(APPLICATION_FAILURE_CONTINUE_ON_EXCEPTION);
    }

    public boolean autoRestartOnHardwareFailure() {
        return getBooleanPropertyValue(HARDWARE_FAILURE_AUTO_RESTART);
    }

    public boolean collectStatistics() {
        return getBooleanPropertyValue(STATISTICS_COLLECT);
    }

    public long getStatisicsCollectionPeriodInSecs() {
        return getIntegerPropertyValue(STATISTICS_COLLECT_PERIOD) * getTimeUnitFactor();
    }

    public boolean isElastic() {
        return getBooleanPropertyValue(ELASTIC);
    }

    private int getTimeUnitFactor() {
        String v = feedPolicy.get(STATISTICS_COLLECT_PERIOD_UNIT);
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