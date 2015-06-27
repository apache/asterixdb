package edu.uci.ics.asterix.common.feeds;

import edu.uci.ics.asterix.common.feeds.api.IFeedMetricCollector.MetricType;

public abstract class Series {

    protected final MetricType type;
    protected int runningSum;

    public Series(MetricType type) {
        this.type = type;
    }

    public abstract void addValue(int value);

    public int getRunningSum() {
        return runningSum;
    }

    public MetricType getType() {
        return type;
    }

    public abstract void reset();

}
