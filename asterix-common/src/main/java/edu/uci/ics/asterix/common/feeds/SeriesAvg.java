package edu.uci.ics.asterix.common.feeds;

import edu.uci.ics.asterix.common.feeds.api.IFeedMetricCollector.MetricType;

public class SeriesAvg extends Series {

    private int count;

    public SeriesAvg() {
        super(MetricType.AVG);
    }

    public int getAvg() {
        return runningSum / count;
    }

    public synchronized void addValue(int value) {
        if (value < 0) {
            return;
        }
        runningSum += value;
        count++;
    }
    
    public  void reset(){
        count = 0;
    }

}
