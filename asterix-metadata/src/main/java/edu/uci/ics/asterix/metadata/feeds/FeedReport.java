package edu.uci.ics.asterix.metadata.feeds;

import java.util.HashMap;
import java.util.Map;

import edu.uci.ics.asterix.common.feeds.FeedConnectionId;
import edu.uci.ics.asterix.common.feeds.FeedRuntime.FeedRuntimeType;
import edu.uci.ics.asterix.common.feeds.SuperFeedManager.FeedReportMessageType;

public class FeedReport implements Comparable {

    private FeedConnectionId feedId;
    private FeedReportMessageType reportType;
    private int partition = -1;
    private FeedRuntimeType runtimeType;
    private long value = -1;
    private String[] representation;

    public FeedReport() {
    }

    public FeedReport(String message) {
        representation = message.split("\\|");
    }

    public void reset(String message) {
        representation = message.split("\\|");
        reportType = null;
        feedId = null;
        runtimeType = null;
        partition = -1;
        value = -1;
    }

    @Override
    public String toString() {
        return getFeedId() + " " + getReportType() + " " + getPartition() + " " + getRuntimeType() + " " + getValue();
    }

    public FeedConnectionId getFeedId() {
        if (feedId == null) {
            String feedIdRep = representation[1];
            String[] feedIdComp = feedIdRep.split(":");
            feedId = new FeedConnectionId(feedIdComp[0], feedIdComp[1], feedIdComp[2]);
        }
        return feedId;
    }

    public FeedReportMessageType getReportType() {
        if (reportType == null) {
            reportType = FeedReportMessageType.valueOf(representation[0].toUpperCase());
        }
        return reportType;
    }

    public int getPartition() {
        if (partition < 0) {
            partition = Integer.parseInt(representation[3]);
        }
        return partition;
    }

    public FeedRuntimeType getRuntimeType() {
        if (runtimeType == null) {
            runtimeType = FeedRuntimeType.valueOf(representation[2].toUpperCase());
        }
        return runtimeType;
    }

    public long getValue() {
        if (value < 0) {
            value = Long.parseLong(representation[4]);
        }
        return value;
    }

    public String[] getRepresentation() {
        return representation;
    }

    @Override
    public int compareTo(Object o) {
        if (!(o instanceof FeedReport)) {
            throw new IllegalArgumentException("Incorrect operand type " + o);
        }

        FeedReport other = (FeedReport) o;
        if (!other.getReportType().equals(getReportType())) {
            throw new IllegalArgumentException("Incorrect operand type " + o);
        }

        int returnValue = 0;

        switch (getReportType()) {
            case CONGESTION:
                returnValue = ranking.get(getRuntimeType()) - ranking.get(other.getRuntimeType());
                break;

            case THROUGHPUT:
                returnValue = (int) (other.getValue() - getValue());
                break;
        }

        return returnValue;
    }

    private static Map<FeedRuntimeType, Integer> ranking = populateRanking();

    private static Map<FeedRuntimeType, Integer> populateRanking() {
        Map<FeedRuntimeType, Integer> ranking = new HashMap<FeedRuntimeType, Integer>();
        ranking.put(FeedRuntimeType.INGESTION, 1);
        ranking.put(FeedRuntimeType.COMPUTE, 2);
        ranking.put(FeedRuntimeType.STORAGE, 3);
        ranking.put(FeedRuntimeType.COMMIT, 4);
        return ranking;
    }
}
