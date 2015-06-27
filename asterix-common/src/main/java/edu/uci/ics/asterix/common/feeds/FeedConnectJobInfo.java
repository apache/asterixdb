package edu.uci.ics.asterix.common.feeds;

import java.util.List;
import java.util.Map;

import edu.uci.ics.asterix.common.feeds.api.IFeedJoint;
import edu.uci.ics.hyracks.api.job.JobId;
import edu.uci.ics.hyracks.api.job.JobSpecification;

public class FeedConnectJobInfo extends FeedJobInfo {

    private final FeedConnectionId connectionId;
    private final Map<String, String> feedPolicy;
    private final IFeedJoint sourceFeedJoint;
    private IFeedJoint computeFeedJoint;

    private List<String> collectLocations;
    private List<String> computeLocations;
    private List<String> storageLocations;

    public FeedConnectJobInfo(JobId jobId, FeedJobState state, FeedConnectionId connectionId,
            IFeedJoint sourceFeedJoint, IFeedJoint computeFeedJoint, JobSpecification spec,
            Map<String, String> feedPolicy) {
        super(jobId, state, FeedJobInfo.JobType.FEED_CONNECT, spec);
        this.connectionId = connectionId;
        this.sourceFeedJoint = sourceFeedJoint;
        this.computeFeedJoint = computeFeedJoint;
        this.feedPolicy = feedPolicy;
    }

    public FeedConnectionId getConnectionId() {
        return connectionId;
    }

    public List<String> getCollectLocations() {
        return collectLocations;
    }

    public List<String> getComputeLocations() {
        return computeLocations;
    }

    public List<String> getStorageLocations() {
        return storageLocations;
    }

    public void setCollectLocations(List<String> collectLocations) {
        this.collectLocations = collectLocations;
    }

    public void setComputeLocations(List<String> computeLocations) {
        this.computeLocations = computeLocations;
    }

    public void setStorageLocations(List<String> storageLocations) {
        this.storageLocations = storageLocations;
    }

    public IFeedJoint getSourceFeedJoint() {
        return sourceFeedJoint;
    }

    public IFeedJoint getComputeFeedJoint() {
        return computeFeedJoint;
    }

    public Map<String, String> getFeedPolicy() {
        return feedPolicy;
    }

    public void setComputeFeedJoint(IFeedJoint computeFeedJoint) {
        this.computeFeedJoint = computeFeedJoint;
    }

}
