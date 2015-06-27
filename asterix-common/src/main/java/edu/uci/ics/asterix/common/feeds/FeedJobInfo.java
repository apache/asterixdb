package edu.uci.ics.asterix.common.feeds;

import java.util.logging.Level;
import java.util.logging.Logger;

import edu.uci.ics.hyracks.api.job.JobId;
import edu.uci.ics.hyracks.api.job.JobSpecification;

public class FeedJobInfo {

    private static final Logger LOGGER = Logger.getLogger(FeedJobInfo.class.getName());

    public enum JobType {
        INTAKE,
        FEED_CONNECT
    }

    public enum FeedJobState {
        CREATED,
        ACTIVE,
        UNDER_RECOVERY,
        ENDED
    }

    protected final JobId jobId;
    protected final JobType jobType;
    protected FeedJobState state;
    protected JobSpecification spec;

    public FeedJobInfo(JobId jobId, FeedJobState state, JobType jobType, JobSpecification spec) {
        this.jobId = jobId;
        this.state = state;
        this.jobType = jobType;
        this.spec = spec;
    }

    public JobId getJobId() {
        return jobId;
    }

    public FeedJobState getState() {
        return state;
    }

    public void setState(FeedJobState state) {
        this.state = state;
        if (LOGGER.isLoggable(Level.INFO)) {
            LOGGER.info(this + " is in " + state + " state.");
        }
    }

    public JobType getJobType() {
        return jobType;
    }

    public JobSpecification getSpec() {
        return spec;
    }

    public void setSpec(JobSpecification spec) {
        this.spec = spec;
    }

    public String toString() {
        return jobId + " [" + jobType + "]";
    }

}
