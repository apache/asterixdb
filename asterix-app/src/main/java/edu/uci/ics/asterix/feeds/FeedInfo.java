package edu.uci.ics.asterix.feeds;

import edu.uci.ics.hyracks.api.job.JobId;
import edu.uci.ics.hyracks.api.job.JobInfo;
import edu.uci.ics.hyracks.api.job.JobSpecification;

public class FeedInfo {
    public JobSpecification jobSpec;
    public JobInfo jobInfo;
    public JobId jobId;
    public FeedInfoType infoType;
    public State state;

    public enum State {
        ACTIVE,
        INACTIVE
    }

    public enum FeedInfoType {
        INTAKE,
        COLLECT
    }

    public FeedInfo(JobSpecification jobSpec, JobId jobId, FeedInfoType infoType) {
        this.jobSpec = jobSpec;
        this.jobId = jobId;
        this.infoType = infoType;
        this.state = State.INACTIVE;
    }

    @Override
    public String toString() {
        return " job id " + jobId;
    }
}
