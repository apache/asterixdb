package edu.uci.ics.asterix.metadata.cluster;

import edu.uci.ics.asterix.metadata.api.IClusterManagementWork;

public class ClusterManagementWorkResponse implements IClusterManagementWorkResponse {

    protected final IClusterManagementWork work;

    protected Status status;

    public ClusterManagementWorkResponse(IClusterManagementWork w) {
        this.work = w;
        this.status = Status.IN_PROGRESS;
    }

    @Override
    public IClusterManagementWork getWork() {
        return work;
    }

    @Override
    public Status getStatus() {
        return status;
    }

    @Override
    public void setStatus(Status status) {
        this.status = status;
    }

}
