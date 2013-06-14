package edu.uci.ics.asterix.metadata.cluster;

import edu.uci.ics.asterix.metadata.api.IClusterManagementWork;

public class ClusterManagementWorkResponse implements IClusterManagementWorkResponse {

    protected final IClusterManagementWork work;

    protected final Status status;

    public ClusterManagementWorkResponse(IClusterManagementWork w, Status status) {
        this.work = w;
        this.status = status;
    }

    @Override
    public IClusterManagementWork getWork() {
        return work;
    }

    @Override
    public Status getStatus() {
        return status;
    }

}
