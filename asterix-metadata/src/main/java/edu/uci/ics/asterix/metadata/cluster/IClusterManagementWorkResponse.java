package edu.uci.ics.asterix.metadata.cluster;

import edu.uci.ics.asterix.metadata.api.IClusterManagementWork;

public interface IClusterManagementWorkResponse {

    public enum Status {
        SUCCESS,
        FAILURE
    }

    /**
     * @return
     */
    public IClusterManagementWork getWork();

    /**
     * @return
     */
    public Status getStatus();

}
