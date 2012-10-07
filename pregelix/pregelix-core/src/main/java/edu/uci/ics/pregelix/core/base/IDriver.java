package edu.uci.ics.pregelix.core.base;

import edu.uci.ics.hyracks.api.exceptions.HyracksException;
import edu.uci.ics.pregelix.api.job.PregelixJob;

public interface IDriver {

    public static enum Plan {
        INNER_JOIN, OUTER_JOIN, OUTER_JOIN_SORT, OUTER_JOIN_SINGLE_SORT
    }

    public void runJob(PregelixJob job, Plan planChoice, String ipAddress, int port, boolean profiling)
            throws HyracksException;
}
