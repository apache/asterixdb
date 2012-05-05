package edu.uci.ics.hyracks.dataflow.std.base;

import edu.uci.ics.hyracks.api.dataflow.state.IStateObject;
import edu.uci.ics.hyracks.api.job.JobId;

public abstract class AbstractStateObject implements IStateObject {
    protected JobId jobId;

    protected Object id;

    protected long memoryOccupancy;

    protected AbstractStateObject() {
    }

    protected AbstractStateObject(JobId jobId, Object id) {
        this.jobId = jobId;
        this.id = id;
    }

    @Override
    public final JobId getJobId() {
        return jobId;
    }

    public final void setJobId(JobId jobId) {
        this.jobId = jobId;
    }

    @Override
    public final Object getId() {
        return id;
    }

    public final void setId(Object id) {
        this.id = id;
    }

    @Override
    public final long getMemoryOccupancy() {
        return memoryOccupancy;
    }

    public void setMemoryOccupancy(long memoryOccupancy) {
        this.memoryOccupancy = memoryOccupancy;
    }
}