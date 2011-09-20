package edu.uci.ics.hyracks.dataflow.std.base;

import edu.uci.ics.hyracks.api.dataflow.TaskId;
import edu.uci.ics.hyracks.api.dataflow.state.ITaskState;
import edu.uci.ics.hyracks.api.job.JobId;

public abstract class AbstractTaskState implements ITaskState {
    protected JobId jobId;

    protected TaskId taId;

    protected long memoryOccupancy;

    protected AbstractTaskState() {
    }

    protected AbstractTaskState(JobId jobId, TaskId taId) {
        this.jobId = jobId;
        this.taId = taId;
    }

    @Override
    public final JobId getJobId() {
        return jobId;
    }

    public final void setJobId(JobId jobId) {
        this.jobId = jobId;
    }

    @Override
    public final TaskId getTaskId() {
        return taId;
    }

    public final void setTaskId(TaskId tId) {
        this.taId = tId;
    }

    @Override
    public final long getMemoryOccupancy() {
        return memoryOccupancy;
    }

    public void setMemoryOccupancy(long memoryOccupancy) {
        this.memoryOccupancy = memoryOccupancy;
    }
}