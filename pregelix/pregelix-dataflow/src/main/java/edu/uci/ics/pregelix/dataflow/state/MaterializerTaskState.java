package edu.uci.ics.pregelix.dataflow.state;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import edu.uci.ics.hyracks.api.dataflow.TaskId;
import edu.uci.ics.hyracks.api.job.JobId;
import edu.uci.ics.hyracks.dataflow.common.io.RunFileWriter;
import edu.uci.ics.hyracks.dataflow.std.base.AbstractStateObject;

public class MaterializerTaskState extends AbstractStateObject {
    private RunFileWriter out;

    public MaterializerTaskState() {
    }

    public MaterializerTaskState(JobId jobId, TaskId taskId) {
        super(jobId, taskId);
    }

    @Override
    public void toBytes(DataOutput out) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void fromBytes(DataInput in) throws IOException {
        throw new UnsupportedOperationException();
    }

    public RunFileWriter getRunFileWriter() {
        return out;
    }

    public void setRunFileWriter(RunFileWriter out) {
        this.out = out;
    }
}