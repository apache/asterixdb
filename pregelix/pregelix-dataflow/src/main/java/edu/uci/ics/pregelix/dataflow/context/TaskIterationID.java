package edu.uci.ics.pregelix.dataflow.context;

public class TaskIterationID {

    private TaskID tid;
    private int iteration;

    public TaskIterationID(TaskID tid, int iteration) {
        this.tid = tid;
        this.iteration = iteration;
    }

    public TaskIterationID(String jobId, int partition, int iteration) {
        this.tid = new TaskID(jobId, partition);
        this.iteration = iteration;
    }

    public TaskID getTaskID() {
        return tid;
    }

    public int getIteration() {
        return iteration;
    }

    public TaskIterationID getNextTaskIterationID() {
        return new TaskIterationID(tid, iteration + 1);
    }

    public TaskIterationID getPreviousTaskIterationID() {
        return new TaskIterationID(tid, iteration - 1);
    }

    @Override
    public int hashCode() {
        return tid.hashCode() + iteration;
    }

    public boolean equals(Object o) {
        if (!(o instanceof TaskIterationID)) {
            return false;
        }
        TaskIterationID tiid = (TaskIterationID) o;
        return tid.equals(tiid.getTaskID()) && iteration == tiid.getIteration();
    }

    @Override
    public String toString() {
        return tid.toString() + " iteration:" + iteration;
    }
}
