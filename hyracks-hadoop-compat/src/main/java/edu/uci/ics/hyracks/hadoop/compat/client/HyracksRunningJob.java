package edu.uci.ics.hyracks.hadoop.compat.client;

import java.io.IOException;

import org.apache.hadoop.mapred.Counters;
import org.apache.hadoop.mapred.JobID;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapred.TaskAttemptID;
import org.apache.hadoop.mapred.TaskCompletionEvent;

import edu.uci.ics.hyracks.api.job.JobId;
import edu.uci.ics.hyracks.api.job.JobSpecification;

public class HyracksRunningJob implements RunningJob {

    JobId jobId;
    JobSpecification spec;
    HyracksClient hyracksClient;

    public JobId getJobId() {
        return jobId;
    }

    public void setJobId(JobId jobId) {
        this.jobId = jobId;
    }

    public JobSpecification getSpec() {
        return spec;
    }

    public void setSpec(JobSpecification spec) {
        this.spec = spec;
    }

    public HyracksRunningJob(JobId jobId, JobSpecification jobSpec, HyracksClient hyracksClient) {
        this.spec = jobSpec;
        this.jobId = jobId;
        this.hyracksClient = hyracksClient;
    }

    @Override
    public float cleanupProgress() throws IOException {
        return 0;
    }

    @Override
    public Counters getCounters() throws IOException {
        return new Counters();
    }

    @Override
    public JobID getID() {
        return new JobID(this.jobId.toString(), 1);
    }

    @Override
    public String getJobFile() {
        return "";
    }

    @Override
    public String getJobID() {
        return this.jobId.toString();
    }

    @Override
    public String getJobName() {
        return this.jobId.toString();
    }

    @Override
    public int getJobState() throws IOException {
        return isComplete() ? 2 : 1;
    }

    @Override
    public TaskCompletionEvent[] getTaskCompletionEvents(int startFrom) throws IOException {
        return new TaskCompletionEvent[0];
    }

    @Override
    public String getTrackingURL() {
        return " running on hyrax, remote kill is not supported ";
    }

    @Override
    public boolean isComplete() throws IOException {
        edu.uci.ics.hyracks.api.job.JobStatus status = null;
        try {
            status = hyracksClient.getJobStatus(jobId);
        } catch (Exception e) {
            e.printStackTrace();
        }

        return status.equals(edu.uci.ics.hyracks.api.job.JobStatus.TERMINATED);
    }

    @Override
    public boolean isSuccessful() throws IOException {
        return isComplete();
    }

    @Override
    public void killJob() throws IOException {
        // TODO Auto-generated method stub

    }

    @Override
    public void killTask(String taskId, boolean shouldFail) throws IOException {
        // TODO Auto-generated method stub

    }

    @Override
    public float mapProgress() throws IOException {
        return 0;
    }

    @Override
    public float reduceProgress() throws IOException {
        return 0;
    }

    @Override
    public void setJobPriority(String priority) throws IOException {

    }

    @Override
    public float setupProgress() throws IOException {
        return 0;
    }

    @Override
    public void waitForCompletion() throws IOException {
        while (!isComplete()) {
            try {
                Thread.sleep(5000);
            } catch (InterruptedException ie) {
            }
        }
    }

    @Override
    public String[] getTaskDiagnostics(TaskAttemptID arg0) throws IOException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public void killTask(TaskAttemptID arg0, boolean arg1) throws IOException {
        // TODO Auto-generated method stub

    }

}
