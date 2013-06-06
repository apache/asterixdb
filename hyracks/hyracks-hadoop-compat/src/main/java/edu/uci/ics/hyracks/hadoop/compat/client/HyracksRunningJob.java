/*
 * Copyright 2009-2013 by The Regents of the University of California
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * you may obtain a copy of the License from
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.uci.ics.hyracks.hadoop.compat.client;

import java.io.IOException;

import org.apache.hadoop.mapred.Counters;
import org.apache.hadoop.mapred.JobID;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapred.TaskAttemptID;
import org.apache.hadoop.mapred.TaskCompletionEvent;

import edu.uci.ics.hyracks.api.job.IOperatorDescriptorRegistry;
import edu.uci.ics.hyracks.api.job.JobId;

public class HyracksRunningJob implements RunningJob {

    JobId jobId;
    IOperatorDescriptorRegistry spec;
    HyracksClient hyracksClient;

    public JobId getJobId() {
        return jobId;
    }

    public void setJobId(JobId jobId) {
        this.jobId = jobId;
    }

    public IOperatorDescriptorRegistry getSpec() {
        return spec;
    }

    public void setSpec(IOperatorDescriptorRegistry spec) {
        this.spec = spec;
    }

    public HyracksRunningJob(JobId jobId, IOperatorDescriptorRegistry jobSpec, HyracksClient hyracksClient) {
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
