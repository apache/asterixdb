package edu.uci.ics.hyracks.api.job;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

import edu.uci.ics.hyracks.api.dataflow.OperatorDescriptorId;

public class JobInfo implements Serializable{

    private final JobId jobId;

    private JobStatus status;

    private List<Exception> exceptions;

    private JobStatus pendingStatus;

    private List<Exception> pendingExceptions;

    private Map<OperatorDescriptorId, List<String>> operatorLocations;

    public JobInfo(JobId jobId, JobStatus jobStatus, Map<OperatorDescriptorId, List<String>> operatorLocations) {
        this.jobId = jobId;
        this.operatorLocations = operatorLocations;
        this.status = status;
    }

    public JobStatus getStatus() {
        return status;
    }

    public void setStatus(JobStatus status) {
        this.status = status;
    }

    public List<Exception> getExceptions() {
        return exceptions;
    }

    public void setExceptions(List<Exception> exceptions) {
        this.exceptions = exceptions;
    }

    public JobStatus getPendingStatus() {
        return pendingStatus;
    }

    public void setPendingStatus(JobStatus pendingStatus) {
        this.pendingStatus = pendingStatus;
    }

    public List<Exception> getPendingExceptions() {
        return pendingExceptions;
    }

    public void setPendingExceptions(List<Exception> pendingExceptions) {
        this.pendingExceptions = pendingExceptions;
    }

    public Map<OperatorDescriptorId, List<String>> getOperatorLocations() {
        return operatorLocations;
    }

    public void setOperatorLocations(Map<OperatorDescriptorId, List<String>> operatorLocations) {
        this.operatorLocations = operatorLocations;
    }

    public JobId getJobId() {
        return jobId;
    }
}
