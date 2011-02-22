package edu.uci.ics.hyracks.control.cc;

import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;
import java.util.EnumSet;
import java.util.UUID;

import edu.uci.ics.hyracks.api.client.ClusterControllerInfo;
import edu.uci.ics.hyracks.api.client.IHyracksClientInterface;
import edu.uci.ics.hyracks.api.job.JobFlag;
import edu.uci.ics.hyracks.api.job.JobStatus;

public class CCClientInterface extends UnicastRemoteObject implements IHyracksClientInterface {
    private static final long serialVersionUID = 1L;

    private final ClusterControllerService ccs;

    public CCClientInterface(ClusterControllerService ccs) throws RemoteException {
        this.ccs = ccs;
    }

    @Override
    public ClusterControllerInfo getClusterControllerInfo() throws Exception {
        return ccs.getClusterControllerInfo();
    }

    @Override
    public void createApplication(String appName) throws Exception {
        ccs.createApplication(appName);
    }

    @Override
    public void startApplication(String appName) throws Exception {
        ccs.startApplication(appName);
    }

    @Override
    public void destroyApplication(String appName) throws Exception {
        ccs.destroyApplication(appName);
    }

    @Override
    public UUID createJob(String appName, byte[] jobSpec, EnumSet<JobFlag> jobFlags) throws Exception {
        return ccs.createJob(appName, jobSpec, jobFlags);
    }

    @Override
    public JobStatus getJobStatus(UUID jobId) throws Exception {
        return ccs.getJobStatus(jobId);
    }

    @Override
    public void start(UUID jobId) throws Exception {
        ccs.start(jobId);
    }

    @Override
    public void waitForCompletion(UUID jobId) throws Exception {
        ccs.waitForCompletion(jobId);
    }
}