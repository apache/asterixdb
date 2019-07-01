/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.hyracks.ipc.impl;

import java.net.URL;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.hyracks.api.client.ClusterControllerInfo;
import org.apache.hyracks.api.client.HyracksClientInterfaceFunctions;
import org.apache.hyracks.api.client.IHyracksClientInterface;
import org.apache.hyracks.api.client.NodeControllerInfo;
import org.apache.hyracks.api.comm.NetworkAddress;
import org.apache.hyracks.api.deployment.DeploymentId;
import org.apache.hyracks.api.job.DeployedJobSpecId;
import org.apache.hyracks.api.job.JobFlag;
import org.apache.hyracks.api.job.JobId;
import org.apache.hyracks.api.job.JobInfo;
import org.apache.hyracks.api.job.JobStatus;
import org.apache.hyracks.api.topology.ClusterTopology;
import org.apache.hyracks.ipc.api.IIPCHandle;
import org.apache.hyracks.ipc.api.RPCInterface;
import org.apache.hyracks.ipc.exceptions.IPCException;

public class HyracksClientInterfaceRemoteProxy implements IHyracksClientInterface {
    private static final int SHUTDOWN_CONNECTION_TIMEOUT_SECS = 30;

    private final IIPCHandle ipcHandle;

    private final RPCInterface rpci;

    public HyracksClientInterfaceRemoteProxy(IIPCHandle ipcHandle, RPCInterface rpci) {
        this.ipcHandle = ipcHandle;
        this.rpci = rpci;
    }

    @Override
    public ClusterControllerInfo getClusterControllerInfo() throws Exception {
        HyracksClientInterfaceFunctions.GetClusterControllerInfoFunction gccif =
                new HyracksClientInterfaceFunctions.GetClusterControllerInfoFunction();
        return (ClusterControllerInfo) rpci.call(ipcHandle, gccif);
    }

    @Override
    public JobStatus getJobStatus(JobId jobId) throws Exception {
        HyracksClientInterfaceFunctions.GetJobStatusFunction gjsf =
                new HyracksClientInterfaceFunctions.GetJobStatusFunction(jobId);
        return (JobStatus) rpci.call(ipcHandle, gjsf);
    }

    @Override
    public JobId startJob(byte[] acggfBytes, EnumSet<JobFlag> jobFlags) throws Exception {
        HyracksClientInterfaceFunctions.StartJobFunction sjf =
                new HyracksClientInterfaceFunctions.StartJobFunction(acggfBytes, jobFlags);
        return (JobId) rpci.call(ipcHandle, sjf);
    }

    @Override
    public void cancelJob(JobId jobId) throws Exception {
        HyracksClientInterfaceFunctions.CancelJobFunction cjf =
                new HyracksClientInterfaceFunctions.CancelJobFunction(jobId);
        rpci.call(ipcHandle, cjf);
    }

    @Override
    public JobId startJob(DeployedJobSpecId deployedJobSpecId, Map<byte[], byte[]> jobParameters) throws Exception {
        HyracksClientInterfaceFunctions.StartJobFunction sjf =
                new HyracksClientInterfaceFunctions.StartJobFunction(deployedJobSpecId, jobParameters);
        return (JobId) rpci.call(ipcHandle, sjf);
    }

    @Override
    public JobId startJob(DeploymentId deploymentId, byte[] acggfBytes, EnumSet<JobFlag> jobFlags) throws Exception {
        HyracksClientInterfaceFunctions.StartJobFunction sjf =
                new HyracksClientInterfaceFunctions.StartJobFunction(deploymentId, acggfBytes, jobFlags);
        return (JobId) rpci.call(ipcHandle, sjf);
    }

    @Override
    public DeployedJobSpecId deployJobSpec(byte[] acggfBytes) throws Exception {
        HyracksClientInterfaceFunctions.DeployJobSpecFunction sjf =
                new HyracksClientInterfaceFunctions.DeployJobSpecFunction(acggfBytes);
        return (DeployedJobSpecId) rpci.call(ipcHandle, sjf);
    }

    @Override
    public void redeployJobSpec(DeployedJobSpecId deployedJobSpecId, byte[] acggfBytes) throws Exception {
        HyracksClientInterfaceFunctions.redeployJobSpecFunction udjsf =
                new HyracksClientInterfaceFunctions.redeployJobSpecFunction(deployedJobSpecId, acggfBytes);
        rpci.call(ipcHandle, udjsf);
    }

    @Override
    public void undeployJobSpec(DeployedJobSpecId deployedJobSpecId) throws Exception {
        HyracksClientInterfaceFunctions.UndeployJobSpecFunction sjf =
                new HyracksClientInterfaceFunctions.UndeployJobSpecFunction(deployedJobSpecId);
        rpci.call(ipcHandle, sjf);
    }

    @Override
    public NetworkAddress getResultDirectoryAddress() throws Exception {
        HyracksClientInterfaceFunctions.GetResultDirectoryAddressFunction gddsf =
                new HyracksClientInterfaceFunctions.GetResultDirectoryAddressFunction();
        return (NetworkAddress) rpci.call(ipcHandle, gddsf);
    }

    @Override
    public void waitForCompletion(JobId jobId) throws Exception {
        HyracksClientInterfaceFunctions.WaitForCompletionFunction wfcf =
                new HyracksClientInterfaceFunctions.WaitForCompletionFunction(jobId);
        rpci.call(ipcHandle, wfcf);
    }

    @Override
    public Map<String, NodeControllerInfo> getNodeControllersInfo() throws Exception {
        HyracksClientInterfaceFunctions.GetNodeControllersInfoFunction gncif =
                new HyracksClientInterfaceFunctions.GetNodeControllersInfoFunction();
        return (Map<String, NodeControllerInfo>) rpci.call(ipcHandle, gncif);
    }

    @Override
    public ClusterTopology getClusterTopology() throws Exception {
        HyracksClientInterfaceFunctions.GetClusterTopologyFunction gctf =
                new HyracksClientInterfaceFunctions.GetClusterTopologyFunction();
        return (ClusterTopology) rpci.call(ipcHandle, gctf);
    }

    @Override
    public void deployBinary(List<URL> binaryURLs, DeploymentId deploymentId, boolean extractFromArchive)
            throws Exception {
        HyracksClientInterfaceFunctions.CliDeployBinaryFunction dbf =
                new HyracksClientInterfaceFunctions.CliDeployBinaryFunction(binaryURLs, deploymentId,
                        extractFromArchive);
        rpci.call(ipcHandle, dbf);
    }

    @Override
    public void unDeployBinary(DeploymentId deploymentId) throws Exception {
        HyracksClientInterfaceFunctions.CliUnDeployBinaryFunction dbf =
                new HyracksClientInterfaceFunctions.CliUnDeployBinaryFunction(deploymentId);
        rpci.call(ipcHandle, dbf);
    }

    @Override
    public JobInfo getJobInfo(JobId jobId) throws Exception {
        HyracksClientInterfaceFunctions.GetJobInfoFunction gjsf =
                new HyracksClientInterfaceFunctions.GetJobInfoFunction(jobId);
        return (JobInfo) rpci.call(ipcHandle, gjsf);
    }

    @Override
    public void stopCluster(boolean terminateNCService) throws Exception {
        HyracksClientInterfaceFunctions.ClusterShutdownFunction csdf =
                new HyracksClientInterfaceFunctions.ClusterShutdownFunction(terminateNCService);
        rpci.call(ipcHandle, csdf);
        int i = 0;
        // give the CC some time to do final settling after it returns our request
        while (ipcHandle.isConnected() && i++ < SHUTDOWN_CONNECTION_TIMEOUT_SECS) {
            synchronized (this) {
                wait(TimeUnit.SECONDS.toMillis(1));
            }
        }
        if (ipcHandle.isConnected()) {
            throw new IPCException(
                    "CC refused to release connection after " + SHUTDOWN_CONNECTION_TIMEOUT_SECS + " seconds");
        }
    }

    @Override
    public String getNodeDetailsJSON(String nodeId, boolean includeStats, boolean includeConfig) throws Exception {
        HyracksClientInterfaceFunctions.GetNodeDetailsJSONFunction gjsf =
                new HyracksClientInterfaceFunctions.GetNodeDetailsJSONFunction(nodeId, includeStats, includeConfig);
        return (String) rpci.call(ipcHandle, gjsf);
    }

    @Override
    public String getThreadDump(String node) throws Exception {
        HyracksClientInterfaceFunctions.ThreadDumpFunction tdf =
                new HyracksClientInterfaceFunctions.ThreadDumpFunction(node);
        return (String) rpci.call(ipcHandle, tdf);
    }

    @Override
    public boolean isConnected() {
        return ipcHandle.isConnected();
    }
}
