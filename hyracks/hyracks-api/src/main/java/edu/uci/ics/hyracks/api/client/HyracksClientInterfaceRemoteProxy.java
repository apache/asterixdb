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
package edu.uci.ics.hyracks.api.client;

import java.net.URL;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;

import edu.uci.ics.hyracks.api.comm.NetworkAddress;
import edu.uci.ics.hyracks.api.deployment.DeploymentId;
import edu.uci.ics.hyracks.api.job.JobFlag;
import edu.uci.ics.hyracks.api.job.JobId;
import edu.uci.ics.hyracks.api.job.JobStatus;
import edu.uci.ics.hyracks.api.topology.ClusterTopology;
import edu.uci.ics.hyracks.ipc.api.IIPCHandle;
import edu.uci.ics.hyracks.ipc.api.RPCInterface;

public class HyracksClientInterfaceRemoteProxy implements IHyracksClientInterface {
    private final IIPCHandle ipcHandle;

    private final RPCInterface rpci;

    public HyracksClientInterfaceRemoteProxy(IIPCHandle ipcHandle, RPCInterface rpci) {
        this.ipcHandle = ipcHandle;
        this.rpci = rpci;
    }

    @Override
    public ClusterControllerInfo getClusterControllerInfo() throws Exception {
        HyracksClientInterfaceFunctions.GetClusterControllerInfoFunction gccif = new HyracksClientInterfaceFunctions.GetClusterControllerInfoFunction();
        return (ClusterControllerInfo) rpci.call(ipcHandle, gccif);
    }

    @Override
    public JobStatus getJobStatus(JobId jobId) throws Exception {
        HyracksClientInterfaceFunctions.GetJobStatusFunction gjsf = new HyracksClientInterfaceFunctions.GetJobStatusFunction(
                jobId);
        return (JobStatus) rpci.call(ipcHandle, gjsf);
    }

    @Override
    public JobId startJob(byte[] acggfBytes, EnumSet<JobFlag> jobFlags) throws Exception {
        HyracksClientInterfaceFunctions.StartJobFunction sjf = new HyracksClientInterfaceFunctions.StartJobFunction(
                acggfBytes, jobFlags);
        return (JobId) rpci.call(ipcHandle, sjf);
    }

    @Override
    public JobId startJob(DeploymentId deploymentId, byte[] acggfBytes, EnumSet<JobFlag> jobFlags) throws Exception {
        HyracksClientInterfaceFunctions.StartJobFunction sjf = new HyracksClientInterfaceFunctions.StartJobFunction(
                deploymentId, acggfBytes, jobFlags);
        return (JobId) rpci.call(ipcHandle, sjf);
    }

    @Override
    public NetworkAddress getDatasetDirectoryServiceInfo() throws Exception {
        HyracksClientInterfaceFunctions.GetDatasetDirectoryServiceInfoFunction gddsf = new HyracksClientInterfaceFunctions.GetDatasetDirectoryServiceInfoFunction();
        return (NetworkAddress) rpci.call(ipcHandle, gddsf);
    }

    @Override
    public void waitForCompletion(JobId jobId) throws Exception {
        HyracksClientInterfaceFunctions.WaitForCompletionFunction wfcf = new HyracksClientInterfaceFunctions.WaitForCompletionFunction(
                jobId);
        rpci.call(ipcHandle, wfcf);
    }

    @Override
    public Map<String, NodeControllerInfo> getNodeControllersInfo() throws Exception {
        HyracksClientInterfaceFunctions.GetNodeControllersInfoFunction gncif = new HyracksClientInterfaceFunctions.GetNodeControllersInfoFunction();
        return (Map<String, NodeControllerInfo>) rpci.call(ipcHandle, gncif);
    }

    @Override
    public ClusterTopology getClusterTopology() throws Exception {
        HyracksClientInterfaceFunctions.GetClusterTopologyFunction gctf = new HyracksClientInterfaceFunctions.GetClusterTopologyFunction();
        return (ClusterTopology) rpci.call(ipcHandle, gctf);
    }

    @Override
    public void deployBinary(List<URL> binaryURLs, DeploymentId deploymentId) throws Exception {
        HyracksClientInterfaceFunctions.CliDeployBinaryFunction dbf = new HyracksClientInterfaceFunctions.CliDeployBinaryFunction(
                binaryURLs, deploymentId);
        rpci.call(ipcHandle, dbf);
    }

    @Override
    public void unDeployBinary(DeploymentId deploymentId) throws Exception {
        HyracksClientInterfaceFunctions.CliUnDeployBinaryFunction dbf = new HyracksClientInterfaceFunctions.CliUnDeployBinaryFunction(
                deploymentId);
        rpci.call(ipcHandle, dbf);
    }
}