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
package org.apache.hyracks.control.common.ipc;

import java.net.InetSocketAddress;
import java.net.URL;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hyracks.api.comm.NetworkAddress;
import org.apache.hyracks.api.control.CcId;
import org.apache.hyracks.api.dataflow.ConnectorDescriptorId;
import org.apache.hyracks.api.dataflow.TaskAttemptId;
import org.apache.hyracks.api.dataflow.connectors.IConnectorPolicy;
import org.apache.hyracks.api.deployment.DeploymentId;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.job.DeployedJobSpecId;
import org.apache.hyracks.api.job.JobFlag;
import org.apache.hyracks.api.job.JobId;
import org.apache.hyracks.api.job.JobStatus;
import org.apache.hyracks.api.partitions.PartitionId;
import org.apache.hyracks.control.common.base.INodeController;
import org.apache.hyracks.control.common.controllers.NodeParameters;
import org.apache.hyracks.control.common.ipc.CCNCFunctions.AbortTasksFunction;
import org.apache.hyracks.control.common.ipc.CCNCFunctions.CleanupJobletFunction;
import org.apache.hyracks.control.common.ipc.CCNCFunctions.DeployBinaryFunction;
import org.apache.hyracks.control.common.ipc.CCNCFunctions.DeployJobSpecFunction;
import org.apache.hyracks.control.common.ipc.CCNCFunctions.ReportPartitionAvailabilityFunction;
import org.apache.hyracks.control.common.ipc.CCNCFunctions.SendApplicationMessageFunction;
import org.apache.hyracks.control.common.ipc.CCNCFunctions.ShutdownRequestFunction;
import org.apache.hyracks.control.common.ipc.CCNCFunctions.StartTasksFunction;
import org.apache.hyracks.control.common.ipc.CCNCFunctions.StateDumpRequestFunction;
import org.apache.hyracks.control.common.ipc.CCNCFunctions.ThreadDumpRequestFunction;
import org.apache.hyracks.control.common.ipc.CCNCFunctions.UnDeployBinaryFunction;
import org.apache.hyracks.control.common.ipc.CCNCFunctions.UndeployJobSpecFunction;
import org.apache.hyracks.control.common.job.TaskAttemptDescriptor;
import org.apache.hyracks.ipc.api.IIPCHandle;
import org.apache.hyracks.ipc.exceptions.IPCException;

public class NodeControllerRemoteProxy implements INodeController {
    private final CcId ccId;
    private final IIPCHandle ipcHandle;

    public NodeControllerRemoteProxy(CcId ccId, IIPCHandle ipcHandle) {
        this.ccId = ccId;
        this.ipcHandle = ipcHandle;
    }

    @Override
    public void startTasks(DeploymentId deploymentId, JobId jobId, byte[] planBytes,
            List<TaskAttemptDescriptor> taskDescriptors, Map<ConnectorDescriptorId, IConnectorPolicy> connectorPolicies,
            Set<JobFlag> flags, Map<byte[], byte[]> jobParameters, DeployedJobSpecId deployedJobSpecId,
            long jobStartTime) throws Exception {
        StartTasksFunction stf = new StartTasksFunction(deploymentId, jobId, planBytes, taskDescriptors,
                connectorPolicies, flags, jobParameters, deployedJobSpecId, jobStartTime);
        ipcHandle.send(-1, stf, null);
    }

    @Override
    public void abortTasks(JobId jobId, List<TaskAttemptId> tasks) throws Exception {
        AbortTasksFunction atf = new AbortTasksFunction(jobId, tasks);
        ipcHandle.send(-1, atf, null);
    }

    @Override
    public void cleanUpJoblet(JobId jobId, JobStatus status) throws Exception {
        CleanupJobletFunction cjf = new CleanupJobletFunction(jobId, status);
        ipcHandle.send(-1, cjf, null);
    }

    @Override
    public void reportPartitionAvailability(PartitionId pid, NetworkAddress networkAddress) throws Exception {
        ReportPartitionAvailabilityFunction rpaf = new ReportPartitionAvailabilityFunction(pid, networkAddress);
        ipcHandle.send(-1, rpaf, null);
    }

    @Override
    public void deployBinary(DeploymentId deploymentId, List<URL> binaryURLs, boolean extractFromArchive)
            throws Exception {
        DeployBinaryFunction rpaf = new DeployBinaryFunction(deploymentId, binaryURLs, ccId, extractFromArchive);
        ipcHandle.send(-1, rpaf, null);
    }

    @Override
    public void undeployBinary(DeploymentId deploymentId) throws Exception {
        UnDeployBinaryFunction rpaf = new UnDeployBinaryFunction(deploymentId, ccId);
        ipcHandle.send(-1, rpaf, null);
    }

    @Override
    public void deployJobSpec(DeployedJobSpecId deployedJobSpecId, byte[] planBytes, boolean upsert) throws Exception {
        DeployJobSpecFunction fn = new DeployJobSpecFunction(deployedJobSpecId, planBytes, upsert, ccId);
        ipcHandle.send(-1, fn, null);
    }

    @Override
    public void undeployJobSpec(DeployedJobSpecId deployedJobSpecId) throws Exception {
        UndeployJobSpecFunction fn = new UndeployJobSpecFunction(deployedJobSpecId, ccId);
        ipcHandle.send(-1, fn, null);
    }

    @Override
    public void dumpState(String stateDumpId) throws Exception {
        StateDumpRequestFunction dsf = new StateDumpRequestFunction(stateDumpId, ccId);
        ipcHandle.send(-1, dsf, null);
    }

    @Override
    public void shutdown(boolean terminateNCService) throws Exception {
        ShutdownRequestFunction sdrf = new ShutdownRequestFunction(terminateNCService, ccId);
        ipcHandle.send(-1, sdrf, null);
    }

    @Override
    public void sendApplicationMessageToNC(byte[] data, DeploymentId deploymentId, String nodeId) throws Exception {
        SendApplicationMessageFunction fn = new SendApplicationMessageFunction(data, deploymentId, nodeId);
        ipcHandle.send(-1, fn, null);
    }

    @Override
    public void takeThreadDump(String requestId) throws Exception {
        ThreadDumpRequestFunction fn = new ThreadDumpRequestFunction(requestId, ccId);
        ipcHandle.send(-1, fn, null);
    }

    @Override
    public void abortJobs(CcId ccId) throws IPCException {
        ipcHandle.send(-1, new CCNCFunctions.AbortCCJobsFunction(ccId), null);
    }

    @Override
    public void sendRegistrationResult(NodeParameters parameters, Exception regFailure) throws IPCException {
        ipcHandle.send(-1, new CCNCFunctions.NodeRegistrationResult(parameters, regFailure), null);
    }

    @Override
    public void ping(CcId ccId) throws IPCException {
        ipcHandle.send(-1, new CCNCFunctions.PingFunction(ccId), null);
    }

    @Override
    public void heartbeatAck(CcId ccId, HyracksDataException e) throws IPCException {
        ipcHandle.send(-1, new CCNCFunctions.NodeHeartbeatAckFunction(ccId, e), null);
    }

    public InetSocketAddress getAddress() {
        return ipcHandle.getRemoteAddress();
    }
}
