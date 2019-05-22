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
import java.util.List;

import org.apache.hyracks.api.comm.NetworkAddress;
import org.apache.hyracks.api.dataflow.TaskAttemptId;
import org.apache.hyracks.api.deployment.DeploymentId;
import org.apache.hyracks.api.job.DeployedJobSpecId;
import org.apache.hyracks.api.job.JobId;
import org.apache.hyracks.api.result.IResultMetadata;
import org.apache.hyracks.api.result.ResultSetId;
import org.apache.hyracks.control.common.base.IClusterController;
import org.apache.hyracks.control.common.controllers.NodeRegistration;
import org.apache.hyracks.control.common.deployment.DeploymentStatus;
import org.apache.hyracks.control.common.heartbeat.HeartbeatData;
import org.apache.hyracks.control.common.ipc.CCNCFunctions.GetNodeControllersInfoFunction;
import org.apache.hyracks.control.common.ipc.CCNCFunctions.NodeHeartbeatFunction;
import org.apache.hyracks.control.common.ipc.CCNCFunctions.NotifyDeployBinaryFunction;
import org.apache.hyracks.control.common.ipc.CCNCFunctions.NotifyJobletCleanupFunction;
import org.apache.hyracks.control.common.ipc.CCNCFunctions.NotifyTaskCompleteFunction;
import org.apache.hyracks.control.common.ipc.CCNCFunctions.NotifyTaskFailureFunction;
import org.apache.hyracks.control.common.ipc.CCNCFunctions.RegisterNodeFunction;
import org.apache.hyracks.control.common.ipc.CCNCFunctions.RegisterPartitionProviderFunction;
import org.apache.hyracks.control.common.ipc.CCNCFunctions.RegisterPartitionRequestFunction;
import org.apache.hyracks.control.common.ipc.CCNCFunctions.RegisterResultPartitionLocationFunction;
import org.apache.hyracks.control.common.ipc.CCNCFunctions.ReportDeployedJobSpecFailureFunction;
import org.apache.hyracks.control.common.ipc.CCNCFunctions.ReportProfileFunction;
import org.apache.hyracks.control.common.ipc.CCNCFunctions.ReportResultPartitionWriteCompletionFunction;
import org.apache.hyracks.control.common.ipc.CCNCFunctions.SendApplicationMessageFunction;
import org.apache.hyracks.control.common.ipc.CCNCFunctions.ShutdownResponseFunction;
import org.apache.hyracks.control.common.ipc.CCNCFunctions.StateDumpResponseFunction;
import org.apache.hyracks.control.common.ipc.CCNCFunctions.ThreadDumpResponseFunction;
import org.apache.hyracks.control.common.ipc.CCNCFunctions.UnregisterNodeFunction;
import org.apache.hyracks.control.common.job.PartitionDescriptor;
import org.apache.hyracks.control.common.job.PartitionRequest;
import org.apache.hyracks.control.common.job.profiling.om.JobProfile;
import org.apache.hyracks.control.common.job.profiling.om.TaskProfile;
import org.apache.hyracks.ipc.api.IIPCHandle;

public class ClusterControllerRemoteProxy implements IClusterController {

    private IIPCHandle ipcHandle;

    public ClusterControllerRemoteProxy(IIPCHandle ipcHandle) {
        this.ipcHandle = ipcHandle;
    }

    @Override
    public void registerNode(NodeRegistration reg, int registrationId) throws Exception {
        RegisterNodeFunction fn = new RegisterNodeFunction(reg, registrationId);
        ipcHandle.send(-1, fn, null);
    }

    @Override
    public void unregisterNode(String nodeId) throws Exception {
        UnregisterNodeFunction fn = new UnregisterNodeFunction(nodeId);
        ipcHandle.send(-1, fn, null);
    }

    @Override
    public void notifyTaskComplete(JobId jobId, TaskAttemptId taskId, String nodeId, TaskProfile statistics)
            throws Exception {
        NotifyTaskCompleteFunction fn = new NotifyTaskCompleteFunction(jobId, taskId, nodeId, statistics);
        ipcHandle.send(-1, fn, null);
    }

    @Override
    public void notifyTaskFailure(JobId jobId, TaskAttemptId taskId, String nodeId, List<Exception> exceptions)
            throws Exception {
        NotifyTaskFailureFunction fn = new NotifyTaskFailureFunction(jobId, taskId, nodeId, exceptions);
        ipcHandle.send(-1, fn, null);
    }

    @Override
    public void notifyJobletCleanup(JobId jobId, String nodeId) throws Exception {
        NotifyJobletCleanupFunction fn = new NotifyJobletCleanupFunction(jobId, nodeId);
        ipcHandle.send(-1, fn, null);
    }

    @Override
    public void notifyDeployBinary(DeploymentId deploymentId, String nodeId, DeploymentStatus status) throws Exception {
        NotifyDeployBinaryFunction fn = new NotifyDeployBinaryFunction(deploymentId, nodeId, status);
        ipcHandle.send(-1, fn, null);
    }

    @Override
    public void nodeHeartbeat(String id, HeartbeatData hbData, InetSocketAddress ncAddress) throws Exception {
        NodeHeartbeatFunction fn = new NodeHeartbeatFunction(id, hbData, ncAddress);
        ipcHandle.send(-1, fn, null);
    }

    @Override
    public void reportProfile(String id, List<JobProfile> profiles) throws Exception {
        ReportProfileFunction fn = new ReportProfileFunction(id, profiles);
        ipcHandle.send(-1, fn, null);
    }

    @Override
    public void registerPartitionProvider(PartitionDescriptor partitionDescriptor) throws Exception {
        RegisterPartitionProviderFunction fn = new RegisterPartitionProviderFunction(partitionDescriptor);
        ipcHandle.send(-1, fn, null);
    }

    @Override
    public void registerPartitionRequest(PartitionRequest partitionRequest) throws Exception {
        RegisterPartitionRequestFunction fn = new RegisterPartitionRequestFunction(partitionRequest);
        ipcHandle.send(-1, fn, null);
    }

    @Override
    public void sendApplicationMessageToCC(byte[] data, DeploymentId deploymentId, String nodeId) throws Exception {
        SendApplicationMessageFunction fn = new SendApplicationMessageFunction(data, deploymentId, nodeId);
        ipcHandle.send(-1, fn, null);
    }

    @Override
    public void registerResultPartitionLocation(JobId jobId, ResultSetId rsId, IResultMetadata metadata,
            boolean emptyResult, int partition, int nPartitions, NetworkAddress networkAddress) throws Exception {
        RegisterResultPartitionLocationFunction fn = new RegisterResultPartitionLocationFunction(jobId, rsId, metadata,
                emptyResult, partition, nPartitions, networkAddress);
        ipcHandle.send(-1, fn, null);
    }

    @Override
    public void reportResultPartitionWriteCompletion(JobId jobId, ResultSetId rsId, int partition) throws Exception {
        ReportResultPartitionWriteCompletionFunction fn =
                new ReportResultPartitionWriteCompletionFunction(jobId, rsId, partition);
        ipcHandle.send(-1, fn, null);
    }

    @Override
    public void notifyDeployedJobSpecFailure(DeployedJobSpecId deployedJobSpecId, String nodeId) throws Exception {
        ReportDeployedJobSpecFailureFunction fn = new ReportDeployedJobSpecFailureFunction(deployedJobSpecId, nodeId);
        ipcHandle.send(-1, fn, null);
    }

    @Override
    public void getNodeControllerInfos() throws Exception {
        ipcHandle.send(-1, new GetNodeControllersInfoFunction(), null);
    }

    @Override
    public void notifyStateDump(String nodeId, String stateDumpId, String state) throws Exception {
        StateDumpResponseFunction fn = new StateDumpResponseFunction(nodeId, stateDumpId, state);
        ipcHandle.send(-1, fn, null);
    }

    @Override
    public void notifyShutdown(String nodeId) throws Exception {
        ShutdownResponseFunction sdrf = new ShutdownResponseFunction(nodeId);
        ipcHandle.send(-1, sdrf, null);
    }

    @Override
    public void notifyThreadDump(String nodeId, String requestId, String threadDumpJSON) throws Exception {
        ThreadDumpResponseFunction tdrf = new ThreadDumpResponseFunction(nodeId, requestId, threadDumpJSON);
        ipcHandle.send(-1, tdrf, null);
    }

    @Override
    public void notifyPingResponse(String nodeId) throws Exception {
        CCNCFunctions.PingResponseFunction fn = new CCNCFunctions.PingResponseFunction(nodeId);
        ipcHandle.send(-1, fn, null);
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + " [" + ipcHandle.getRemoteAddress() + "]";
    }
}
