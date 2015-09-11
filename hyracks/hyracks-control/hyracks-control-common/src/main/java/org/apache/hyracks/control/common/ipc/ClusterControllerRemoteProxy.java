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

import java.util.List;

import org.apache.hyracks.api.comm.NetworkAddress;
import org.apache.hyracks.api.dataflow.TaskAttemptId;
import org.apache.hyracks.api.dataset.ResultSetId;
import org.apache.hyracks.api.deployment.DeploymentId;
import org.apache.hyracks.api.job.JobId;
import org.apache.hyracks.control.common.base.IClusterController;
import org.apache.hyracks.control.common.controllers.NodeRegistration;
import org.apache.hyracks.control.common.deployment.DeploymentStatus;
import org.apache.hyracks.control.common.heartbeat.HeartbeatData;
import org.apache.hyracks.control.common.job.PartitionDescriptor;
import org.apache.hyracks.control.common.job.PartitionRequest;
import org.apache.hyracks.control.common.job.profiling.om.JobProfile;
import org.apache.hyracks.control.common.job.profiling.om.TaskProfile;
import org.apache.hyracks.ipc.api.IIPCHandle;

public class ClusterControllerRemoteProxy implements IClusterController {
    private final IIPCHandle ipcHandle;

    public ClusterControllerRemoteProxy(IIPCHandle ipcHandle) {
        this.ipcHandle = ipcHandle;
    }

    @Override
    public void registerNode(NodeRegistration reg) throws Exception {
        CCNCFunctions.RegisterNodeFunction fn = new CCNCFunctions.RegisterNodeFunction(reg);
        ipcHandle.send(-1, fn, null);
    }

    @Override
    public void unregisterNode(String nodeId) throws Exception {
        CCNCFunctions.UnregisterNodeFunction fn = new CCNCFunctions.UnregisterNodeFunction(nodeId);
        ipcHandle.send(-1, fn, null);
    }

    @Override
    public void notifyTaskComplete(JobId jobId, TaskAttemptId taskId, String nodeId, TaskProfile statistics)
            throws Exception {
        CCNCFunctions.NotifyTaskCompleteFunction fn = new CCNCFunctions.NotifyTaskCompleteFunction(jobId, taskId,
                nodeId, statistics);
        ipcHandle.send(-1, fn, null);
    }

    @Override
    public void notifyTaskFailure(JobId jobId, TaskAttemptId taskId, String nodeId, List<Exception> exceptions)
            throws Exception {
        CCNCFunctions.NotifyTaskFailureFunction fn = new CCNCFunctions.NotifyTaskFailureFunction(jobId, taskId, nodeId,
                exceptions);
        ipcHandle.send(-1, fn, null);
    }

    @Override
    public void notifyJobletCleanup(JobId jobId, String nodeId) throws Exception {
        CCNCFunctions.NotifyJobletCleanupFunction fn = new CCNCFunctions.NotifyJobletCleanupFunction(jobId, nodeId);
        ipcHandle.send(-1, fn, null);
    }

    @Override
    public void notifyDeployBinary(DeploymentId deploymentId, String nodeId, DeploymentStatus status) throws Exception {
        CCNCFunctions.NotifyDeployBinaryFunction fn = new CCNCFunctions.NotifyDeployBinaryFunction(deploymentId,
                nodeId, status);
        ipcHandle.send(-1, fn, null);
    }

    @Override
    public void nodeHeartbeat(String id, HeartbeatData hbData) throws Exception {
        CCNCFunctions.NodeHeartbeatFunction fn = new CCNCFunctions.NodeHeartbeatFunction(id, hbData);
        ipcHandle.send(-1, fn, null);
    }

    @Override
    public void reportProfile(String id, List<JobProfile> profiles) throws Exception {
        CCNCFunctions.ReportProfileFunction fn = new CCNCFunctions.ReportProfileFunction(id, profiles);
        ipcHandle.send(-1, fn, null);
    }

    @Override
    public void registerPartitionProvider(PartitionDescriptor partitionDescriptor) throws Exception {
        CCNCFunctions.RegisterPartitionProviderFunction fn = new CCNCFunctions.RegisterPartitionProviderFunction(
                partitionDescriptor);
        ipcHandle.send(-1, fn, null);
    }

    @Override
    public void registerPartitionRequest(PartitionRequest partitionRequest) throws Exception {
        CCNCFunctions.RegisterPartitionRequestFunction fn = new CCNCFunctions.RegisterPartitionRequestFunction(
                partitionRequest);
        ipcHandle.send(-1, fn, null);
    }

    @Override
    public void sendApplicationMessageToCC(byte[] data, DeploymentId deploymentId, String nodeId) throws Exception {
        CCNCFunctions.SendApplicationMessageFunction fn = new CCNCFunctions.SendApplicationMessageFunction(data,
                deploymentId, nodeId);
        ipcHandle.send(-1, fn, null);
    }

    public void registerResultPartitionLocation(JobId jobId, ResultSetId rsId, boolean orderedResult, boolean emptyResult, int partition,
            int nPartitions, NetworkAddress networkAddress) throws Exception {
        CCNCFunctions.RegisterResultPartitionLocationFunction fn = new CCNCFunctions.RegisterResultPartitionLocationFunction(
                jobId, rsId, orderedResult, emptyResult, partition, nPartitions, networkAddress);
        ipcHandle.send(-1, fn, null);
    }

    @Override
    public void reportResultPartitionWriteCompletion(JobId jobId, ResultSetId rsId, int partition) throws Exception {
        CCNCFunctions.ReportResultPartitionWriteCompletionFunction fn = new CCNCFunctions.ReportResultPartitionWriteCompletionFunction(
                jobId, rsId, partition);
        ipcHandle.send(-1, fn, null);
    }

    @Override
    public void reportResultPartitionFailure(JobId jobId, ResultSetId rsId, int partition) throws Exception {
        CCNCFunctions.ReportResultPartitionFailureFunction fn = new CCNCFunctions.ReportResultPartitionFailureFunction(
                jobId, rsId, partition);
        ipcHandle.send(-1, fn, null);
    }

    @Override
    public void getNodeControllerInfos() throws Exception {
        ipcHandle.send(-1, new CCNCFunctions.GetNodeControllersInfoFunction(), null);
    }

    @Override
    public void notifyStateDump(String nodeId, String stateDumpId, String state) throws Exception {
        CCNCFunctions.StateDumpResponseFunction fn = new CCNCFunctions.StateDumpResponseFunction(nodeId, stateDumpId,
                state);
        ipcHandle.send(-1, fn, null);
    }
    @Override
    public void notifyShutdown(String nodeId) throws Exception{
        CCNCFunctions.ShutdownResponseFunction sdrf = new CCNCFunctions.ShutdownResponseFunction(nodeId);
        ipcHandle.send(-1,sdrf,null);
    }

}