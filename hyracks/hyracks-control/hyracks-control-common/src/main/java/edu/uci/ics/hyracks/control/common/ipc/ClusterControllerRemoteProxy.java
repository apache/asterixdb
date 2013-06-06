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
package edu.uci.ics.hyracks.control.common.ipc;

import java.util.List;

import edu.uci.ics.hyracks.api.comm.NetworkAddress;
import edu.uci.ics.hyracks.api.dataflow.TaskAttemptId;
import edu.uci.ics.hyracks.api.dataset.ResultSetId;
import edu.uci.ics.hyracks.api.deployment.DeploymentId;
import edu.uci.ics.hyracks.api.job.JobId;
import edu.uci.ics.hyracks.control.common.base.IClusterController;
import edu.uci.ics.hyracks.control.common.controllers.NodeRegistration;
import edu.uci.ics.hyracks.control.common.deployment.DeploymentStatus;
import edu.uci.ics.hyracks.control.common.heartbeat.HeartbeatData;
import edu.uci.ics.hyracks.control.common.job.PartitionDescriptor;
import edu.uci.ics.hyracks.control.common.job.PartitionRequest;
import edu.uci.ics.hyracks.control.common.job.profiling.om.JobProfile;
import edu.uci.ics.hyracks.control.common.job.profiling.om.TaskProfile;
import edu.uci.ics.hyracks.ipc.api.IIPCHandle;

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

    public void registerResultPartitionLocation(JobId jobId, ResultSetId rsId, boolean orderedResult, int partition,
            int nPartitions, NetworkAddress networkAddress) throws Exception {
        CCNCFunctions.RegisterResultPartitionLocationFunction fn = new CCNCFunctions.RegisterResultPartitionLocationFunction(
                jobId, rsId, orderedResult, partition, nPartitions, networkAddress);
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

}