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

import static org.apache.hyracks.control.common.ipc.CCNCFunctions.*;

import java.net.InetSocketAddress;
import java.util.List;
import java.util.logging.Logger;

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
import org.apache.hyracks.ipc.impl.IPCSystem;

public class ClusterControllerRemoteProxy extends ControllerRemoteProxy implements IClusterController {
    private static final Logger LOGGER = Logger.getLogger(ClusterControllerRemoteProxy.class.getName());

    private final int clusterConnectRetries;

    public ClusterControllerRemoteProxy(IPCSystem ipc, InetSocketAddress inetSocketAddress, int clusterConnectRetries,
                                        IControllerRemoteProxyIPCEventListener eventListener) {
        super(ipc, inetSocketAddress, eventListener);
        this.clusterConnectRetries = clusterConnectRetries;
    }

    @Override
    protected int getRetries(boolean first) {
        return first ? clusterConnectRetries : 0;
    }

    @Override
    protected Logger getLogger() {
        return LOGGER;
    }

    @Override
    public void registerNode(NodeRegistration reg) throws Exception {
        RegisterNodeFunction fn = new RegisterNodeFunction(reg);
        ensureIpcHandle().send(-1, fn, null);
    }

    @Override
    public void unregisterNode(String nodeId) throws Exception {
        UnregisterNodeFunction fn = new UnregisterNodeFunction(nodeId);
        ensureIpcHandle().send(-1, fn, null);
    }

    @Override
    public void notifyTaskComplete(JobId jobId, TaskAttemptId taskId, String nodeId, TaskProfile statistics)
            throws Exception {
        NotifyTaskCompleteFunction fn = new NotifyTaskCompleteFunction(jobId, taskId,
                nodeId, statistics);
        ensureIpcHandle().send(-1, fn, null);
    }

    @Override
    public void notifyTaskFailure(JobId jobId, TaskAttemptId taskId, String nodeId, List<Exception> exceptions)
            throws Exception {
        NotifyTaskFailureFunction fn = new NotifyTaskFailureFunction(jobId, taskId, nodeId,
                exceptions);
        ensureIpcHandle().send(-1, fn, null);
    }

    @Override
    public void notifyJobletCleanup(JobId jobId, String nodeId) throws Exception {
        NotifyJobletCleanupFunction fn = new NotifyJobletCleanupFunction(jobId, nodeId);
        ensureIpcHandle().send(-1, fn, null);
    }

    @Override
    public void notifyDeployBinary(DeploymentId deploymentId, String nodeId, DeploymentStatus status) throws Exception {
        NotifyDeployBinaryFunction fn = new NotifyDeployBinaryFunction(deploymentId, nodeId,
                status);
        ensureIpcHandle().send(-1, fn, null);
    }

    @Override
    public void nodeHeartbeat(String id, HeartbeatData hbData) throws Exception {
        NodeHeartbeatFunction fn = new NodeHeartbeatFunction(id, hbData);
        ensureIpcHandle().send(-1, fn, null);
    }

    @Override
    public void reportProfile(String id, List<JobProfile> profiles) throws Exception {
        ReportProfileFunction fn = new ReportProfileFunction(id, profiles);
        ensureIpcHandle().send(-1, fn, null);
    }

    @Override
    public void registerPartitionProvider(PartitionDescriptor partitionDescriptor) throws Exception {
        RegisterPartitionProviderFunction fn = new RegisterPartitionProviderFunction(
                partitionDescriptor);
        ensureIpcHandle().send(-1, fn, null);
    }

    @Override
    public void registerPartitionRequest(PartitionRequest partitionRequest) throws Exception {
        RegisterPartitionRequestFunction fn = new RegisterPartitionRequestFunction(
                partitionRequest);
        ensureIpcHandle().send(-1, fn, null);
    }

    @Override
    public void sendApplicationMessageToCC(byte[] data, DeploymentId deploymentId, String nodeId) throws Exception {
        SendApplicationMessageFunction fn = new SendApplicationMessageFunction(data,
                deploymentId, nodeId);
        ensureIpcHandle().send(-1, fn, null);
    }

    @Override
    public void registerResultPartitionLocation(JobId jobId, ResultSetId rsId, boolean orderedResult,
            boolean emptyResult, int partition, int nPartitions, NetworkAddress networkAddress) throws Exception {
        RegisterResultPartitionLocationFunction fn = new RegisterResultPartitionLocationFunction(
                jobId, rsId, orderedResult, emptyResult, partition, nPartitions, networkAddress);
        ensureIpcHandle().send(-1, fn, null);
    }

    @Override
    public void reportResultPartitionWriteCompletion(JobId jobId, ResultSetId rsId, int partition) throws Exception {
        ReportResultPartitionWriteCompletionFunction fn = new ReportResultPartitionWriteCompletionFunction(
                jobId, rsId, partition);
        ensureIpcHandle().send(-1, fn, null);
    }

    @Override
    public void notifyDistributedJobFailure(JobId jobId, String nodeId) throws Exception {
        ReportDistributedJobFailureFunction fn = new ReportDistributedJobFailureFunction(
                jobId, nodeId);
        ensureIpcHandle().send(-1, fn, null);
    }

    @Override
    public void getNodeControllerInfos() throws Exception {
        ensureIpcHandle().send(-1, new GetNodeControllersInfoFunction(), null);
    }

    @Override
    public void notifyStateDump(String nodeId, String stateDumpId, String state) throws Exception {
        StateDumpResponseFunction fn = new StateDumpResponseFunction(nodeId, stateDumpId,
                state);
        ensureIpcHandle().send(-1, fn, null);
    }

    @Override
    public void notifyShutdown(String nodeId) throws Exception {
        ShutdownResponseFunction sdrf = new ShutdownResponseFunction(nodeId);
        ensureIpcHandle().send(-1, sdrf, null);
    }

    @Override
    public void notifyThreadDump(String nodeId, String requestId, String threadDumpJSON) throws Exception {
        ThreadDumpResponseFunction tdrf = new ThreadDumpResponseFunction(nodeId, requestId,
                threadDumpJSON);
        ensureIpcHandle().send(-1, tdrf, null);
    }
}
