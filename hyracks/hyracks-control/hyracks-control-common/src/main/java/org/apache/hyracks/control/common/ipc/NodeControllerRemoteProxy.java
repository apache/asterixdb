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

import java.net.URL;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;

import org.apache.hyracks.api.comm.NetworkAddress;
import org.apache.hyracks.api.dataflow.ConnectorDescriptorId;
import org.apache.hyracks.api.dataflow.TaskAttemptId;
import org.apache.hyracks.api.dataflow.connectors.IConnectorPolicy;
import org.apache.hyracks.api.deployment.DeploymentId;
import org.apache.hyracks.api.job.JobFlag;
import org.apache.hyracks.api.job.JobId;
import org.apache.hyracks.api.job.JobStatus;
import org.apache.hyracks.api.partitions.PartitionId;
import org.apache.hyracks.control.common.base.INodeController;
import org.apache.hyracks.control.common.job.TaskAttemptDescriptor;
import org.apache.hyracks.ipc.api.IIPCHandle;

public class NodeControllerRemoteProxy implements INodeController {
    private final IIPCHandle ipcHandle;

    public NodeControllerRemoteProxy(IIPCHandle ipcHandle) {
        this.ipcHandle = ipcHandle;
    }

    @Override
    public void startTasks(DeploymentId deploymentId, JobId jobId, byte[] planBytes,
            List<TaskAttemptDescriptor> taskDescriptors,
            Map<ConnectorDescriptorId, IConnectorPolicy> connectorPolicies, EnumSet<JobFlag> flags) throws Exception {
        CCNCFunctions.StartTasksFunction stf = new CCNCFunctions.StartTasksFunction(deploymentId, jobId, planBytes,
                taskDescriptors, connectorPolicies, flags);
        ipcHandle.send(-1, stf, null);
    }

    @Override
    public void abortTasks(JobId jobId, List<TaskAttemptId> tasks) throws Exception {
        CCNCFunctions.AbortTasksFunction atf = new CCNCFunctions.AbortTasksFunction(jobId, tasks);
        ipcHandle.send(-1, atf, null);
    }

    @Override
    public void cleanUpJoblet(JobId jobId, JobStatus status) throws Exception {
        CCNCFunctions.CleanupJobletFunction cjf = new CCNCFunctions.CleanupJobletFunction(jobId, status);
        ipcHandle.send(-1, cjf, null);
    }

    @Override
    public void reportPartitionAvailability(PartitionId pid, NetworkAddress networkAddress) throws Exception {
        CCNCFunctions.ReportPartitionAvailabilityFunction rpaf = new CCNCFunctions.ReportPartitionAvailabilityFunction(
                pid, networkAddress);
        ipcHandle.send(-1, rpaf, null);
    }

    @Override
    public void deployBinary(DeploymentId deploymentId, List<URL> binaryURLs) throws Exception {
        CCNCFunctions.DeployBinaryFunction rpaf = new CCNCFunctions.DeployBinaryFunction(deploymentId, binaryURLs);
        ipcHandle.send(-1, rpaf, null);
    }

    @Override
    public void undeployBinary(DeploymentId deploymentId) throws Exception {
        CCNCFunctions.UnDeployBinaryFunction rpaf = new CCNCFunctions.UnDeployBinaryFunction(deploymentId);
        ipcHandle.send(-1, rpaf, null);
    }

    @Override
    public void dumpState(String stateDumpId) throws Exception {
        CCNCFunctions.StateDumpRequestFunction dsf = new CCNCFunctions.StateDumpRequestFunction(stateDumpId);
        ipcHandle.send(-1, dsf, null);
    }

    @Override
    public void shutDown() throws Exception {
        CCNCFunctions.ShutdownRequestFunction sdrf = new CCNCFunctions.ShutdownRequestFunction();
        ipcHandle.send(-1, sdrf, null);
    }
}