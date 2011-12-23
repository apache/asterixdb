/*
 * Copyright 2009-2010 by The Regents of the University of California
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
import java.util.Map;

import edu.uci.ics.hyracks.api.comm.NetworkAddress;
import edu.uci.ics.hyracks.api.dataflow.ConnectorDescriptorId;
import edu.uci.ics.hyracks.api.dataflow.TaskAttemptId;
import edu.uci.ics.hyracks.api.dataflow.connectors.IConnectorPolicy;
import edu.uci.ics.hyracks.api.job.JobId;
import edu.uci.ics.hyracks.api.job.JobStatus;
import edu.uci.ics.hyracks.api.partitions.PartitionId;
import edu.uci.ics.hyracks.control.common.base.INodeController;
import edu.uci.ics.hyracks.control.common.job.TaskAttemptDescriptor;
import edu.uci.ics.hyracks.ipc.api.IIPCHandle;
import edu.uci.ics.hyracks.ipc.api.SyncRMI;

public class NodeControllerRemoteProxy implements INodeController {
    private final IIPCHandle ipcHandle;

    public NodeControllerRemoteProxy(IIPCHandle ipcHandle) {
        this.ipcHandle = ipcHandle;
    }

    @Override
    public void startTasks(String appName, JobId jobId, byte[] planBytes, List<TaskAttemptDescriptor> taskDescriptors,
            Map<ConnectorDescriptorId, IConnectorPolicy> connectorPolicies) throws Exception {
        SyncRMI sync = new SyncRMI();
        NodeControllerFunctions.StartTasksFunction stf = new NodeControllerFunctions.StartTasksFunction(appName, jobId,
                planBytes, taskDescriptors, connectorPolicies);
        sync.call(ipcHandle, stf);
    }

    @Override
    public void abortTasks(JobId jobId, List<TaskAttemptId> tasks) throws Exception {
        SyncRMI sync = new SyncRMI();
        NodeControllerFunctions.AbortTasksFunction atf = new NodeControllerFunctions.AbortTasksFunction(jobId, tasks);
        sync.call(ipcHandle, atf);
    }

    @Override
    public void cleanUpJoblet(JobId jobId, JobStatus status) throws Exception {
        SyncRMI sync = new SyncRMI();
        NodeControllerFunctions.CleanupJobletFunction cjf = new NodeControllerFunctions.CleanupJobletFunction(jobId,
                status);
        sync.call(ipcHandle, cjf);
    }

    @Override
    public void createApplication(String appName, boolean deployHar, byte[] serializedDistributedState)
            throws Exception {
        SyncRMI sync = new SyncRMI();
        NodeControllerFunctions.CreateApplicationFunction caf = new NodeControllerFunctions.CreateApplicationFunction(
                appName, deployHar, serializedDistributedState);
        sync.call(ipcHandle, caf);
    }

    @Override
    public void destroyApplication(String appName) throws Exception {
        SyncRMI sync = new SyncRMI();
        NodeControllerFunctions.DestroyApplicationFunction daf = new NodeControllerFunctions.DestroyApplicationFunction(
                appName);
        sync.call(ipcHandle, daf);
    }

    @Override
    public void reportPartitionAvailability(PartitionId pid, NetworkAddress networkAddress) throws Exception {
        SyncRMI sync = new SyncRMI();
        NodeControllerFunctions.ReportPartitionAvailabilityFunction rpaf = new NodeControllerFunctions.ReportPartitionAvailabilityFunction(
                pid, networkAddress);
        sync.call(ipcHandle, rpaf);
    }
}