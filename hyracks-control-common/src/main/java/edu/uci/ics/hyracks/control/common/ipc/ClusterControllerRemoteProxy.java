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

import edu.uci.ics.hyracks.api.dataflow.TaskAttemptId;
import edu.uci.ics.hyracks.api.job.JobId;
import edu.uci.ics.hyracks.control.common.base.IClusterController;
import edu.uci.ics.hyracks.control.common.controllers.NodeParameters;
import edu.uci.ics.hyracks.control.common.controllers.NodeRegistration;
import edu.uci.ics.hyracks.control.common.heartbeat.HeartbeatData;
import edu.uci.ics.hyracks.control.common.job.PartitionDescriptor;
import edu.uci.ics.hyracks.control.common.job.PartitionRequest;
import edu.uci.ics.hyracks.control.common.job.profiling.om.JobProfile;
import edu.uci.ics.hyracks.control.common.job.profiling.om.TaskProfile;
import edu.uci.ics.hyracks.ipc.api.IIPCHandle;
import edu.uci.ics.hyracks.ipc.api.SyncRMI;

public class ClusterControllerRemoteProxy implements IClusterController {
    private final IIPCHandle ipcHandle;

    public ClusterControllerRemoteProxy(IIPCHandle ipcHandle) {
        this.ipcHandle = ipcHandle;
    }

    @Override
    public NodeParameters registerNode(NodeRegistration reg) throws Exception {
        SyncRMI sync = new SyncRMI();
        ClusterControllerFunctions.RegisterNodeFunction fn = new ClusterControllerFunctions.RegisterNodeFunction(reg);
        NodeParameters result = (NodeParameters) sync.call(ipcHandle, fn);
        return result;
    }

    @Override
    public void unregisterNode(String nodeId) throws Exception {
        SyncRMI sync = new SyncRMI();
        ClusterControllerFunctions.UnregisterNodeFunction fn = new ClusterControllerFunctions.UnregisterNodeFunction(
                nodeId);
        sync.call(ipcHandle, fn);
    }

    @Override
    public void notifyTaskComplete(JobId jobId, TaskAttemptId taskId, String nodeId, TaskProfile statistics)
            throws Exception {
        ClusterControllerFunctions.NotifyTaskCompleteFunction fn = new ClusterControllerFunctions.NotifyTaskCompleteFunction(
                jobId, taskId, nodeId, statistics);
        ipcHandle.send(fn, null);
    }

    @Override
    public void notifyTaskFailure(JobId jobId, TaskAttemptId taskId, String nodeId, String details) throws Exception {
        ClusterControllerFunctions.NotifyTaskFailureFunction fn = new ClusterControllerFunctions.NotifyTaskFailureFunction(
                jobId, taskId, nodeId, details);
        ipcHandle.send(fn, null);
    }

    @Override
    public void notifyJobletCleanup(JobId jobId, String nodeId) throws Exception {
        ClusterControllerFunctions.NotifyJobletCleanupFunction fn = new ClusterControllerFunctions.NotifyJobletCleanupFunction(
                jobId, nodeId);
        ipcHandle.send(fn, null);
    }

    @Override
    public void nodeHeartbeat(String id, HeartbeatData hbData) throws Exception {
        ClusterControllerFunctions.NodeHeartbeatFunction fn = new ClusterControllerFunctions.NodeHeartbeatFunction(id,
                hbData);
        ipcHandle.send(fn, null);
    }

    @Override
    public void reportProfile(String id, List<JobProfile> profiles) throws Exception {
        ClusterControllerFunctions.ReportProfileFunction fn = new ClusterControllerFunctions.ReportProfileFunction(id,
                profiles);
        ipcHandle.send(fn, null);
    }

    @Override
    public void registerPartitionProvider(PartitionDescriptor partitionDescriptor) throws Exception {
        ClusterControllerFunctions.RegisterPartitionProviderFunction fn = new ClusterControllerFunctions.RegisterPartitionProviderFunction(
                partitionDescriptor);
        ipcHandle.send(fn, null);
    }

    @Override
    public void registerPartitionRequest(PartitionRequest partitionRequest) throws Exception {
        ClusterControllerFunctions.RegisterPartitionRequestFunction fn = new ClusterControllerFunctions.RegisterPartitionRequestFunction(
                partitionRequest);
        ipcHandle.send(fn, null);
    }
}