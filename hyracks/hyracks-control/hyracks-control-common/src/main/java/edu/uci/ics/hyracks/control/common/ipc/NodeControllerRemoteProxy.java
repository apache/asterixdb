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

import java.net.URL;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;

import edu.uci.ics.hyracks.api.comm.NetworkAddress;
import edu.uci.ics.hyracks.api.dataflow.ConnectorDescriptorId;
import edu.uci.ics.hyracks.api.dataflow.TaskAttemptId;
import edu.uci.ics.hyracks.api.dataflow.connectors.IConnectorPolicy;
import edu.uci.ics.hyracks.api.deployment.DeploymentId;
import edu.uci.ics.hyracks.api.job.JobFlag;
import edu.uci.ics.hyracks.api.job.JobId;
import edu.uci.ics.hyracks.api.job.JobStatus;
import edu.uci.ics.hyracks.api.partitions.PartitionId;
import edu.uci.ics.hyracks.control.common.base.INodeController;
import edu.uci.ics.hyracks.control.common.job.TaskAttemptDescriptor;
import edu.uci.ics.hyracks.ipc.api.IIPCHandle;

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
}