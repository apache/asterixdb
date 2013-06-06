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
package edu.uci.ics.hyracks.control.common.base;

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
import edu.uci.ics.hyracks.control.common.job.TaskAttemptDescriptor;

public interface INodeController {
    public void startTasks(DeploymentId deploymentId, JobId jobId, byte[] planBytes,
            List<TaskAttemptDescriptor> taskDescriptors,
            Map<ConnectorDescriptorId, IConnectorPolicy> connectorPolicies, EnumSet<JobFlag> flags) throws Exception;

    public void abortTasks(JobId jobId, List<TaskAttemptId> tasks) throws Exception;

    public void cleanUpJoblet(JobId jobId, JobStatus status) throws Exception;

    public void reportPartitionAvailability(PartitionId pid, NetworkAddress networkAddress) throws Exception;

    public void deployBinary(DeploymentId deploymentId, List<URL> url) throws Exception;

    public void undeployBinary(DeploymentId deploymentId) throws Exception;
}