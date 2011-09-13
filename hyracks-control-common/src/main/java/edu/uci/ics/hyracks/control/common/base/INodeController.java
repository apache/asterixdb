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
package edu.uci.ics.hyracks.control.common.base;

import java.rmi.Remote;
import java.util.List;
import java.util.Map;

import edu.uci.ics.hyracks.api.comm.NetworkAddress;
import edu.uci.ics.hyracks.api.dataflow.ConnectorDescriptorId;
import edu.uci.ics.hyracks.api.dataflow.TaskAttemptId;
import edu.uci.ics.hyracks.api.dataflow.connectors.IConnectorPolicy;
import edu.uci.ics.hyracks.api.job.JobId;
import edu.uci.ics.hyracks.api.partitions.PartitionId;
import edu.uci.ics.hyracks.control.common.controllers.NCConfig;
import edu.uci.ics.hyracks.control.common.job.TaskAttemptDescriptor;

public interface INodeController extends Remote {
    public String getId() throws Exception;

    public NCConfig getConfiguration() throws Exception;

    public void startTasks(String appName, JobId jobId, byte[] planBytes, List<TaskAttemptDescriptor> taskDescriptors,
            Map<ConnectorDescriptorId, IConnectorPolicy> connectorPolicies, byte[] ctxVarBytes) throws Exception;

    public void abortTasks(JobId jobId, List<TaskAttemptId> tasks) throws Exception;

    public void cleanUpJob(JobId jobId) throws Exception;

    public void notifyRegistration(IClusterController ccs) throws Exception;

    public void createApplication(String appName, boolean deployHar, byte[] serializedDistributedState)
            throws Exception;

    public void destroyApplication(String appName) throws Exception;

    public void reportPartitionAvailability(PartitionId pid, NetworkAddress networkAddress) throws Exception;
}