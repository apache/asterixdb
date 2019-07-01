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
package org.apache.hyracks.control.common.base;

import java.net.URL;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hyracks.api.comm.NetworkAddress;
import org.apache.hyracks.api.control.CcId;
import org.apache.hyracks.api.dataflow.ConnectorDescriptorId;
import org.apache.hyracks.api.dataflow.TaskAttemptId;
import org.apache.hyracks.api.dataflow.connectors.IConnectorPolicy;
import org.apache.hyracks.api.deployment.DeploymentId;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.job.DeployedJobSpecId;
import org.apache.hyracks.api.job.JobFlag;
import org.apache.hyracks.api.job.JobId;
import org.apache.hyracks.api.job.JobStatus;
import org.apache.hyracks.api.partitions.PartitionId;
import org.apache.hyracks.control.common.controllers.NodeParameters;
import org.apache.hyracks.control.common.job.TaskAttemptDescriptor;
import org.apache.hyracks.ipc.exceptions.IPCException;

public interface INodeController {
    void startTasks(DeploymentId deploymentId, JobId jobId, byte[] planBytes,
            List<TaskAttemptDescriptor> taskDescriptors, Map<ConnectorDescriptorId, IConnectorPolicy> connectorPolicies,
            Set<JobFlag> flags, Map<byte[], byte[]> jobParameters, DeployedJobSpecId deployedJobSpecId, long startTime)
            throws Exception;

    void abortTasks(JobId jobId, List<TaskAttemptId> tasks) throws Exception;

    void cleanUpJoblet(JobId jobId, JobStatus status) throws Exception;

    void reportPartitionAvailability(PartitionId pid, NetworkAddress networkAddress) throws Exception;

    void deployBinary(DeploymentId deploymentId, List<URL> url, boolean extractFromArchive) throws Exception;

    void undeployBinary(DeploymentId deploymentId) throws Exception;

    void deployJobSpec(DeployedJobSpecId deployedJobSpecId, byte[] planBytes, boolean checkForDuplicate)
            throws Exception;

    void undeployJobSpec(DeployedJobSpecId deployedJobSpecId) throws Exception;

    void dumpState(String stateDumpId) throws Exception;

    void shutdown(boolean terminateNCService) throws Exception;

    void sendApplicationMessageToNC(byte[] data, DeploymentId deploymentId, String nodeId) throws Exception;

    void takeThreadDump(String requestId) throws Exception;

    /**
     * Sends a request to this {@link INodeController} to abort all jobs
     * started by cluster controller with id {@code ccId}
     *
     * @param ccId
     * @throws IPCException
     */
    void abortJobs(CcId ccId) throws IPCException;

    /**
     * Sends node registration result to this {@link INodeController}.
     *
     * @param parameters
     * @param regFailure
     * @throws IPCException
     */
    void sendRegistrationResult(NodeParameters parameters, Exception regFailure) throws IPCException;

    /**
     * Sends a request to this {@link INodeController} to ping the
     * cluster controller with id {@code ccId}
     *
     * @param ccId
     * @throws IPCException
     */
    void ping(CcId ccId) throws IPCException;

    /**
     * Delivers a response to a heartbeat delivered to this {@link CcId}
     *
     * @param ccId
     * @param e
     * @throws IPCException
     */
    void heartbeatAck(CcId ccId, HyracksDataException e) throws IPCException;
}
