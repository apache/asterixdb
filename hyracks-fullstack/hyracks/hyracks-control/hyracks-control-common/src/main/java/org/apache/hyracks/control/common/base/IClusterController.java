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

import java.net.InetSocketAddress;
import java.util.List;

import org.apache.hyracks.api.comm.NetworkAddress;
import org.apache.hyracks.api.dataflow.TaskAttemptId;
import org.apache.hyracks.api.deployment.DeploymentId;
import org.apache.hyracks.api.job.DeployedJobSpecId;
import org.apache.hyracks.api.job.JobId;
import org.apache.hyracks.api.result.IResultMetadata;
import org.apache.hyracks.api.result.ResultSetId;
import org.apache.hyracks.control.common.controllers.NodeRegistration;
import org.apache.hyracks.control.common.deployment.DeploymentStatus;
import org.apache.hyracks.control.common.heartbeat.HeartbeatData;
import org.apache.hyracks.control.common.job.PartitionDescriptor;
import org.apache.hyracks.control.common.job.PartitionRequest;
import org.apache.hyracks.control.common.job.profiling.om.JobProfile;
import org.apache.hyracks.control.common.job.profiling.om.TaskProfile;

public interface IClusterController {
    void registerNode(NodeRegistration reg, int registrationId) throws Exception;

    void unregisterNode(String nodeId) throws Exception;

    void notifyTaskComplete(JobId jobId, TaskAttemptId taskId, String nodeId, TaskProfile statistics) throws Exception;

    void notifyTaskFailure(JobId jobId, TaskAttemptId taskId, String nodeId, List<Exception> exceptions)
            throws Exception;

    void notifyDeployedJobSpecFailure(DeployedJobSpecId deployedJobSpecId, String nodeId) throws Exception;

    void notifyJobletCleanup(JobId jobId, String nodeId) throws Exception;

    void notifyDeployBinary(DeploymentId deploymentId, String nodeId, DeploymentStatus status) throws Exception;

    void notifyStateDump(String nodeId, String stateDumpId, String state) throws Exception;

    void notifyShutdown(String nodeId) throws Exception;

    void nodeHeartbeat(String id, HeartbeatData hbData, InetSocketAddress ncAddress) throws Exception;

    void reportProfile(String id, List<JobProfile> profiles) throws Exception;

    void registerPartitionProvider(PartitionDescriptor partitionDescriptor) throws Exception;

    void registerPartitionRequest(PartitionRequest partitionRequest) throws Exception;

    void sendApplicationMessageToCC(byte[] data, DeploymentId deploymentId, String nodeId) throws Exception;

    void registerResultPartitionLocation(JobId jobId, ResultSetId rsId, IResultMetadata metadata, boolean emptyResult,
            int partition, int nPartitions, NetworkAddress networkAddress) throws Exception;

    void reportResultPartitionWriteCompletion(JobId jobId, ResultSetId rsId, int partition) throws Exception;

    void getNodeControllerInfos() throws Exception;

    void notifyThreadDump(String nodeId, String requestId, String threadDumpJSON) throws Exception;

    void notifyPingResponse(String nodeId) throws Exception;
}
