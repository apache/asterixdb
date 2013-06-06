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

import java.util.List;

import edu.uci.ics.hyracks.api.comm.NetworkAddress;
import edu.uci.ics.hyracks.api.dataflow.TaskAttemptId;
import edu.uci.ics.hyracks.api.dataset.ResultSetId;
import edu.uci.ics.hyracks.api.deployment.DeploymentId;
import edu.uci.ics.hyracks.api.job.JobId;
import edu.uci.ics.hyracks.control.common.controllers.NodeRegistration;
import edu.uci.ics.hyracks.control.common.deployment.DeploymentStatus;
import edu.uci.ics.hyracks.control.common.heartbeat.HeartbeatData;
import edu.uci.ics.hyracks.control.common.job.PartitionDescriptor;
import edu.uci.ics.hyracks.control.common.job.PartitionRequest;
import edu.uci.ics.hyracks.control.common.job.profiling.om.JobProfile;
import edu.uci.ics.hyracks.control.common.job.profiling.om.TaskProfile;

public interface IClusterController {
    public void registerNode(NodeRegistration reg) throws Exception;

    public void unregisterNode(String nodeId) throws Exception;

    public void notifyTaskComplete(JobId jobId, TaskAttemptId taskId, String nodeId, TaskProfile statistics)
            throws Exception;

    public void notifyTaskFailure(JobId jobId, TaskAttemptId taskId, String nodeId, List<Exception> exceptions)
            throws Exception;

    public void notifyJobletCleanup(JobId jobId, String nodeId) throws Exception;

    public void notifyDeployBinary(DeploymentId deploymentId, String nodeId, DeploymentStatus status) throws Exception;

    public void nodeHeartbeat(String id, HeartbeatData hbData) throws Exception;

    public void reportProfile(String id, List<JobProfile> profiles) throws Exception;

    public void registerPartitionProvider(PartitionDescriptor partitionDescriptor) throws Exception;

    public void registerPartitionRequest(PartitionRequest partitionRequest) throws Exception;

    public void sendApplicationMessageToCC(byte[] data, DeploymentId deploymentId, String nodeId) throws Exception;

    public void registerResultPartitionLocation(JobId jobId, ResultSetId rsId, boolean orderedResult, int partition,
            int nPartitions, NetworkAddress networkAddress) throws Exception;

    public void reportResultPartitionWriteCompletion(JobId jobId, ResultSetId rsId, int partition) throws Exception;

    public void reportResultPartitionFailure(JobId jobId, ResultSetId rsId, int partition) throws Exception;

    public void getNodeControllerInfos() throws Exception;
}