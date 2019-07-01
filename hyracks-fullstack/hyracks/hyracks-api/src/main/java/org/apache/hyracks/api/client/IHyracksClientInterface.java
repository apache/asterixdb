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
package org.apache.hyracks.api.client;

import java.net.URL;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;

import org.apache.hyracks.api.comm.NetworkAddress;
import org.apache.hyracks.api.deployment.DeploymentId;
import org.apache.hyracks.api.job.DeployedJobSpecId;
import org.apache.hyracks.api.job.JobFlag;
import org.apache.hyracks.api.job.JobId;
import org.apache.hyracks.api.job.JobInfo;
import org.apache.hyracks.api.job.JobStatus;
import org.apache.hyracks.api.topology.ClusterTopology;

public interface IHyracksClientInterface {
    public ClusterControllerInfo getClusterControllerInfo() throws Exception;

    public JobStatus getJobStatus(JobId jobId) throws Exception;

    public JobId startJob(byte[] acggfBytes, EnumSet<JobFlag> jobFlags) throws Exception;

    public JobId startJob(DeployedJobSpecId deployedJobSpecId, Map<byte[], byte[]> jobParameters) throws Exception;

    public void cancelJob(JobId jobId) throws Exception;

    public DeployedJobSpecId deployJobSpec(byte[] acggfBytes) throws Exception;

    public void redeployJobSpec(DeployedJobSpecId deployedJobSpecId, byte[] acggfBytes) throws Exception;

    public void undeployJobSpec(DeployedJobSpecId deployedJobSpecId) throws Exception;

    public NetworkAddress getResultDirectoryAddress() throws Exception;

    public void waitForCompletion(JobId jobId) throws Exception;

    public Map<String, NodeControllerInfo> getNodeControllersInfo() throws Exception;

    public ClusterTopology getClusterTopology() throws Exception;

    public void deployBinary(List<URL> binaryURLs, DeploymentId deploymentId, boolean extractFromArchive)
            throws Exception;

    public void unDeployBinary(DeploymentId deploymentId) throws Exception;

    public JobId startJob(DeploymentId deploymentId, byte[] acggfBytes, EnumSet<JobFlag> jobFlags) throws Exception;

    public JobInfo getJobInfo(JobId jobId) throws Exception;

    public void stopCluster(boolean terminateNCService) throws Exception;

    public String getNodeDetailsJSON(String nodeId, boolean includeStats, boolean includeConfig) throws Exception;

    public String getThreadDump(String node) throws Exception;

    public boolean isConnected();
}
