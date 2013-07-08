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
package edu.uci.ics.hyracks.api.client;

import java.net.URL;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;

import edu.uci.ics.hyracks.api.comm.NetworkAddress;
import edu.uci.ics.hyracks.api.deployment.DeploymentId;
import edu.uci.ics.hyracks.api.job.JobFlag;
import edu.uci.ics.hyracks.api.job.JobId;
import edu.uci.ics.hyracks.api.job.JobStatus;
import edu.uci.ics.hyracks.api.topology.ClusterTopology;

public interface IHyracksClientInterface {
    public ClusterControllerInfo getClusterControllerInfo() throws Exception;

    public JobStatus getJobStatus(JobId jobId) throws Exception;

    public JobId startJob(byte[] acggfBytes, EnumSet<JobFlag> jobFlags) throws Exception;

    public NetworkAddress getDatasetDirectoryServiceInfo() throws Exception;

    public void waitForCompletion(JobId jobId) throws Exception;

    public Map<String, NodeControllerInfo> getNodeControllersInfo() throws Exception;

    public ClusterTopology getClusterTopology() throws Exception;

    public void deployBinary(List<URL> binaryURLs, DeploymentId deploymentId) throws Exception;

    public void unDeployBinary(DeploymentId deploymentId) throws Exception;

    public JobId startJob(DeploymentId deploymentId, byte[] acggfBytes, EnumSet<JobFlag> jobFlags) throws Exception;
}