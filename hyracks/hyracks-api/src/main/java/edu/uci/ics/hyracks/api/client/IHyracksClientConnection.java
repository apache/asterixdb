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

import java.util.EnumSet;
import java.util.List;
import java.util.Map;

import edu.uci.ics.hyracks.api.comm.NetworkAddress;
import edu.uci.ics.hyracks.api.deployment.DeploymentId;
import edu.uci.ics.hyracks.api.job.IActivityClusterGraphGeneratorFactory;
import edu.uci.ics.hyracks.api.job.JobFlag;
import edu.uci.ics.hyracks.api.job.JobId;
import edu.uci.ics.hyracks.api.job.JobSpecification;
import edu.uci.ics.hyracks.api.job.JobStatus;
import edu.uci.ics.hyracks.api.topology.ClusterTopology;

/**
 * Interface used by clients to communicate with the Hyracks Cluster Controller.
 * 
 * @author vinayakb
 */
public interface IHyracksClientConnection {
    /**
     * Gets the status of the specified Job.
     * 
     * @param jobId
     *            JobId of the Job
     * @return {@link JobStatus}
     * @throws Exception
     */
    public JobStatus getJobStatus(JobId jobId) throws Exception;

    /**
     * Start the specified Job.
     * 
     * @param appName
     *            Name of the application
     * @param jobSpec
     *            Job Specification
     * @throws Exception
     */
    public JobId startJob(JobSpecification jobSpec) throws Exception;

    /**
     * Start the specified Job.
     * 
     * @param appName
     *            Name of the application
     * @param jobSpec
     *            Job Specification
     * @param jobFlags
     *            Flags
     * @throws Exception
     */
    public JobId startJob(JobSpecification jobSpec, EnumSet<JobFlag> jobFlags) throws Exception;

    /**
     * Start the specified Job.
     * 
     * @param appName
     *            Name of the application
     * @param acggf
     *            Activity Cluster Graph Generator Factory
     * @param jobFlags
     *            Flags
     * @throws Exception
     */
    public JobId startJob(IActivityClusterGraphGeneratorFactory acggf, EnumSet<JobFlag> jobFlags) throws Exception;

    /**
     * Gets the IP Address and port for the DatasetDirectoryService wrapped in NetworkAddress
     * 
     * @return {@link NetworkAddress}
     * @throws Exception
     */
    public NetworkAddress getDatasetDirectoryServiceInfo() throws Exception;

    /**
     * Waits until the specified job has completed, either successfully or has
     * encountered a permanent failure.
     * 
     * @param jobId
     *            JobId of the Job
     * @throws Exception
     */
    public void waitForCompletion(JobId jobId) throws Exception;

    /**
     * Gets a map of node controller names to node information.
     * 
     * @return Map of node name to node information.
     */
    public Map<String, NodeControllerInfo> getNodeControllerInfos() throws Exception;

    /**
     * Get the cluster topology
     * 
     * @return the cluster topology
     * @throws Exception
     */
    public ClusterTopology getClusterTopology() throws Exception;

    /**
     * Deploy the user-defined jars to the cluster
     * 
     * @param jars
     *            a list of user-defined jars
     */
    public DeploymentId deployBinary(List<String> jars) throws Exception;

    /**
     * undeploy a certain deployment
     * 
     * @param jars
     *            a list of user-defined jars
     */
    public void unDeployBinary(DeploymentId deploymentId) throws Exception;

    /**
     * Start the specified Job.
     * 
     * @param deploymentId
     *            the id of the specific deployment
     * @param jobSpec
     *            Job Specification
     * @throws Exception
     */
    public JobId startJob(DeploymentId deploymentId, JobSpecification jobSpec) throws Exception;

    /**
     * Start the specified Job.
     * 
     * @param deploymentId
     *            the id of the specific deployment
     * @param jobSpec
     *            Job Specification
     * @param jobFlags
     *            Flags
     * @throws Exception
     */
    public JobId startJob(DeploymentId deploymentId, JobSpecification jobSpec, EnumSet<JobFlag> jobFlags)
            throws Exception;

    /**
     * Start the specified Job.
     * 
     * @param deploymentId
     *            the id of the specific deployment
     * @param acggf
     *            Activity Cluster Graph Generator Factory
     * @param jobFlags
     *            Flags
     * @throws Exception
     */
    public JobId startJob(DeploymentId deploymentId, IActivityClusterGraphGeneratorFactory acggf,
            EnumSet<JobFlag> jobFlags) throws Exception;

}
