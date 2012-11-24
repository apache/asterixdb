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
package edu.uci.ics.hyracks.api.client;

import java.io.File;
import java.util.EnumSet;
import java.util.Map;

import edu.uci.ics.hyracks.api.comm.NetworkAddress;
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
     * Create a Hyracks Application
     * 
     * @param appName
     *            Name of the application
     * @param harFile
     *            Archive that contains deployable code for the application
     * @throws Exception
     */
    public void createApplication(String appName, File harFile) throws Exception;

    /**
     * Destroy an already-deployed Hyracks application
     * 
     * @param appName
     *            Name of the application
     * @throws Exception
     */
    public void destroyApplication(String appName) throws Exception;

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
    public JobId startJob(String appName, JobSpecification jobSpec) throws Exception;

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
    public JobId startJob(String appName, JobSpecification jobSpec, EnumSet<JobFlag> jobFlags) throws Exception;

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
    public JobId startJob(String appName, IActivityClusterGraphGeneratorFactory acggf, EnumSet<JobFlag> jobFlags)
            throws Exception;

    /**
     * Gets the IP Address and port for the DatasetDirectoryService wrapped in NetworkAddress
     * 
     * @param jobId
     *            JobId of the Job
     * @return {@link NetworkAddress}
     * @throws Exception
     */
    public NetworkAddress getDatasetDirectoryServiceInfo(JobId jobId) throws Exception;

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
}