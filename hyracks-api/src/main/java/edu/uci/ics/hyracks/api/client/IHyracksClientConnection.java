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

import edu.uci.ics.hyracks.api.job.JobFlag;
import edu.uci.ics.hyracks.api.job.JobId;
import edu.uci.ics.hyracks.api.job.JobSpecification;
import edu.uci.ics.hyracks.api.job.JobStatus;

/**
 * Interface used by clients to communicate with the Hyracks Cluster Controller.
 * 
 * @author vinayakb
 * 
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
     * Creates a Job Instance in the specified Hyracks application using the
     * specified {@link JobSpecification}.
     * 
     * @param appName
     *            Name of the application
     * @param jobSpec
     *            Job Specification
     * @return
     * @throws Exception
     */
    public JobId createJob(String appName, JobSpecification jobSpec) throws Exception;

    /**
     * Creates a Job Instance in the specified Hyracks application using the
     * specified {@link JobSpecification}. The specified flags are used to
     * configure the Job creation process.
     * 
     * @param appName
     *            Name of the application
     * @param jobSpec
     *            Job Specification
     * @param jobFlags
     *            Flags
     * @return
     * @throws Exception
     */
    public JobId createJob(String appName, JobSpecification jobSpec, EnumSet<JobFlag> jobFlags) throws Exception;

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
     * @param jobId
     *            JobId of the Job.
     * @throws Exception
     */
    public void start(JobId jobId) throws Exception;

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
}