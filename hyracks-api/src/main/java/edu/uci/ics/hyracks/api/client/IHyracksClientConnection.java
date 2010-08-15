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
import java.util.UUID;

import edu.uci.ics.hyracks.api.job.JobFlag;
import edu.uci.ics.hyracks.api.job.JobSpecification;
import edu.uci.ics.hyracks.api.job.JobStatus;
import edu.uci.ics.hyracks.api.job.statistics.JobStatistics;

public interface IHyracksClientConnection {
    public void createApplication(String appName, File harFile) throws Exception;

    public void destroyApplication(String appName) throws Exception;

    public UUID createJob(String appName, JobSpecification jobSpec) throws Exception;

    public UUID createJob(String appName, JobSpecification jobSpec, EnumSet<JobFlag> jobFlags) throws Exception;

    public JobStatus getJobStatus(UUID jobId) throws Exception;

    public void start(UUID jobId) throws Exception;

    public JobStatistics waitForCompletion(UUID jobId) throws Exception;
}