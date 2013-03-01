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

import java.util.EnumSet;
import java.util.Map;

import edu.uci.ics.hyracks.api.job.JobFlag;
import edu.uci.ics.hyracks.api.job.JobId;
import edu.uci.ics.hyracks.api.job.JobStatus;
import edu.uci.ics.hyracks.api.topology.ClusterTopology;

public interface IHyracksClientInterface {
    public ClusterControllerInfo getClusterControllerInfo() throws Exception;

    public void createApplication(String appName) throws Exception;

    public void startApplication(String appName) throws Exception;

    public void destroyApplication(String appName) throws Exception;

    public JobStatus getJobStatus(JobId jobId) throws Exception;

    public JobId startJob(String appName, byte[] acggfBytes, EnumSet<JobFlag> jobFlags) throws Exception;

    public void waitForCompletion(JobId jobId) throws Exception;

    public Map<String, NodeControllerInfo> getNodeControllersInfo() throws Exception;

    public ClusterTopology getClusterTopology() throws Exception;
}