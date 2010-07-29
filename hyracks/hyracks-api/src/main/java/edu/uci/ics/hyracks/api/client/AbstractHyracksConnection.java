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

import java.net.InetAddress;
import java.util.EnumSet;
import java.util.Map;
import java.util.UUID;

import edu.uci.ics.hyracks.api.job.JobFlag;
import edu.uci.ics.hyracks.api.job.JobSpecification;
import edu.uci.ics.hyracks.api.job.JobStatus;
import edu.uci.ics.hyracks.api.job.statistics.JobStatistics;

abstract class AbstractHyracksConnection implements IHyracksClientConnection {
    private IHyracksClientInterface hci;

    public AbstractHyracksConnection(IHyracksClientInterface hci) {
        this.hci = hci;
    }

    @Override
    public void createApplication(String appName) throws Exception {
        hci.createApplication(appName);
    }

    @Override
    public void startApplication(String appName) throws Exception {
        hci.startApplication(appName);
    }

    @Override
    public void destroyApplication(String appName) throws Exception {
        hci.destroyApplication(appName);
    }

    @Override
    public UUID createJob(JobSpecification jobSpec) throws Exception {
        return hci.createJob(jobSpec);
    }

    @Override
    public UUID createJob(JobSpecification jobSpec, EnumSet<JobFlag> jobFlags) throws Exception {
        return hci.createJob(jobSpec, jobFlags);
    }

    @Override
    public JobStatus getJobStatus(UUID jobId) throws Exception {
        return hci.getJobStatus(jobId);
    }

    @Override
    public void start(UUID jobId) throws Exception {
        hci.start(jobId);
    }

    @Override
    public JobStatistics waitForCompletion(UUID jobId) throws Exception {
        return hci.waitForCompletion(jobId);
    }

    @Override
    public Map<String, InetAddress[]> getRegistry() throws Exception {
        return hci.getRegistry();
    }
}