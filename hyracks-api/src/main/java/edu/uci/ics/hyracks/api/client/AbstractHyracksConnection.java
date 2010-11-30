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

import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.entity.FileEntity;
import org.apache.http.impl.client.DefaultHttpClient;

import edu.uci.ics.hyracks.api.exceptions.HyracksException;
import edu.uci.ics.hyracks.api.job.JobFlag;
import edu.uci.ics.hyracks.api.job.JobSpecification;
import edu.uci.ics.hyracks.api.job.JobStatus;
import edu.uci.ics.hyracks.api.util.JavaSerializationUtils;

abstract class AbstractHyracksConnection implements IHyracksClientConnection {
    private final String ccHost;

    private final IHyracksClientInterface hci;

    private final ClusterControllerInfo ccInfo;

    public AbstractHyracksConnection(String ccHost, IHyracksClientInterface hci) throws Exception {
        this.ccHost = ccHost;
        this.hci = hci;
        ccInfo = hci.getClusterControllerInfo();
    }

    @Override
    public void createApplication(String appName, File harFile) throws Exception {
        hci.createApplication(appName);
        if (harFile != null) {
            HttpClient hc = new DefaultHttpClient();
            HttpPut put = new HttpPut("http://" + ccHost + ":" + ccInfo.getWebPort() + "/applications/" + appName);
            put.setEntity(new FileEntity(harFile, "application/octet-stream"));
            HttpResponse response = hc.execute(put);
            if (response.getStatusLine().getStatusCode() != 200) {
                hci.destroyApplication(appName);
                throw new HyracksException(response.getStatusLine().toString());
            }
        }
        hci.startApplication(appName);
    }

    @Override
    public void destroyApplication(String appName) throws Exception {
        hci.destroyApplication(appName);
    }

    @Override
    public UUID createJob(String appName, JobSpecification jobSpec) throws Exception {
        return createJob(appName, jobSpec, EnumSet.noneOf(JobFlag.class));
    }

    @Override
    public UUID createJob(String appName, JobSpecification jobSpec, EnumSet<JobFlag> jobFlags) throws Exception {
        return hci.createJob(appName, JavaSerializationUtils.serialize(jobSpec), jobFlags);
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
    public void waitForCompletion(UUID jobId) throws Exception {
        hci.waitForCompletion(jobId);
    }
}