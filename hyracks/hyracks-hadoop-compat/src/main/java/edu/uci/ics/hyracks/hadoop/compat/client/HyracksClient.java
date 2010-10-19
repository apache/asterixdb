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
package edu.uci.ics.hyracks.hadoop.compat.client;

import java.util.List;
import java.util.Properties;
import java.util.UUID;

import org.apache.hadoop.mapred.JobConf;

import edu.uci.ics.hyracks.api.client.HyracksRMIConnection;
import edu.uci.ics.hyracks.api.job.JobSpecification;
import edu.uci.ics.hyracks.api.job.JobStatus;
import edu.uci.ics.hyracks.hadoop.compat.util.ConfigurationConstants;
import edu.uci.ics.hyracks.hadoop.compat.util.HadoopAdapter;
import edu.uci.ics.hyracks.hadoop.compat.util.Utilities;

public class HyracksClient {

    private HadoopAdapter hadoopAdapter;
    private static HyracksRMIConnection connection;
    private static final String applicationName = "CompatibilityLayer";

    public HyracksClient(String clusterConf) throws Exception {
        Properties properties = Utilities.getProperties(clusterConf, '=');
        String clusterController = (String) properties.get(ConfigurationConstants.clusterControllerHost);
        String fileSystem = (String) properties.get(ConfigurationConstants.namenodeURL);
        initialize(clusterController, fileSystem);
    }

    public HyracksClient(String clusterControllerAddr, String fileSystem) throws Exception {
        initialize(clusterControllerAddr, fileSystem);
    }

    private void initialize(String clusterControllerAddr, String namenodeUrl) throws Exception {
        connection = new HyracksRMIConnection(clusterControllerAddr, 1099);
        connection.destroyApplication(applicationName);
        hadoopAdapter = new HadoopAdapter(namenodeUrl);
    }

    public HyracksRunningJob submitJobs(List<JobConf> confs, String[] requiredLibs) throws Exception {
        JobSpecification spec = hadoopAdapter.getJobSpecification(confs);
        return submitJob(spec, requiredLibs);
    }

    public HyracksRunningJob submitJob(JobConf conf, String[] requiredLibs) throws Exception {
        JobSpecification spec = hadoopAdapter.getJobSpecification(conf);
        return submitJob(spec, requiredLibs);
    }

    public JobStatus getJobStatus(UUID jobId) throws Exception {
        return connection.getJobStatus(jobId);
    }

    public HyracksRunningJob submitJob(JobSpecification spec, String[] requiredLibs) throws Exception {
        String applicationName = "" + spec.toString().hashCode();
        connection.createApplication(applicationName, Utilities.getHyracksArchive(applicationName, requiredLibs));
        System.out.println(" created application :" + applicationName);
        UUID jobId = connection.createJob(applicationName, spec);
        connection.start(jobId);
        HyracksRunningJob runningJob = new HyracksRunningJob(jobId, spec, this);
        return runningJob;
    }

    public HadoopAdapter getHadoopAdapter() {
        return hadoopAdapter;
    }

    public void setHadoopAdapter(HadoopAdapter hadoopAdapter) {
        this.hadoopAdapter = hadoopAdapter;
    }

    public void waitForCompleton(UUID jobId) throws Exception {
        connection.waitForCompletion(jobId);
    }

}
