package edu.uci.ics.hyracks.hadoop.compat.client;

import java.io.File;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;

import org.apache.hadoop.mapred.JobConf;

import edu.uci.ics.hyracks.hadoop.compat.util.ConfigurationConstants;
import edu.uci.ics.hyracks.hadoop.compat.util.HadoopAdapter;
import edu.uci.ics.hyracks.hadoop.compat.util.Utilities;
import edu.uci.ics.hyracks.hadoop.compat.client.HyracksRunningJob;
import edu.uci.ics.hyracks.api.client.HyracksRMIConnection;
import edu.uci.ics.hyracks.api.job.JobSpecification;
import edu.uci.ics.hyracks.api.job.JobStatus;

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

    public HyracksRunningJob submitJobs(List<JobConf> confs, Set<String> requiredLibs) throws Exception {
        JobSpecification spec = hadoopAdapter.getJobSpecification(confs);
        String appName  = getApplicationNameHadoopJob(confs.get(0));
        return submitJob(appName,spec, requiredLibs);
    }

    private String getApplicationNameHadoopJob(JobConf jobConf) {
        String jar = jobConf.getJar();
        if( jar != null){
            return jar.substring(jar.lastIndexOf("/") >=0 ? jar.lastIndexOf("/") +1 : 0);
        }else {
            return "" + System.currentTimeMillis();
        }
    }
    
    public HyracksRunningJob submitJob(JobConf conf, Set<String> requiredLibs) throws Exception {
        JobSpecification spec = hadoopAdapter.getJobSpecification(conf);
        String appName  = getApplicationNameHadoopJob(conf);
        return submitJob(appName, spec, requiredLibs);
    }

    public JobStatus getJobStatus(UUID jobId) throws Exception {
        return connection.getJobStatus(jobId);
    }

    public HyracksRunningJob submitJob(String applicationName, JobSpecification spec, Set<String> requiredLibs) throws Exception {
        UUID jobId = null;
        try {
            jobId = connection.createJob(applicationName, spec);
        } catch (Exception e){
            System.out.println(" application not found, creating application" + applicationName);
            connection.createApplication(applicationName, Utilities.getHyracksArchive(applicationName, requiredLibs));
            jobId = connection.createJob(applicationName, spec);
        }
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
