package edu.uci.ics.hyracks.hadoop.compat.client;

import java.util.EnumSet;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;

import edu.uci.ics.hyracks.api.client.HyracksConnection;
import edu.uci.ics.hyracks.api.job.JobFlag;
import edu.uci.ics.hyracks.api.job.JobId;
import edu.uci.ics.hyracks.api.job.JobSpecification;
import edu.uci.ics.hyracks.api.job.JobStatus;
import edu.uci.ics.hyracks.hadoop.compat.util.ConfigurationConstants;
import edu.uci.ics.hyracks.hadoop.compat.util.Utilities;

public class HyracksClient {

    private static HyracksConnection connection;
    private static final String jobProfilingKey = "jobProfilingKey";
    Set<String> systemLibs;

    public HyracksClient(Properties clusterProperties) throws Exception {
        initialize(clusterProperties);
    }

    private void initialize(Properties properties) throws Exception {
        String clusterController = (String) properties.get(ConfigurationConstants.clusterControllerHost);
        connection = new HyracksConnection(clusterController, 1098);
        systemLibs = new HashSet<String>();
        for (String systemLib : ConfigurationConstants.systemLibs) {
            String systemLibPath = properties.getProperty(systemLib);
            if (systemLibPath != null) {
                systemLibs.add(systemLibPath);
            }
        }
    }

    public HyracksClient(String clusterConf, char delimiter) throws Exception {
        Properties properties = Utilities.getProperties(clusterConf, delimiter);
        initialize(properties);
    }

    private Set<String> getRequiredLibs(Set<String> userLibs) {
        Set<String> requiredLibs = new HashSet<String>();
        for (String systemLib : systemLibs) {
            requiredLibs.add(systemLib);
        }
        for (String userLib : userLibs) {
            requiredLibs.add(userLib);
        }
        return requiredLibs;
    }

    public JobStatus getJobStatus(JobId jobId) throws Exception {
        return connection.getJobStatus(jobId);
    }

    private void createApplication(String applicationName, Set<String> userLibs) throws Exception {
        connection.createApplication(applicationName,
                Utilities.getHyracksArchive(applicationName, getRequiredLibs(userLibs)));
    }

    public HyracksRunningJob submitJob(String applicationName, JobSpecification spec) throws Exception {
        String jobProfilingVal = System.getenv(jobProfilingKey);
        boolean doProfiling = ("true".equalsIgnoreCase(jobProfilingVal));
        JobId jobId;
        if (doProfiling) {
            System.out.println("PROFILING");
            jobId = connection.startJob(applicationName, spec, EnumSet.of(JobFlag.PROFILE_RUNTIME));
        } else {
            jobId = connection.startJob(applicationName, spec);
        }
        HyracksRunningJob runningJob = new HyracksRunningJob(jobId, spec, this);
        return runningJob;
    }

    public HyracksRunningJob submitJob(String applicationName, JobSpecification spec, Set<String> userLibs)
            throws Exception {
        createApplication(applicationName, userLibs);
        return submitJob(applicationName, spec);
    }

    public void waitForCompleton(JobId jobId) throws Exception {
        connection.waitForCompletion(jobId);
    }
}
