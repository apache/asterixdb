package edu.uci.ics.hyracks.hadoop.compat.driver;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.Map.Entry;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import org.kohsuke.args4j.CmdLineParser;

import edu.uci.ics.hyracks.hadoop.compat.client.HyracksClient;
import edu.uci.ics.hyracks.hadoop.compat.client.HyracksRunningJob;
import edu.uci.ics.hyracks.hadoop.compat.util.CompatibilityConfig;
import edu.uci.ics.hyracks.hadoop.compat.util.ConfigurationConstants;
import edu.uci.ics.hyracks.hadoop.compat.util.DCacheHandler;
import edu.uci.ics.hyracks.hadoop.compat.util.HadoopAdapter;
import edu.uci.ics.hyracks.hadoop.compat.util.Utilities;
import edu.uci.ics.hyracks.api.job.JobSpecification;

public class CompatibilityLayer {

    HyracksClient hyracksClient;
    DCacheHandler dCacheHander = null;
    Properties clusterConf;
    Set<String> systemLibs;

    private static char configurationFileDelimiter = '=';
    private static final String dacheKeyPrefix = "dcache.key";

    public CompatibilityLayer(CompatibilityConfig clConfig) throws Exception {
        initialize(clConfig);
    }

    public HyracksRunningJob submitJobs(String[] jobFiles, Set<String> userLibs) throws Exception {
        Set<String> requiredLibs = getRequiredLibs(userLibs);
        List<JobConf> jobConfs = constructHadoopJobConfs(jobFiles);
        Map<String, String> dcacheTasks = preparePreLaunchDCacheTasks(jobFiles[0]);
        String tempDir = "/tmp";
        if (dcacheTasks.size() > 0) {
            HadoopAdapter hadoopAdapter = hyracksClient.getHadoopAdapter();
            for (String key : dcacheTasks.keySet()) {
                String destPath = tempDir + "/" + key + System.currentTimeMillis();
                hadoopAdapter.getHDFSClient().copyToLocalFile(new Path(dcacheTasks.get(key)), new Path(destPath));
                System.out.println(" source :" + dcacheTasks.get(key));
                System.out.println(" dest :" + destPath);
                System.out.println(" key :" + key);
                System.out.println(" value :" + destPath);
                dCacheHander.put(key, destPath);
            }
        }
        HyracksRunningJob hyraxRunningJob = hyracksClient.submitJobs(jobConfs, requiredLibs);
        return hyraxRunningJob;
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

    private void initialize(CompatibilityConfig clConfig) throws Exception {
        clusterConf = Utilities.getProperties(clConfig.clusterConf, configurationFileDelimiter);
        systemLibs = new HashSet<String>();
        for (String systemLib : ConfigurationConstants.systemLibs) {
            String systemLibPath = clusterConf.getProperty(systemLib);
            if (systemLibPath != null) {
                systemLibs.add(systemLibPath);
            }
        }
        String clusterControllerHost = clusterConf.getProperty(ConfigurationConstants.clusterControllerHost);
        String dacheServerConfiguration = clusterConf.getProperty(ConfigurationConstants.dcacheServerConfiguration);
        String fileSystem = clusterConf.getProperty(ConfigurationConstants.namenodeURL);
        hyracksClient = new HyracksClient(clusterControllerHost, fileSystem);
        try {
            dCacheHander = new DCacheHandler(dacheServerConfiguration);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private Map<String, String> initializeCustomProperties(Properties properties, String prefix) {
        Map<String, String> foundProperties = new HashMap<String, String>();
        Set<Entry<Object, Object>> entrySet = properties.entrySet();
        for (Entry entry : entrySet) {
            String key = (String) entry.getKey();
            String value = (String) entry.getValue();
            if ((key.startsWith(prefix))) {
                String actualKey = key.substring(prefix.length() + 1); // "cut off '<prefix>.' from the beginning"
                foundProperties.put(actualKey, value);
            }
        }
        return foundProperties;
    }

    public Map<String, String> preparePreLaunchDCacheTasks(String jobFile) {
        Properties jobProperties = Utilities.getProperties(jobFile, ',');
        Map<String, String> dcacheTasks = new HashMap<String, String>();
        Map<String, String> dcacheKeys = initializeCustomProperties(jobProperties, dacheKeyPrefix);
        for (String key : dcacheKeys.keySet()) {
            String sourcePath = dcacheKeys.get(key);
            if (sourcePath != null) {
                dcacheTasks.put(key, sourcePath);
            }
        }
        return dcacheTasks;
    }

    public void waitForCompletion(UUID jobId) throws Exception {
        hyracksClient.waitForCompleton(jobId);
    }

    public HyracksRunningJob submitHadoopJobToHyrax(JobConf jobConf, Set<String> userLibs) {
        HyracksRunningJob hyraxRunningJob = null;
        List<JobConf> jobConfs = new ArrayList<JobConf>();
        jobConfs.add(jobConf);
        try {
            hyraxRunningJob = hyracksClient.submitJobs(jobConfs, getRequiredLibs(userLibs));
            System.out.println(" Result in " + jobConf.get("mapred.output.dir"));
        } catch (Exception e) {
            e.printStackTrace();
        }
        return hyraxRunningJob;
    }

    public HyracksRunningJob submitJob(String appName, JobSpecification jobSpec, Set<String> userLibs) {
        HyracksRunningJob hyraxRunningJob = null;
        try {
            hyraxRunningJob = hyracksClient.submitJob(appName, jobSpec, getRequiredLibs(userLibs));
        } catch (Exception e) {
            e.printStackTrace();
        }
        return hyraxRunningJob;
    }

    private List<JobConf> constructHadoopJobConfs(String[] jobFiles) throws Exception {
        List<JobConf> jobConfs = new ArrayList<JobConf>();
        for (String jobFile : jobFiles) {
            jobConfs.add(constructHadoopJobConf(jobFile));
        }
        return jobConfs;
    }

    private JobConf constructHadoopJobConf(String jobFile) {
        Properties jobProperties = Utilities.getProperties(jobFile, '=');
        JobConf conf = hyracksClient.getHadoopAdapter().getConf();
        for (Entry entry : jobProperties.entrySet()) {
            conf.set((String) entry.getKey(), (String) entry.getValue());
            System.out.println((String) entry.getKey() + " : " + (String) entry.getValue());
        }
        return conf;
    }

    private String[] getJobs(CompatibilityConfig clConfig) {
        return clConfig.jobFiles == null ? new String[0] : clConfig.jobFiles.split(",");
    }

    public static void main(String args[]) throws Exception {
        long startTime = System.nanoTime();
        CompatibilityConfig clConfig = new CompatibilityConfig();
        CmdLineParser cp = new CmdLineParser(clConfig);
        try {
            cp.parseArgument(args);
        } catch (Exception e) {
            System.err.println(e.getMessage());
            cp.printUsage(System.err);
            return;
        }
        CompatibilityLayer compatLayer = new CompatibilityLayer(clConfig);
        String[] jobFiles = compatLayer.getJobs(clConfig);
        String[] tempUserLibs = clConfig.userLibs == null ? new String[0] : clConfig.userLibs.split(",");
        Set<String> userLibs = new HashSet<String>();
        for(String userLib : tempUserLibs) {
            userLibs.add(userLib);
        }
        HyracksRunningJob hyraxRunningJob = null;
        try {
            hyraxRunningJob = compatLayer.submitJobs(jobFiles, userLibs);
            compatLayer.waitForCompletion(hyraxRunningJob.getJobId());
        } catch (Exception e) {
            e.printStackTrace();
        }
        hyraxRunningJob.waitForCompletion();
        long end_time = System.nanoTime();
        System.out.println("TOTAL TIME (from Launch to Completion):" + ((end_time - startTime) / (float) 1000000000.0)
                + " seconds.");
    }

}
