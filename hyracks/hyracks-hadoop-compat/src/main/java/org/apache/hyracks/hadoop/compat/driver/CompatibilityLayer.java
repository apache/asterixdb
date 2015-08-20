/*
 * Copyright 2009-2013 by The Regents of the University of California
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
package edu.uci.ics.hyracks.hadoop.compat.driver;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import org.kohsuke.args4j.CmdLineParser;

import edu.uci.ics.hyracks.api.job.JobId;
import edu.uci.ics.hyracks.api.job.JobSpecification;
import edu.uci.ics.hyracks.hadoop.compat.client.HyracksClient;
import edu.uci.ics.hyracks.hadoop.compat.client.HyracksRunningJob;
import edu.uci.ics.hyracks.hadoop.compat.util.CompatibilityConfig;
import edu.uci.ics.hyracks.hadoop.compat.util.ConfigurationConstants;
import edu.uci.ics.hyracks.hadoop.compat.util.DCacheHandler;
import edu.uci.ics.hyracks.hadoop.compat.util.HadoopAdapter;
import edu.uci.ics.hyracks.hadoop.compat.util.Utilities;

public class CompatibilityLayer {

    HyracksClient hyracksClient;
    DCacheHandler dCacheHander = null;
    Properties clusterConf;
    HadoopAdapter hadoopAdapter;

    private static char configurationFileDelimiter = '=';
    private static final String dacheKeyPrefix = "dcache.key";

    public CompatibilityLayer(CompatibilityConfig clConfig) throws Exception {
        initialize(clConfig);
    }

    private void initialize(CompatibilityConfig clConfig) throws Exception {
        clusterConf = Utilities.getProperties(clConfig.clusterConf, configurationFileDelimiter);
        hadoopAdapter = new HadoopAdapter(clusterConf.getProperty(ConfigurationConstants.namenodeURL));
        hyracksClient = new HyracksClient(clusterConf);
        dCacheHander = new DCacheHandler(clusterConf.getProperty(ConfigurationConstants.dcacheServerConfiguration));
    }

    public HyracksRunningJob submitJob(JobConf conf, Set<String> userLibs) throws Exception {
        List<JobConf> jobConfs = new ArrayList<JobConf>();
        jobConfs.add(conf);
        String applicationName = conf.getJobName() + System.currentTimeMillis();
        JobSpecification spec = hadoopAdapter.getJobSpecification(jobConfs);
        HyracksRunningJob hyracksRunningJob = hyracksClient.submitJob(applicationName, spec, userLibs);
        return hyracksRunningJob;
    }

    public HyracksRunningJob submitJobs(String applicationName, String[] jobFiles, Set<String> userLibs)
            throws Exception {
        List<JobConf> jobConfs = constructHadoopJobConfs(jobFiles);
        populateDCache(jobFiles[0]);
        JobSpecification spec = hadoopAdapter.getJobSpecification(jobConfs);
        HyracksRunningJob hyracksRunningJob = hyracksClient.submitJob(applicationName, spec, userLibs);
        return hyracksRunningJob;
    }

    public HyracksRunningJob submitJobs(String applicationName, String[] jobFiles) throws Exception {
        List<JobConf> jobConfs = constructHadoopJobConfs(jobFiles);
        populateDCache(jobFiles[0]);
        JobSpecification spec = hadoopAdapter.getJobSpecification(jobConfs);
        HyracksRunningJob hyracksRunningJob = hyracksClient.submitJob(applicationName, spec);
        return hyracksRunningJob;
    }

    private void populateDCache(String jobFile) throws IOException {
        Map<String, String> dcacheTasks = preparePreLaunchDCacheTasks(jobFile);
        String tempDir = "/tmp";
        if (dcacheTasks.size() > 0) {
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
    }

    private String getApplicationNameForHadoopJob(JobConf jobConf) {
        String jar = jobConf.getJar();
        if (jar != null) {
            return jar.substring(jar.lastIndexOf("/") >= 0 ? jar.lastIndexOf("/") + 1 : 0);
        } else {
            return "" + System.currentTimeMillis();
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

    public void waitForCompletion(JobId jobId) throws Exception {
        hyracksClient.waitForCompleton(jobId);
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
        JobConf conf = new JobConf(hadoopAdapter.getConf());
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
        String applicationName = clConfig.applicationName;
        String[] jobFiles = compatLayer.getJobs(clConfig);
        String[] userLibraries = null;
        if (clConfig.userLibs != null) {
            userLibraries = clConfig.userLibs.split(",");
        }
        try {
            HyracksRunningJob hyraxRunningJob = null;
            if (userLibraries != null) {
                Set<String> userLibs = new HashSet<String>();
                for (String userLib : userLibraries) {
                    userLibs.add(userLib);
                }
                hyraxRunningJob = compatLayer.submitJobs(applicationName, jobFiles, userLibs);
            } else {
                hyraxRunningJob = compatLayer.submitJobs(applicationName, jobFiles);
            }
            compatLayer.waitForCompletion(hyraxRunningJob.getJobId());
            long end_time = System.nanoTime();
            System.out.println("TOTAL TIME (from Launch to Completion):"
                    + ((end_time - startTime) / (float) 1000000000.0) + " seconds.");
        } catch (Exception e) {
            e.printStackTrace();
            throw e;
        }
    }
}
