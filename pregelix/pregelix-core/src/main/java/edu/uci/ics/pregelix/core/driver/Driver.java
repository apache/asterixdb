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

package edu.uci.ics.pregelix.core.driver;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import edu.uci.ics.hyracks.api.client.HyracksConnection;
import edu.uci.ics.hyracks.api.client.IHyracksClientConnection;
import edu.uci.ics.hyracks.api.deployment.DeploymentId;
import edu.uci.ics.hyracks.api.exceptions.HyracksException;
import edu.uci.ics.hyracks.api.job.JobFlag;
import edu.uci.ics.hyracks.api.job.JobId;
import edu.uci.ics.hyracks.api.job.JobSpecification;
import edu.uci.ics.pregelix.api.job.ICheckpointHook;
import edu.uci.ics.pregelix.api.job.PregelixJob;
import edu.uci.ics.pregelix.api.util.BspUtils;
import edu.uci.ics.pregelix.core.base.IDriver;
import edu.uci.ics.pregelix.core.jobgen.JobGen;
import edu.uci.ics.pregelix.core.jobgen.JobGenInnerJoin;
import edu.uci.ics.pregelix.core.jobgen.JobGenOuterJoin;
import edu.uci.ics.pregelix.core.jobgen.JobGenOuterJoinSingleSort;
import edu.uci.ics.pregelix.core.jobgen.JobGenOuterJoinSort;
import edu.uci.ics.pregelix.core.jobgen.clusterconfig.ClusterConfig;
import edu.uci.ics.pregelix.core.util.ExceptionUtilities;
import edu.uci.ics.pregelix.dataflow.util.IterationUtils;

@SuppressWarnings("rawtypes")
public class Driver implements IDriver {
    private static final Log LOG = LogFactory.getLog(Driver.class);
    private IHyracksClientConnection hcc;
    private Class exampleClass;
    private boolean profiling = false;

    public Driver(Class exampleClass) {
        this.exampleClass = exampleClass;
    }

    @Override
    public void runJob(PregelixJob job, String ipAddress, int port) throws HyracksException {
        runJob(job, Plan.OUTER_JOIN, ipAddress, port, false);
    }

    @Override
    public void runJobs(List<PregelixJob> jobs, String ipAddress, int port) throws HyracksException {
        runJobs(jobs, Plan.OUTER_JOIN, ipAddress, port, false);
    }

    @Override
    public void runJob(PregelixJob job, Plan planChoice, String ipAddress, int port, boolean profiling)
            throws HyracksException {
        runJobs(Collections.singletonList(job), planChoice, ipAddress, port, profiling);
    }

    @Override
    public void runJobs(List<PregelixJob> jobs, Plan planChoice, String ipAddress, int port, boolean profiling)
            throws HyracksException {
        try {
            if (jobs.size() <= 0) {
                throw new HyracksException("Please submit at least one job for execution!");
            }
            this.profiling = profiling;
            PregelixJob currentJob = jobs.get(0);
            PregelixJob lastJob = currentJob;
            addHadoopConfiguration(currentJob, ipAddress, port, true);
            JobGen jobGen = null;

            /** prepare job -- deploy jars */
            DeploymentId deploymentId = prepareJobs(ipAddress, port);
            LOG.info("job started");

            IntWritable lastSnapshotJobIndex = new IntWritable(0);
            IntWritable lastSnapshotSuperstep = new IntWritable(0);
            boolean failed = false;
            int retryCount = 0;
            int maxRetryCount = 3;
            jobGen = selectJobGen(planChoice, currentJob);

            do {
                try {
                    for (int i = lastSnapshotJobIndex.get(); i < jobs.size(); i++) {
                        lastJob = currentJob;
                        currentJob = jobs.get(i);
                        currentJob.setRecoveryCount(retryCount);

                        /** add hadoop configurations */
                        addHadoopConfiguration(currentJob, ipAddress, port, failed);
                        ICheckpointHook ckpHook = BspUtils.createCheckpointHook(currentJob.getConfiguration());

                        /** load the data */
                        if ((i == 0 || compatible(lastJob, currentJob)) && !failed) {
                            if (i != 0) {
                                finishJobs(jobGen, deploymentId);
                                /** invalidate/clear checkpoint */
                                lastSnapshotJobIndex.set(0);
                                lastSnapshotSuperstep.set(0);
                            }
                            jobGen.reset(currentJob);
                            loadData(currentJob, jobGen, deploymentId);
                        } else {
                            jobGen.reset(currentJob);
                        }

                        /** run loop-body jobs */
                        runLoopBody(deploymentId, currentJob, jobGen, i, lastSnapshotJobIndex, lastSnapshotSuperstep,
                                ckpHook, failed);
                        runClearState(deploymentId, jobGen);
                        failed = false;
                    }

                    /** finish the jobs */
                    finishJobs(jobGen, deploymentId);
                    /** clear checkpoints if any */
                    jobGen.clearCheckpoints();
                    hcc.unDeployBinary(deploymentId);
                } catch (Exception e1) {
                    Set<String> blackListNodes = new HashSet<String>();
                    /** disk failures or node failures */
                    if (ExceptionUtilities.recoverable(e1, blackListNodes)) {
                        ClusterConfig.addToBlackListNodes(blackListNodes);
                        failed = true;
                        retryCount++;
                    } else {
                        throw e1;
                    }
                }
            } while (failed && retryCount < maxRetryCount);
            LOG.info("job finished");
        } catch (Exception e) {
            throw new HyracksException(e);
        }
    }

    private boolean compatible(PregelixJob lastJob, PregelixJob currentJob) {
        Class lastVertexIdClass = BspUtils.getVertexIndexClass(lastJob.getConfiguration());
        Class lastVertexValueClass = BspUtils.getVertexValueClass(lastJob.getConfiguration());
        Class lastEdgeValueClass = BspUtils.getEdgeValueClass(lastJob.getConfiguration());
        Path lastOutputPath = FileOutputFormat.getOutputPath(lastJob);

        Class currentVertexIdClass = BspUtils.getVertexIndexClass(currentJob.getConfiguration());
        Class currentVertexValueClass = BspUtils.getVertexValueClass(currentJob.getConfiguration());
        Class currentEdegeValueClass = BspUtils.getEdgeValueClass(currentJob.getConfiguration());
        Path[] currentInputPaths = FileInputFormat.getInputPaths(currentJob);

        return lastVertexIdClass.equals(currentVertexIdClass)
                && lastVertexValueClass.equals(currentVertexValueClass)
                && lastEdgeValueClass.equals(currentEdegeValueClass)
                && (currentInputPaths.length == 0 || (currentInputPaths.length == 1 && lastOutputPath
                        .equals(currentInputPaths[0])));
    }

    private JobGen selectJobGen(Plan planChoice, PregelixJob currentJob) {
        JobGen jobGen;
        switch (planChoice) {
            case INNER_JOIN:
                jobGen = new JobGenInnerJoin(currentJob);
                break;
            case OUTER_JOIN:
                jobGen = new JobGenOuterJoin(currentJob);
                break;
            case OUTER_JOIN_SORT:
                jobGen = new JobGenOuterJoinSort(currentJob);
                break;
            case OUTER_JOIN_SINGLE_SORT:
                jobGen = new JobGenOuterJoinSingleSort(currentJob);
                break;
            default:
                jobGen = new JobGenInnerJoin(currentJob);
        }
        return jobGen;
    }

    private long loadData(PregelixJob currentJob, JobGen jobGen, DeploymentId deploymentId) throws IOException,
            Exception {
        long start;
        long end;
        long time;
        start = System.currentTimeMillis();
        FileSystem dfs = FileSystem.get(currentJob.getConfiguration());
        Path outputPath = FileOutputFormat.getOutputPath(currentJob);
        if (outputPath != null) {
            dfs.delete(outputPath, true);
        }
        runCreate(deploymentId, jobGen);
        runDataLoad(deploymentId, jobGen);
        end = System.currentTimeMillis();
        time = end - start;
        LOG.info("data loading finished " + time + "ms");
        return time;
    }

    private void finishJobs(JobGen jobGen, DeploymentId deploymentId) throws Exception {
        long start;
        long end;
        long time;
        start = System.currentTimeMillis();
        runHDFSWRite(deploymentId, jobGen);
        runCleanup(deploymentId, jobGen);
        end = System.currentTimeMillis();
        time = end - start;
        LOG.info("result writing finished " + time + "ms");
    }

    private DeploymentId prepareJobs(String ipAddress, int port) throws Exception {
        if (hcc == null) {
            hcc = new HyracksConnection(ipAddress, port);
        }
        URLClassLoader classLoader = (URLClassLoader) exampleClass.getClassLoader();
        List<File> jars = new ArrayList<File>();
        URL[] urls = classLoader.getURLs();
        for (URL url : urls)
            if (url.toString().endsWith(".jar"))
                jars.add(new File(url.getPath()));
        DeploymentId deploymentId = installApplication(jars);
        return deploymentId;
    }

    private void addHadoopConfiguration(PregelixJob job, String ipAddress, int port, boolean loadClusterConfig)
            throws HyracksException {
        URL hadoopCore = job.getClass().getClassLoader().getResource("core-site.xml");
        if (hadoopCore != null) {
            job.getConfiguration().addResource(hadoopCore);
        }
        URL hadoopMapRed = job.getClass().getClassLoader().getResource("mapred-site.xml");
        if (hadoopMapRed != null) {
            job.getConfiguration().addResource(hadoopMapRed);
        }
        URL hadoopHdfs = job.getClass().getClassLoader().getResource("hdfs-site.xml");
        if (hadoopHdfs != null) {
            job.getConfiguration().addResource(hadoopHdfs);
        }
        if (loadClusterConfig) {
            ClusterConfig.loadClusterConfig(ipAddress, port);
        }
    }

    private void runLoopBody(DeploymentId deploymentId, PregelixJob job, JobGen jobGen, int currentJobIndex,
            IntWritable snapshotJobIndex, IntWritable snapshotSuperstep, ICheckpointHook ckpHook, boolean doRecovery)
            throws Exception {
        if (doRecovery) {
            /** reload the checkpoint */
            if (snapshotSuperstep.get() > 0) {
                runClearState(deploymentId, jobGen);
                runLoadCheckpoint(deploymentId, jobGen, snapshotSuperstep.get());
            } else {
                runClearState(deploymentId, jobGen);
                loadData(job, jobGen, deploymentId);
            }
        }
        int i = doRecovery ? snapshotSuperstep.get() + 1 : 1;
        int ckpInterval = BspUtils.getCheckpointingInterval(job.getConfiguration());
        boolean terminate = false;
        long start, end, time;
        do {
            start = System.currentTimeMillis();
            runLoopBodyIteration(deploymentId, jobGen, i);
            end = System.currentTimeMillis();
            time = end - start;
            LOG.info(job + ": iteration " + i + " finished " + time + "ms");
            terminate = IterationUtils.readTerminationState(job.getConfiguration(), jobGen.getJobId())
                    || IterationUtils.readForceTerminationState(job.getConfiguration(), jobGen.getJobId());
            if (ckpHook.checkpoint(i) || (ckpInterval > 0 && i % ckpInterval == 0)) {
                runCheckpoint(deploymentId, jobGen, i);
                snapshotJobIndex.set(currentJobIndex);
                snapshotSuperstep.set(i);
            }
            i++;
        } while (!terminate);
    }

    private void runCheckpoint(DeploymentId deploymentId, JobGen jobGen, int lastSuccessfulIteration) throws Exception {
        try {
            JobSpecification[] ckpJobs = jobGen.generateCheckpointing(lastSuccessfulIteration);
            runJobArray(deploymentId, ckpJobs);
        } catch (Exception e) {
            throw e;
        }
    }

    private void runLoadCheckpoint(DeploymentId deploymentId, JobGen jobGen, int checkPointedIteration)
            throws Exception {
        try {
            JobSpecification[] ckpJobs = jobGen.generateLoadingCheckpoint(checkPointedIteration);
            runJobArray(deploymentId, ckpJobs);
        } catch (Exception e) {
            throw e;
        }
    }

    private void runCreate(DeploymentId deploymentId, JobGen jobGen) throws Exception {
        try {
            JobSpecification treeCreateSpec = jobGen.generateCreatingJob();
            execute(deploymentId, treeCreateSpec);
        } catch (Exception e) {
            throw e;
        }
    }

    private void runDataLoad(DeploymentId deploymentId, JobGen jobGen) throws Exception {
        try {
            JobSpecification bulkLoadJobSpec = jobGen.generateLoadingJob();
            execute(deploymentId, bulkLoadJobSpec);
        } catch (Exception e) {
            throw e;
        }
    }

    private void runLoopBodyIteration(DeploymentId deploymentId, JobGen jobGen, int iteration) throws Exception {
        try {
            JobSpecification loopBody = jobGen.generateJob(iteration);
            execute(deploymentId, loopBody);
        } catch (Exception e) {
            throw e;
        }
    }

    private void runHDFSWRite(DeploymentId deploymentId, JobGen jobGen) throws Exception {
        try {
            JobSpecification scanSortPrintJobSpec = jobGen.scanIndexWriteGraph();
            execute(deploymentId, scanSortPrintJobSpec);
        } catch (Exception e) {
            throw e;
        }
    }

    private void runCleanup(DeploymentId deploymentId, JobGen jobGen) throws Exception {
        try {
            JobSpecification[] cleanups = jobGen.generateCleanup();
            runJobArray(deploymentId, cleanups);
        } catch (Exception e) {
            throw e;
        }
    }

    private void runClearState(DeploymentId deploymentId, JobGen jobGen) throws Exception {
        try {
            JobSpecification clear = jobGen.generateClearState();
            execute(deploymentId, clear);
        } catch (Exception e) {
            throw e;
        }
    }

    private void runJobArray(DeploymentId deploymentId, JobSpecification[] jobs) throws Exception {
        for (JobSpecification job : jobs) {
            execute(deploymentId, job);
        }
    }

    private void execute(DeploymentId deploymentId, JobSpecification job) throws Exception {
        job.setUseConnectorPolicyForScheduling(false);
        job.setMaxReattempts(0);
        JobId jobId = hcc.startJob(deploymentId, job,
                profiling ? EnumSet.of(JobFlag.PROFILE_RUNTIME) : EnumSet.noneOf(JobFlag.class));
        hcc.waitForCompletion(jobId);
    }

    public DeploymentId installApplication(List<File> jars) throws Exception {
        List<String> allJars = new ArrayList<String>();
        for (File jar : jars) {
            allJars.add(jar.getAbsolutePath());
        }
        long start = System.currentTimeMillis();
        DeploymentId deploymentId = hcc.deployBinary(allJars);
        long end = System.currentTimeMillis();
        LOG.info("jar deployment finished " + (end - start) + "ms");
        return deploymentId;
    }
}

class FileFilter implements FilenameFilter {
    private String ext;

    public FileFilter(String ext) {
        this.ext = "." + ext;
    }

    public boolean accept(File dir, String name) {
        return name.endsWith(ext);
    }
}
