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

package edu.uci.ics.pregelix.example.jobrun;

import java.io.File;

import junit.framework.TestCase;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.junit.Test;

import edu.uci.ics.hyracks.api.job.JobSpecification;
import edu.uci.ics.pregelix.api.job.PregelixJob;
import edu.uci.ics.pregelix.core.jobgen.JobGen;
import edu.uci.ics.pregelix.core.jobgen.JobGenInnerJoin;
import edu.uci.ics.pregelix.core.jobgen.JobGenOuterJoin;
import edu.uci.ics.pregelix.core.jobgen.JobGenOuterJoinSingleSort;
import edu.uci.ics.pregelix.core.jobgen.JobGenOuterJoinSort;
import edu.uci.ics.pregelix.core.util.PregelixHyracksIntegrationUtil;
import edu.uci.ics.pregelix.dataflow.util.IterationUtils;
import edu.uci.ics.pregelix.example.util.TestUtils;

public class RunJobTestCase extends TestCase {
    private static final String NC1 = "nc1";
    private static final String HYRACKS_APP_NAME = "giraph";
    private static String HDFS_INPUTPATH = "/webmap";
    private static String HDFS_OUTPUTPAH = "/result";

    private static String HDFS_INPUTPATH2 = "/webmapcomplex";
    private static String HDFS_OUTPUTPAH2 = "/resultcomplex";

    private final PregelixJob job;
    private JobGen[] giraphJobGens;
    private final String resultFileName;
    private final String expectedFileName;
    private final String jobFile;

    public RunJobTestCase(String hadoopConfPath, String jobName, String jobFile, String resultFile, String expectedFile)
            throws Exception {
        super("test");
        this.jobFile = jobFile;
        this.job = new PregelixJob("test");
        this.job.getConfiguration().addResource(new Path(jobFile));
        this.job.getConfiguration().addResource(new Path(hadoopConfPath));
        Path[] inputPaths = FileInputFormat.getInputPaths(job);
        if (inputPaths[0].toString().endsWith(HDFS_INPUTPATH)) {
            FileInputFormat.setInputPaths(job, HDFS_INPUTPATH);
            FileOutputFormat.setOutputPath(job, new Path(HDFS_OUTPUTPAH));
        } else {
            FileInputFormat.setInputPaths(job, HDFS_INPUTPATH2);
            FileOutputFormat.setOutputPath(job, new Path(HDFS_OUTPUTPAH2));
        }
        job.setJobName(jobName);
        this.resultFileName = resultFile;
        this.expectedFileName = expectedFile;
        giraphJobGens = new JobGen[4];
        giraphJobGens[0] = new JobGenOuterJoin(job);
        waitawhile();
        giraphJobGens[1] = new JobGenInnerJoin(job);
        waitawhile();
        giraphJobGens[2] = new JobGenOuterJoinSort(job);
        waitawhile();
        giraphJobGens[3] = new JobGenOuterJoinSingleSort(job);
    }

    private void waitawhile() throws InterruptedException {
        synchronized (this) {
            this.wait(20);
        }
    }

    @Test
    public void test() throws Exception {
        setUp();
        for (JobGen jobGen : giraphJobGens) {
            FileSystem dfs = FileSystem.get(job.getConfiguration());
            dfs.delete(new Path(HDFS_OUTPUTPAH), true);
            runCreate(jobGen);
            runDataLoad(jobGen);
            int i = 1;
            boolean terminate = false;
            do {
                runLoopBodyIteration(jobGen, i);
                terminate = IterationUtils.readTerminationState(job.getConfiguration(), jobGen.getJobId());
                i++;
            } while (!terminate);
            runIndexScan(jobGen);
            runHDFSWRite(jobGen);
            runCleanup(jobGen);
            compareResults();
        }
        tearDown();
        waitawhile();
    }

    private void runCreate(JobGen jobGen) throws Exception {
        try {
            JobSpecification treeCreateJobSpec = jobGen.generateCreatingJob();
            PregelixHyracksIntegrationUtil.runJob(treeCreateJobSpec, HYRACKS_APP_NAME);
        } catch (Exception e) {
            throw e;
        }
    }

    private void runDataLoad(JobGen jobGen) throws Exception {
        try {
            JobSpecification bulkLoadJobSpec = jobGen.generateLoadingJob();
            PregelixHyracksIntegrationUtil.runJob(bulkLoadJobSpec, HYRACKS_APP_NAME);
        } catch (Exception e) {
            throw e;
        }
    }

    private void runLoopBodyIteration(JobGen jobGen, int iteration) throws Exception {
        try {
            JobSpecification loopBody = jobGen.generateJob(iteration);
            PregelixHyracksIntegrationUtil.runJob(loopBody, HYRACKS_APP_NAME);
        } catch (Exception e) {
            throw e;
        }
    }

    private void runIndexScan(JobGen jobGen) throws Exception {
        try {
            JobSpecification scanSortPrintJobSpec = jobGen.scanIndexPrintGraph(NC1, resultFileName);
            PregelixHyracksIntegrationUtil.runJob(scanSortPrintJobSpec, HYRACKS_APP_NAME);
        } catch (Exception e) {
            throw e;
        }
    }

    private void runHDFSWRite(JobGen jobGen) throws Exception {
        try {
            JobSpecification scanSortPrintJobSpec = jobGen.scanIndexWriteGraph();
            PregelixHyracksIntegrationUtil.runJob(scanSortPrintJobSpec, HYRACKS_APP_NAME);
        } catch (Exception e) {
            throw e;
        }
    }

    private void runCleanup(JobGen jobGen) throws Exception {
        try {
            JobSpecification[] cleanups = jobGen.generateCleanup();
            runJobArray(cleanups);
        } catch (Exception e) {
            throw e;
        }
    }

    private void runJobArray(JobSpecification[] jobs) throws Exception {
        for (JobSpecification job : jobs) {
            PregelixHyracksIntegrationUtil.runJob(job, HYRACKS_APP_NAME);
        }
    }

    private void compareResults() throws Exception {
        TestUtils.compareWithResult(new File(resultFileName), new File(expectedFileName));
    }

    public String toString() {
        return jobFile;
    }
}
