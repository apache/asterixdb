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

package edu.uci.ics.pregelix.example.jobrun;

import java.io.File;

import junit.framework.TestCase;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.junit.Test;

import edu.uci.ics.pregelix.api.job.PregelixJob;
import edu.uci.ics.pregelix.core.base.IDriver.Plan;
import edu.uci.ics.pregelix.core.driver.Driver;
import edu.uci.ics.pregelix.core.util.PregelixHyracksIntegrationUtil;
import edu.uci.ics.pregelix.example.util.TestUtils;

public class RunJobTestCase extends TestCase {
    private static String HDFS_INPUTPATH = "/webmap";
    private static String HDFS_OUTPUTPAH = "/result";

    private static String HDFS_INPUTPATH2 = "/webmapcomplex";
    private static String HDFS_OUTPUTPAH2 = "/resultcomplex";

    private static String HDFS_INPUTPATH3 = "/clique";
    private static String HDFS_OUTPUTPAH3 = "/resultclique";

    private static String HDFS_INPUTPATH4 = "/clique2";
    private static String HDFS_OUTPUTPAH4 = "/resultclique";

    private static String HDFS_INPUTPATH5 = "/clique3";
    private static String HDFS_OUTPUTPAH5 = "/resultclique";

    private final PregelixJob job;
    private final String resultFileDir;
    private final String expectedFileDir;
    private final String jobFile;
    private final Driver driver = new Driver(this.getClass());
    private final FileSystem dfs;

    public RunJobTestCase(String hadoopConfPath, String jobName, String jobFile, String resultFile,
            String expectedFile, FileSystem dfs) throws Exception {
        super("test");
        this.jobFile = jobFile;
        this.job = new PregelixJob("test");
        this.job.getConfiguration().addResource(new Path(jobFile));
        this.job.getConfiguration().addResource(new Path(hadoopConfPath));
        Path[] inputPaths = FileInputFormat.getInputPaths(job);
        if (inputPaths[0].toString().endsWith(HDFS_INPUTPATH)) {
            FileInputFormat.setInputPaths(job, HDFS_INPUTPATH);
            FileOutputFormat.setOutputPath(job, new Path(HDFS_OUTPUTPAH));
        } else if (inputPaths[0].toString().endsWith(HDFS_INPUTPATH2)) {
            FileInputFormat.setInputPaths(job, HDFS_INPUTPATH2);
            FileOutputFormat.setOutputPath(job, new Path(HDFS_OUTPUTPAH2));
        } else if (inputPaths[0].toString().endsWith(HDFS_INPUTPATH3)) {
            FileInputFormat.setInputPaths(job, HDFS_INPUTPATH3);
            FileOutputFormat.setOutputPath(job, new Path(HDFS_OUTPUTPAH3));
        } else if (inputPaths[0].toString().endsWith(HDFS_INPUTPATH4)) {
            FileInputFormat.setInputPaths(job, HDFS_INPUTPATH4);
            FileOutputFormat.setOutputPath(job, new Path(HDFS_OUTPUTPAH4));
        } else if (inputPaths[0].toString().endsWith(HDFS_INPUTPATH5)) {
            FileInputFormat.setInputPaths(job, HDFS_INPUTPATH5);
            FileOutputFormat.setOutputPath(job, new Path(HDFS_OUTPUTPAH5));
        }
        job.setJobName(jobName);
        this.resultFileDir = resultFile;
        this.expectedFileDir = expectedFile;
        this.dfs = dfs;
    }

    private void waitawhile() throws InterruptedException {
        synchronized (this) {
            this.wait(20);
        }
    }

    @Test
    public void test() throws Exception {
        setUp();
        Plan[] plans = new Plan[] { Plan.INNER_JOIN, Plan.OUTER_JOIN, Plan.OUTER_JOIN_SINGLE_SORT, Plan.OUTER_JOIN_SORT };
        for (Plan plan : plans) {
            driver.runJob(job, plan, PregelixHyracksIntegrationUtil.CC_HOST,
                    PregelixHyracksIntegrationUtil.TEST_HYRACKS_CC_CLIENT_PORT, false);
            compareResults();
        }
        tearDown();
        waitawhile();
    }

    private void compareResults() throws Exception {
        FileUtils.deleteQuietly(new File(resultFileDir));
        dfs.copyToLocalFile(FileOutputFormat.getOutputPath(job), new Path(resultFileDir));
        TestUtils.compareWithResultDir(new File(expectedFileDir), new File(resultFileDir));
    }

    public String toString() {
        return jobFile;
    }
}
