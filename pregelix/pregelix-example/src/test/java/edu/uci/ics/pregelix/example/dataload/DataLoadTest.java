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
package edu.uci.ics.pregelix.example.dataload;

import java.io.File;
import java.io.IOException;
import java.util.logging.Logger;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.JobConf;
import org.junit.Test;

import edu.uci.ics.hyracks.api.job.JobSpecification;
import edu.uci.ics.pregelix.api.job.PregelixJob;
import edu.uci.ics.pregelix.core.jobgen.JobGenOuterJoin;
import edu.uci.ics.pregelix.core.jobgen.clusterconfig.ClusterConfig;
import edu.uci.ics.pregelix.core.util.PregelixHyracksIntegrationUtil;
import edu.uci.ics.pregelix.example.PageRankVertex;
import edu.uci.ics.pregelix.example.PageRankVertex.SimulatedPageRankVertexInputFormat;
import edu.uci.ics.pregelix.example.util.TestUtils;

@SuppressWarnings("deprecation")
public class DataLoadTest {
    private static final String EXPECT_RESULT_DIR = "expected";
    private static final String ACTUAL_RESULT_DIR = "actual";
    private static final String NC1 = "nc1";

    private static final Logger LOGGER = Logger.getLogger(DataLoadTest.class.getName());

    private static final String PATH_TO_HADOOP_CONF = "src/test/resources/hadoop/conf";
    private static final String PATH_TO_CLUSTER_STORE = "src/test/resources/cluster/stores.properties";
    private static final String PATH_TO_CLUSTER_PROPERTIES = "src/test/resources/cluster/cluster.properties";

    private static final String HYRACKS_APP_NAME = "giraph";
    private static final String GIRAPH_JOB_NAME = "DataLoadTest";

    private MiniDFSCluster dfsCluster;

    private JobConf conf = new JobConf();
    private int numberOfNC = 2;
    private JobGenOuterJoin giraphTestJobGen;
    private PregelixJob job;

    public DataLoadTest() throws Exception {
        job = new PregelixJob(GIRAPH_JOB_NAME);
        job.setVertexClass(PageRankVertex.class);
        job.setVertexInputFormatClass(SimulatedPageRankVertexInputFormat.class);
        job.getConfiguration().setClass(PregelixJob.VERTEX_INDEX_CLASS, LongWritable.class, WritableComparable.class);
        job.getConfiguration().setClass(PregelixJob.VERTEX_VALUE_CLASS, DoubleWritable.class, Writable.class);
        job.getConfiguration().setClass(PregelixJob.EDGE_VALUE_CLASS, FloatWritable.class, Writable.class);
        job.getConfiguration().setClass(PregelixJob.MESSAGE_VALUE_CLASS, DoubleWritable.class, Writable.class);
    }

    public void setUp() throws Exception {
        ClusterConfig.setStorePath(PATH_TO_CLUSTER_STORE);
        ClusterConfig.setClusterPropertiesPath(PATH_TO_CLUSTER_PROPERTIES);
        cleanupStores();
        PregelixHyracksIntegrationUtil.init();
        LOGGER.info("Hyracks mini-cluster started");
        startHDFS();
        FileUtils.forceMkdir(new File(EXPECT_RESULT_DIR));
        FileUtils.forceMkdir(new File(ACTUAL_RESULT_DIR));
        FileUtils.cleanDirectory(new File(EXPECT_RESULT_DIR));
        FileUtils.cleanDirectory(new File(ACTUAL_RESULT_DIR));
        giraphTestJobGen = new JobGenOuterJoin(job);
    }

    private void cleanupStores() throws IOException {
        FileUtils.forceMkdir(new File("teststore"));
        FileUtils.forceMkdir(new File("build"));
        FileUtils.cleanDirectory(new File("teststore"));
        FileUtils.cleanDirectory(new File("build"));
    }

    private void startHDFS() throws IOException {
        conf.addResource(new Path(PATH_TO_HADOOP_CONF + "/core-site.xml"));
        conf.addResource(new Path(PATH_TO_HADOOP_CONF + "/mapred-site.xml"));
        conf.addResource(new Path(PATH_TO_HADOOP_CONF + "/hdfs-site.xml"));
        FileSystem lfs = FileSystem.getLocal(new Configuration());
        lfs.delete(new Path("build"), true);
        System.setProperty("hadoop.log.dir", "logs");
        dfsCluster = new MiniDFSCluster(conf, numberOfNC, true, null);
    }

    /**
     * cleanup hdfs cluster
     */
    private void cleanupHDFS() throws Exception {
        dfsCluster.shutdown();
    }

    public void tearDown() throws Exception {
        PregelixHyracksIntegrationUtil.deinit();
        LOGGER.info("Hyracks mini-cluster shut down");
        cleanupHDFS();
    }

    @Test
    public void test() throws Exception {
        setUp();
        runDataScan();
        runCreation();
        runDataLoad();
        runIndexScan();
        try {
            compareResults();
        } catch (Exception e) {
            tearDown();
            throw e;
        }
        tearDown();
    }

    private void runCreation() throws Exception {
        try {
            JobSpecification bulkLoadJobSpec = giraphTestJobGen.generateCreatingJob();
            PregelixHyracksIntegrationUtil.runJob(bulkLoadJobSpec, HYRACKS_APP_NAME);
        } catch (Exception e) {
            throw e;
        }
    }

    private void runDataLoad() throws Exception {
        try {
            JobSpecification bulkLoadJobSpec = giraphTestJobGen.generateLoadingJob();
            PregelixHyracksIntegrationUtil.runJob(bulkLoadJobSpec, HYRACKS_APP_NAME);
        } catch (Exception e) {
            throw e;
        }
    }

    private void runDataScan() throws Exception {
        try {
            JobSpecification scanSortPrintJobSpec = giraphTestJobGen.scanSortPrintGraph(NC1, EXPECT_RESULT_DIR
                    + File.separator + job.getJobName());
            PregelixHyracksIntegrationUtil.runJob(scanSortPrintJobSpec, HYRACKS_APP_NAME);
        } catch (Exception e) {
            throw e;
        }
    }

    private void runIndexScan() throws Exception {
        try {
            JobSpecification scanSortPrintJobSpec = giraphTestJobGen.scanIndexPrintGraph(NC1, ACTUAL_RESULT_DIR
                    + File.separator + job.getJobName());
            PregelixHyracksIntegrationUtil.runJob(scanSortPrintJobSpec, HYRACKS_APP_NAME);
        } catch (Exception e) {
            throw e;
        }
    }

    private void compareResults() throws Exception {
        PregelixJob job = new PregelixJob(GIRAPH_JOB_NAME);
        TestUtils.compareWithResult(new File(EXPECT_RESULT_DIR + File.separator + job.getJobName()), new File(
                ACTUAL_RESULT_DIR + File.separator + job.getJobName()));
    }
}
