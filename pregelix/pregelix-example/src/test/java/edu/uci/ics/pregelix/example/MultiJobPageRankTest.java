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

package edu.uci.ics.pregelix.example;

import java.io.File;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.junit.Test;

import edu.uci.ics.pregelix.api.job.PregelixJob;
import edu.uci.ics.pregelix.api.util.ConservativeCheckpointHook;
import edu.uci.ics.pregelix.core.driver.Driver;
import edu.uci.ics.pregelix.core.util.PregelixHyracksIntegrationUtil;
import edu.uci.ics.pregelix.example.PageRankVertex.SimplePageRankVertexOutputFormat;
import edu.uci.ics.pregelix.example.data.VLongNormalizedKeyComputer;
import edu.uci.ics.pregelix.example.inputformat.TextPageRankInputFormat;
import edu.uci.ics.pregelix.example.util.TestCluster;
import edu.uci.ics.pregelix.example.util.TestUtils;

/**
 * This test case tests multi-user workload, using PageRank.
 * 
 * @author yingyib
 */
public class MultiJobPageRankTest {
    private static String INPUTPATH = "data/webmap";
    private static String OUTPUTPAH = "actual/result";
    private static String OUTPUTPAH2 = "actual/result2";
    private static String EXPECTEDPATH = "src/test/resources/expected/PageRankReal";

    @Test
    public void test() throws Exception {
        TestCluster testCluster = new TestCluster();
        try {
            PregelixJob job = new PregelixJob(PageRankVertex.class.getName());
            job.setVertexClass(PageRankVertex.class);
            job.setVertexInputFormatClass(TextPageRankInputFormat.class);
            job.setVertexOutputFormatClass(SimplePageRankVertexOutputFormat.class);
            job.setMessageCombinerClass(PageRankVertex.SimpleSumCombiner.class);
            job.setNoramlizedKeyComputerClass(VLongNormalizedKeyComputer.class);
            FileInputFormat.setInputPaths(job, INPUTPATH);
            FileOutputFormat.setOutputPath(job, new Path(OUTPUTPAH));
            job.getConfiguration().setLong(PregelixJob.NUM_VERTICE, 20);
            job.setCheckpointHook(ConservativeCheckpointHook.class);
            job.setFixedVertexValueSize(true);

            testCluster.setUp();
            Thread thread = new Thread(new Runnable() {

                @Override
                public void run() {
                    try {
                        Driver driver = new Driver(PageRankVertex.class);
                        PregelixJob job2 = new PregelixJob(PageRankVertex.class.getName());
                        job2.setVertexClass(PageRankVertex.class);
                        job2.setVertexInputFormatClass(TextPageRankInputFormat.class);
                        job2.setVertexOutputFormatClass(SimplePageRankVertexOutputFormat.class);
                        job2.setMessageCombinerClass(PageRankVertex.SimpleSumCombiner.class);
                        job2.setNoramlizedKeyComputerClass(VLongNormalizedKeyComputer.class);
                        FileInputFormat.setInputPaths(job2, INPUTPATH);
                        FileOutputFormat.setOutputPath(job2, new Path(OUTPUTPAH2));
                        job2.getConfiguration().setLong(PregelixJob.NUM_VERTICE, 20);
                        job2.setCheckpointHook(ConservativeCheckpointHook.class);
                        job2.setFixedVertexValueSize(true);
                        driver.runJob(job2, "127.0.0.1", PregelixHyracksIntegrationUtil.TEST_HYRACKS_CC_CLIENT_PORT);
                        TestUtils.compareWithResultDir(new File(EXPECTEDPATH), new File(OUTPUTPAH2));
                    } catch (Exception e) {
                        throw new IllegalStateException(e);
                    }
                }
            });
            thread.start();
            Driver driver = new Driver(PageRankVertex.class);
            driver.runJob(job, "127.0.0.1", PregelixHyracksIntegrationUtil.TEST_HYRACKS_CC_CLIENT_PORT);
            TestUtils.compareWithResultDir(new File(EXPECTEDPATH), new File(OUTPUTPAH));
            thread.join();
        } catch (Exception e) {
            PregelixHyracksIntegrationUtil.deinit();
            testCluster.cleanupHDFS();
            throw e;
        }
    }
}
