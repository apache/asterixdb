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
import java.util.ArrayList;
import java.util.List;

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
 * @author yingyib
 */
public class JobConcatenationTest {

    private static String INPUTPATH = "data/webmap";
    private static String OUTPUTPAH = "actual/result";
    private static String EXPECTEDPATH = "src/test/resources/expected/PageRankReal";

    @Test
    public void test() throws Exception {
        TestCluster testCluster = new TestCluster();

        try {
            List<PregelixJob> jobs = new ArrayList<PregelixJob>();
            PregelixJob job1 = new PregelixJob(PageRankVertex.class.getName());
            job1.setVertexClass(PageRankVertex.class);
            job1.setVertexInputFormatClass(TextPageRankInputFormat.class);
            job1.setVertexOutputFormatClass(SimplePageRankVertexOutputFormat.class);
            job1.setMessageCombinerClass(PageRankVertex.SimpleSumCombiner.class);
            job1.setNoramlizedKeyComputerClass(VLongNormalizedKeyComputer.class);
            FileInputFormat.setInputPaths(job1, INPUTPATH);
            job1.getConfiguration().setLong(PregelixJob.NUM_VERTICE, 20);
            job1.setCheckpointHook(ConservativeCheckpointHook.class);

            PregelixJob job2 = new PregelixJob(PageRankVertex.class.getName());
            job2.setVertexClass(PageRankVertex.class);
            job2.setVertexInputFormatClass(TextPageRankInputFormat.class);
            job2.setVertexOutputFormatClass(SimplePageRankVertexOutputFormat.class);
            job2.setMessageCombinerClass(PageRankVertex.SimpleSumCombiner.class);
            job2.setNoramlizedKeyComputerClass(VLongNormalizedKeyComputer.class);
            FileOutputFormat.setOutputPath(job2, new Path(OUTPUTPAH));
            job2.getConfiguration().setLong(PregelixJob.NUM_VERTICE, 20);
            job2.setCheckpointHook(ConservativeCheckpointHook.class);

            jobs.add(job1);
            jobs.add(job2);

            testCluster.setUp();
            Driver driver = new Driver(PageRankVertex.class);
            driver.runJobs(jobs, "127.0.0.1", PregelixHyracksIntegrationUtil.TEST_HYRACKS_CC_CLIENT_PORT);

            TestUtils.compareWithResultDir(new File(EXPECTEDPATH), new File(OUTPUTPAH));
        } catch (Exception e) {
            throw e;
        } finally {
            testCluster.tearDown();
        }
    }

}
