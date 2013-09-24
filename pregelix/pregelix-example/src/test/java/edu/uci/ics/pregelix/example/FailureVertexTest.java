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

import junit.framework.Assert;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.junit.Test;

import edu.uci.ics.pregelix.api.job.PregelixJob;
import edu.uci.ics.pregelix.core.driver.Driver;
import edu.uci.ics.pregelix.core.util.PregelixHyracksIntegrationUtil;
import edu.uci.ics.pregelix.example.ConnectedComponentsVertex;
import edu.uci.ics.pregelix.example.ConnectedComponentsVertex.SimpleConnectedComponentsVertexOutputFormat;
import edu.uci.ics.pregelix.example.FailureVertex;
import edu.uci.ics.pregelix.example.data.VLongNormalizedKeyComputer;
import edu.uci.ics.pregelix.example.inputformat.TextPageRankInputFormat;
import edu.uci.ics.pregelix.example.util.TestCluster;

/**
 * This test case tests the error message propagation.
 * 
 * @author yingyib
 */
public class FailureVertexTest {

    private static String INPUT_PATH = "data/webmapcomplex";
    private static String OUTPUT_PATH = "actual/resultcomplex";

    @Test
    public void test() throws Exception {
        TestCluster testCluster = new TestCluster();
        try {
            PregelixJob job = new PregelixJob(FailureVertex.class.getSimpleName());
            job.setVertexClass(FailureVertex.class);
            job.setVertexInputFormatClass(TextPageRankInputFormat.class);
            job.setVertexOutputFormatClass(SimpleConnectedComponentsVertexOutputFormat.class);
            job.setMessageCombinerClass(ConnectedComponentsVertex.SimpleMinCombiner.class);
            job.setNoramlizedKeyComputerClass(VLongNormalizedKeyComputer.class);
            job.setDynamicVertexValueSize(true);

            FileInputFormat.setInputPaths(job, INPUT_PATH);
            FileOutputFormat.setOutputPath(job, new Path(OUTPUT_PATH));
            job.getConfiguration().setLong(PregelixJob.NUM_VERTICE, 23);

            Driver driver = new Driver(FailureVertex.class);
            testCluster.setUp();
            driver.runJob(job, "127.0.0.1", PregelixHyracksIntegrationUtil.TEST_HYRACKS_CC_CLIENT_PORT);
        } catch (Exception e) {
            Assert.assertTrue(e.toString().contains("This job is going to fail"));
        } finally {
            testCluster.tearDown();
        }
    }

}
