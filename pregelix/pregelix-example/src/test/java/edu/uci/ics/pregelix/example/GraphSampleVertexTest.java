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

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;

import junit.framework.Assert;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.junit.Test;

import edu.uci.ics.pregelix.api.job.PregelixJob;
import edu.uci.ics.pregelix.core.driver.Driver;
import edu.uci.ics.pregelix.core.util.PregelixHyracksIntegrationUtil;
import edu.uci.ics.pregelix.example.GraphSampleVertex.GraphSampleVertexOutputFormat;
import edu.uci.ics.pregelix.example.data.VLongNormalizedKeyComputer;
import edu.uci.ics.pregelix.example.inputformat.TextGraphSampleVertexInputFormat;
import edu.uci.ics.pregelix.example.util.TestCluster;

/**
 * @author yingyib
 */
public class GraphSampleVertexTest {
    private static String INPUTPATH = "data/webmapcomplex";
    private static String OUTPUTPAH = "actual/result";

    @Test
    public void test() throws Exception {
        TestCluster testCluster = new TestCluster();
        try {
            PregelixJob job = new PregelixJob(GraphSampleVertex.class.getName());
            job.setVertexClass(GraphSampleVertex.class);
            job.setVertexInputFormatClass(TextGraphSampleVertexInputFormat.class);
            job.setVertexOutputFormatClass(GraphSampleVertexOutputFormat.class);
            job.setMessageCombinerClass(GraphSampleVertex.SimpleSampleCombiner.class);
            job.addGlobalAggregatorClass(GraphSampleVertex.GlobalSamplingAggregator.class);
            job.setNoramlizedKeyComputerClass(VLongNormalizedKeyComputer.class);
            job.setFixedVertexValueSize(true);
            job.getConfiguration().set(GraphSampleVertex.GLOBAL_RATE, "0.5f");
            FileInputFormat.setInputPaths(job, INPUTPATH);
            FileOutputFormat.setOutputPath(job, new Path(OUTPUTPAH));

            testCluster.setUp();
            Driver driver = new Driver(GraphSampleVertex.class);
            driver.runJob(job, "127.0.0.1", PregelixHyracksIntegrationUtil.TEST_HYRACKS_CC_CLIENT_PORT);
            int sampledVertexNum = countVertex(OUTPUTPAH);
            int totalVertexNum = countVertex(INPUTPATH);
            float ratio = (float) sampledVertexNum / (float) totalVertexNum;
            Assert.assertEquals(true, ratio >= 0.5f);
        } finally {
            PregelixHyracksIntegrationUtil.deinit();
            testCluster.cleanupHDFS();
        }
    }

    private int countVertex(String filePath) throws Exception {
        File dir = new File(filePath);
        int count = 0;
        if (dir.isDirectory()) {
            File[] files = dir.listFiles();
            for (File file : files) {
                if (file.isFile() && !file.getName().contains(".crc")) {
                    BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(file)));
                    while (reader.readLine() != null) {
                        count++;
                    }
                    reader.close();
                }
            }
            return count;
        } else {
            return count;
        }
    }

}
