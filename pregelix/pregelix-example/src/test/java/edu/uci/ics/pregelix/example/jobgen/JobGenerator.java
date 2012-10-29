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
package edu.uci.ics.pregelix.example.jobgen;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import edu.uci.ics.pregelix.api.job.PregelixJob;
import edu.uci.ics.pregelix.example.ConnectedComponentsVertex;
import edu.uci.ics.pregelix.example.ConnectedComponentsVertex.SimpleConnectedComponentsVertexOutputFormat;
import edu.uci.ics.pregelix.example.PageRankVertex;
import edu.uci.ics.pregelix.example.PageRankVertex.SimplePageRankVertexInputFormat;
import edu.uci.ics.pregelix.example.PageRankVertex.SimplePageRankVertexOutputFormat;
import edu.uci.ics.pregelix.example.ReachabilityVertex;
import edu.uci.ics.pregelix.example.ReachabilityVertex.SimpleReachibilityVertexOutputFormat;
import edu.uci.ics.pregelix.example.ShortestPathsVertex;
import edu.uci.ics.pregelix.example.inputformat.TextConnectedComponentsInputFormat;
import edu.uci.ics.pregelix.example.inputformat.TextPageRankInputFormat;
import edu.uci.ics.pregelix.example.inputformat.TextReachibilityVertexInputFormat;
import edu.uci.ics.pregelix.example.inputformat.TextShortestPathsInputFormat;

public class JobGenerator {
    private static String outputBase = "src/test/resources/jobs/";
    private static String HDFS_INPUTPATH = "/webmap";
    private static String HDFS_OUTPUTPAH = "/result";

    private static String HDFS_INPUTPATH2 = "/webmapcomplex";
    private static String HDFS_OUTPUTPAH2 = "/resultcomplex";

    private static void generatePageRankJobReal(String jobName, String outputPath) throws IOException {
        PregelixJob job = new PregelixJob(jobName);
        job.setVertexClass(PageRankVertex.class);
        job.setVertexInputFormatClass(TextPageRankInputFormat.class);
        job.setVertexOutputFormatClass(SimplePageRankVertexOutputFormat.class);
        job.setMessageCombinerClass(PageRankVertex.SimpleSumCombiner.class);
        FileInputFormat.setInputPaths(job, HDFS_INPUTPATH);
        FileOutputFormat.setOutputPath(job, new Path(HDFS_OUTPUTPAH));
        job.getConfiguration().setLong(PregelixJob.NUM_VERTICE, 20);
        job.getConfiguration().writeXml(new FileOutputStream(new File(outputPath)));
    }

    private static void generatePageRankJobRealComplex(String jobName, String outputPath) throws IOException {
        PregelixJob job = new PregelixJob(jobName);
        job.setVertexClass(PageRankVertex.class);
        job.setVertexInputFormatClass(TextPageRankInputFormat.class);
        job.setVertexOutputFormatClass(SimplePageRankVertexOutputFormat.class);
        job.setMessageCombinerClass(PageRankVertex.SimpleSumCombiner.class);
        FileInputFormat.setInputPaths(job, HDFS_INPUTPATH2);
        FileOutputFormat.setOutputPath(job, new Path(HDFS_OUTPUTPAH2));
        job.getConfiguration().setLong(PregelixJob.NUM_VERTICE, 23);
        job.getConfiguration().writeXml(new FileOutputStream(new File(outputPath)));
    }

    private static void generateShortestPathJobReal(String jobName, String outputPath) throws IOException {
        PregelixJob job = new PregelixJob(jobName);
        job.setVertexClass(ShortestPathsVertex.class);
        job.setVertexInputFormatClass(TextShortestPathsInputFormat.class);
        job.setVertexOutputFormatClass(SimplePageRankVertexOutputFormat.class);
        job.setMessageCombinerClass(ShortestPathsVertex.SimpleMinCombiner.class);
        FileInputFormat.setInputPaths(job, HDFS_INPUTPATH);
        FileOutputFormat.setOutputPath(job, new Path(HDFS_OUTPUTPAH));
        job.getConfiguration().setLong(PregelixJob.NUM_VERTICE, 20);
        job.getConfiguration().setLong(ShortestPathsVertex.SOURCE_ID, 0);
        job.getConfiguration().writeXml(new FileOutputStream(new File(outputPath)));
    }

    private static void generatePageRankJobRealNoCombiner(String jobName, String outputPath) throws IOException {
        PregelixJob job = new PregelixJob(jobName);
        job.setVertexClass(PageRankVertex.class);
        job.setVertexInputFormatClass(TextPageRankInputFormat.class);
        job.setVertexOutputFormatClass(SimplePageRankVertexOutputFormat.class);
        FileInputFormat.setInputPaths(job, HDFS_INPUTPATH);
        FileOutputFormat.setOutputPath(job, new Path(HDFS_OUTPUTPAH));
        job.getConfiguration().setLong(PregelixJob.NUM_VERTICE, 20);
        job.getConfiguration().writeXml(new FileOutputStream(new File(outputPath)));
    }

    private static void generateConnectedComponentsJobReal(String jobName, String outputPath) throws IOException {
        PregelixJob job = new PregelixJob(jobName);
        job.setVertexClass(ConnectedComponentsVertex.class);
        job.setVertexInputFormatClass(TextConnectedComponentsInputFormat.class);
        job.setVertexOutputFormatClass(SimpleConnectedComponentsVertexOutputFormat.class);
        job.setMessageCombinerClass(ConnectedComponentsVertex.SimpleMinCombiner.class);
        FileInputFormat.setInputPaths(job, HDFS_INPUTPATH);
        FileOutputFormat.setOutputPath(job, new Path(HDFS_OUTPUTPAH));
        job.getConfiguration().setLong(PregelixJob.NUM_VERTICE, 20);
        job.getConfiguration().writeXml(new FileOutputStream(new File(outputPath)));
    }

    private static void generateConnectedComponentsJobRealComplex(String jobName, String outputPath) throws IOException {
        PregelixJob job = new PregelixJob(jobName);
        job.setVertexClass(ConnectedComponentsVertex.class);
        job.setVertexInputFormatClass(TextConnectedComponentsInputFormat.class);
        job.setVertexOutputFormatClass(SimpleConnectedComponentsVertexOutputFormat.class);
        job.setMessageCombinerClass(ConnectedComponentsVertex.SimpleMinCombiner.class);
        FileInputFormat.setInputPaths(job, HDFS_INPUTPATH2);
        FileOutputFormat.setOutputPath(job, new Path(HDFS_OUTPUTPAH2));
        job.getConfiguration().setLong(PregelixJob.NUM_VERTICE, 23);
        job.getConfiguration().writeXml(new FileOutputStream(new File(outputPath)));
    }

    private static void generateReachibilityRealComplex(String jobName, String outputPath) throws IOException {
        PregelixJob job = new PregelixJob(jobName);
        job.setVertexClass(ReachabilityVertex.class);
        job.setVertexInputFormatClass(TextReachibilityVertexInputFormat.class);
        job.setVertexOutputFormatClass(SimpleReachibilityVertexOutputFormat.class);
        job.setMessageCombinerClass(ReachabilityVertex.SimpleReachibilityCombiner.class);
        FileInputFormat.setInputPaths(job, HDFS_INPUTPATH2);
        FileOutputFormat.setOutputPath(job, new Path(HDFS_OUTPUTPAH2));
        job.getConfiguration().setLong(PregelixJob.NUM_VERTICE, 23);
        job.getConfiguration().setLong(ReachabilityVertex.SOURCE_ID, 1);
        job.getConfiguration().setLong(ReachabilityVertex.DEST_ID, 10);
        job.getConfiguration().writeXml(new FileOutputStream(new File(outputPath)));
    }

    private static void generateReachibilityRealComplexNoConnectivity(String jobName, String outputPath)
            throws IOException {
        PregelixJob job = new PregelixJob(jobName);
        job.setVertexClass(ReachabilityVertex.class);
        job.setVertexInputFormatClass(TextReachibilityVertexInputFormat.class);
        job.setVertexOutputFormatClass(SimpleReachibilityVertexOutputFormat.class);
        job.setMessageCombinerClass(ReachabilityVertex.SimpleReachibilityCombiner.class);
        FileInputFormat.setInputPaths(job, HDFS_INPUTPATH2);
        FileOutputFormat.setOutputPath(job, new Path(HDFS_OUTPUTPAH2));
        job.getConfiguration().setLong(PregelixJob.NUM_VERTICE, 23);
        job.getConfiguration().setLong(ReachabilityVertex.SOURCE_ID, 1);
        job.getConfiguration().setLong(ReachabilityVertex.DEST_ID, 25);
        job.getConfiguration().writeXml(new FileOutputStream(new File(outputPath)));
    }

    private static void generatePageRankJob(String jobName, String outputPath) throws IOException {
        PregelixJob job = new PregelixJob(jobName);
        job.setVertexClass(PageRankVertex.class);
        job.setVertexInputFormatClass(SimplePageRankVertexInputFormat.class);
        job.setMessageCombinerClass(PageRankVertex.SimpleSumCombiner.class);
        job.setVertexOutputFormatClass(SimplePageRankVertexOutputFormat.class);
        FileInputFormat.setInputPaths(job, HDFS_INPUTPATH);
        FileOutputFormat.setOutputPath(job, new Path(HDFS_OUTPUTPAH));
        job.getConfiguration().setLong(PregelixJob.NUM_VERTICE, 20);
        job.getConfiguration().writeXml(new FileOutputStream(new File(outputPath)));
    }

    private static void genPageRank() throws IOException {
        generatePageRankJob("PageRank", outputBase + "PageRank.xml");
        generatePageRankJobReal("PageRank", outputBase + "PageRankReal.xml");
        generatePageRankJobRealComplex("PageRank", outputBase + "PageRankRealComplex.xml");
        generatePageRankJobRealNoCombiner("PageRank", outputBase + "PageRankRealNoCombiner.xml");
    }

    private static void generateShortestPathJob(String jobName, String outputPath) throws IOException {
        PregelixJob job = new PregelixJob(jobName);
        job.setVertexClass(ShortestPathsVertex.class);
        job.setVertexInputFormatClass(SimplePageRankVertexInputFormat.class);
        job.setMessageCombinerClass(ShortestPathsVertex.SimpleMinCombiner.class);
        job.setVertexOutputFormatClass(SimplePageRankVertexOutputFormat.class);
        FileInputFormat.setInputPaths(job, HDFS_INPUTPATH);
        FileOutputFormat.setOutputPath(job, new Path(HDFS_OUTPUTPAH));
        job.getConfiguration().setLong(PregelixJob.NUM_VERTICE, 20);
        job.getConfiguration().setLong(ShortestPathsVertex.SOURCE_ID, 0);
        job.getConfiguration().writeXml(new FileOutputStream(new File(outputPath)));
    }

    private static void genShortestPath() throws IOException {
        generateShortestPathJob("ShortestPaths", outputBase + "ShortestPaths.xml");
        generateShortestPathJobReal("ShortestPaths", outputBase + "ShortestPathsReal.xml");
    }

    private static void genConnectedComponents() throws IOException {
        generateConnectedComponentsJobReal("ConnectedComponents", outputBase + "ConnectedComponentsReal.xml");
        generateConnectedComponentsJobRealComplex("ConnectedComponents", outputBase
                + "ConnectedComponentsRealComplex.xml");
    }

    private static void genReachibility() throws IOException {
        generateReachibilityRealComplex("Reachibility", outputBase + "ReachibilityRealComplex.xml");
        generateReachibilityRealComplexNoConnectivity("Reachibility", outputBase
                + "ReachibilityRealComplexNoConnectivity.xml");
    }

    public static void main(String[] args) throws IOException {
        genPageRank();
        genShortestPath();
        genConnectedComponents();
        genReachibility();
    }

}
