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
package edu.uci.ics.pregelix.example.jobgen;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import edu.uci.ics.pregelix.api.job.PregelixJob;
import edu.uci.ics.pregelix.api.util.ConservativeCheckpointHook;
import edu.uci.ics.pregelix.api.util.DefaultVertexPartitioner;
import edu.uci.ics.pregelix.example.ConnectedComponentsVertex;
import edu.uci.ics.pregelix.example.ConnectedComponentsVertex.SimpleConnectedComponentsVertexOutputFormat;
import edu.uci.ics.pregelix.example.EarlyTerminationVertex;
import edu.uci.ics.pregelix.example.EarlyTerminationVertex.SimpleEarlyTerminattionVertexOutputFormat;
import edu.uci.ics.pregelix.example.GraphMutationVertex;
import edu.uci.ics.pregelix.example.GraphMutationVertex.SimpleGraphMutationVertexOutputFormat;
import edu.uci.ics.pregelix.example.MessageOverflowFixedsizeVertex;
import edu.uci.ics.pregelix.example.MessageOverflowVertex;
import edu.uci.ics.pregelix.example.MessageOverflowVertex.SimpleMessageOverflowVertexOutputFormat;
import edu.uci.ics.pregelix.example.PageRankVertex;
import edu.uci.ics.pregelix.example.PageRankVertex.SimplePageRankVertexOutputFormat;
import edu.uci.ics.pregelix.example.PageRankVertex.SimulatedPageRankVertexInputFormat;
import edu.uci.ics.pregelix.example.ReachabilityVertex;
import edu.uci.ics.pregelix.example.ReachabilityVertex.SimpleReachibilityVertexOutputFormat;
import edu.uci.ics.pregelix.example.ShortestPathsVertex;
import edu.uci.ics.pregelix.example.data.VLongNormalizedKeyComputer;
import edu.uci.ics.pregelix.example.inputformat.TextConnectedComponentsInputFormat;
import edu.uci.ics.pregelix.example.inputformat.TextPageRankInputFormat;
import edu.uci.ics.pregelix.example.inputformat.TextReachibilityVertexInputFormat;
import edu.uci.ics.pregelix.example.inputformat.TextShortestPathsInputFormat;
import edu.uci.ics.pregelix.example.maximalclique.MaximalCliqueAggregator;
import edu.uci.ics.pregelix.example.maximalclique.MaximalCliqueVertex;
import edu.uci.ics.pregelix.example.maximalclique.MaximalCliqueVertex.MaximalCliqueVertexOutputFormat;
import edu.uci.ics.pregelix.example.maximalclique.TextMaximalCliqueInputFormat;
import edu.uci.ics.pregelix.example.trianglecounting.TextTriangleCountingInputFormat;
import edu.uci.ics.pregelix.example.trianglecounting.TriangleCountingAggregator;
import edu.uci.ics.pregelix.example.trianglecounting.TriangleCountingVertex;
import edu.uci.ics.pregelix.example.trianglecounting.TriangleCountingVertex.TriangleCountingVertexOutputFormat;

public class JobGenerator {
    private static String outputBase = "src/test/resources/jobs/";
    private static String HDFS_INPUTPATH = "/webmap";
    private static String HDFS_OUTPUTPAH = "/result";

    private static String HDFS_INPUTPATH2 = "/webmapcomplex";
    private static String HDFS_OUTPUTPAH2 = "/resultcomplex";

    private static String HDFS_INPUTPATH3 = "/clique";
    private static String HDFS_INPUTPATH4 = "/clique2";
    private static String HDFS_INPUTPATH5 = "/clique3";
    private static String HDFS_OUTPUTPAH3 = "/resultclique";

    private static void generatePageRankJobReal(String jobName, String outputPath) throws IOException {
        PregelixJob job = new PregelixJob(jobName);
        job.setVertexClass(PageRankVertex.class);
        job.setVertexInputFormatClass(TextPageRankInputFormat.class);
        job.setVertexOutputFormatClass(SimplePageRankVertexOutputFormat.class);
        job.setMessageCombinerClass(PageRankVertex.SimpleSumCombiner.class);
        job.setNoramlizedKeyComputerClass(VLongNormalizedKeyComputer.class);
        FileInputFormat.setInputPaths(job, HDFS_INPUTPATH);
        FileOutputFormat.setOutputPath(job, new Path(HDFS_OUTPUTPAH));
        job.getConfiguration().setLong(PregelixJob.NUM_VERTICE, 20);
        job.setCheckpointHook(ConservativeCheckpointHook.class);
        job.getConfiguration().writeXml(new FileOutputStream(new File(outputPath)));
    }

    private static void generatePageRankJobRealComplex(String jobName, String outputPath) throws IOException {
        PregelixJob job = new PregelixJob(jobName);
        job.setVertexClass(PageRankVertex.class);
        job.setVertexInputFormatClass(TextPageRankInputFormat.class);
        job.setVertexOutputFormatClass(SimplePageRankVertexOutputFormat.class);
        job.setMessageCombinerClass(PageRankVertex.SimpleSumCombiner.class);
        job.setNoramlizedKeyComputerClass(VLongNormalizedKeyComputer.class);
        job.setVertexPartitionerClass(DefaultVertexPartitioner.class);
        FileInputFormat.setInputPaths(job, HDFS_INPUTPATH2);
        FileOutputFormat.setOutputPath(job, new Path(HDFS_OUTPUTPAH2));
        job.getConfiguration().setLong(PregelixJob.NUM_VERTICE, 23);
        job.setCheckpointHook(ConservativeCheckpointHook.class);
        job.getConfiguration().writeXml(new FileOutputStream(new File(outputPath)));
    }

    private static void generateShortestPathJobReal(String jobName, String outputPath) throws IOException {
        PregelixJob job = new PregelixJob(jobName);
        job.setVertexClass(ShortestPathsVertex.class);
        job.setVertexInputFormatClass(TextShortestPathsInputFormat.class);
        job.setVertexOutputFormatClass(SimplePageRankVertexOutputFormat.class);
        job.setMessageCombinerClass(ShortestPathsVertex.SimpleMinCombiner.class);
        job.setNoramlizedKeyComputerClass(VLongNormalizedKeyComputer.class);
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
        job.setNoramlizedKeyComputerClass(VLongNormalizedKeyComputer.class);
        FileInputFormat.setInputPaths(job, HDFS_INPUTPATH);
        FileOutputFormat.setOutputPath(job, new Path(HDFS_OUTPUTPAH));
        job.getConfiguration().setLong(PregelixJob.NUM_VERTICE, 20);
        job.setCheckpointHook(ConservativeCheckpointHook.class);
        job.getConfiguration().writeXml(new FileOutputStream(new File(outputPath)));
    }

    private static void generateConnectedComponentsJobReal(String jobName, String outputPath) throws IOException {
        PregelixJob job = new PregelixJob(jobName);
        job.setVertexClass(ConnectedComponentsVertex.class);
        job.setVertexInputFormatClass(TextConnectedComponentsInputFormat.class);
        job.setVertexOutputFormatClass(SimpleConnectedComponentsVertexOutputFormat.class);
        job.setMessageCombinerClass(ConnectedComponentsVertex.SimpleMinCombiner.class);
        job.setNoramlizedKeyComputerClass(VLongNormalizedKeyComputer.class);
        job.setDynamicVertexValueSize(true);
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
        job.setNoramlizedKeyComputerClass(VLongNormalizedKeyComputer.class);
        job.setVertexPartitionerClass(DefaultVertexPartitioner.class);
        job.setDynamicVertexValueSize(true);
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
        job.setNoramlizedKeyComputerClass(VLongNormalizedKeyComputer.class);
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
        job.setNoramlizedKeyComputerClass(VLongNormalizedKeyComputer.class);
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
        job.setVertexInputFormatClass(SimulatedPageRankVertexInputFormat.class);
        job.setMessageCombinerClass(PageRankVertex.SimpleSumCombiner.class);
        job.setVertexOutputFormatClass(SimplePageRankVertexOutputFormat.class);
        job.setNoramlizedKeyComputerClass(VLongNormalizedKeyComputer.class);
        FileInputFormat.setInputPaths(job, HDFS_INPUTPATH);
        FileOutputFormat.setOutputPath(job, new Path(HDFS_OUTPUTPAH));
        job.getConfiguration().setLong(PregelixJob.NUM_VERTICE, 20);
        job.getConfiguration().writeXml(new FileOutputStream(new File(outputPath)));
    }

    private static void generateShortestPathJob(String jobName, String outputPath) throws IOException {
        PregelixJob job = new PregelixJob(jobName);
        job.setVertexClass(ShortestPathsVertex.class);
        job.setVertexInputFormatClass(SimulatedPageRankVertexInputFormat.class);
        job.setMessageCombinerClass(ShortestPathsVertex.SimpleMinCombiner.class);
        job.setVertexOutputFormatClass(SimplePageRankVertexOutputFormat.class);
        job.setNoramlizedKeyComputerClass(VLongNormalizedKeyComputer.class);
        FileInputFormat.setInputPaths(job, HDFS_INPUTPATH);
        FileOutputFormat.setOutputPath(job, new Path(HDFS_OUTPUTPAH));
        job.getConfiguration().setLong(PregelixJob.NUM_VERTICE, 20);
        job.getConfiguration().setLong(ShortestPathsVertex.SOURCE_ID, 0);
        job.getConfiguration().writeXml(new FileOutputStream(new File(outputPath)));
    }

    private static void generatePageRankJobRealDynamic(String jobName, String outputPath) throws IOException {
        PregelixJob job = new PregelixJob(jobName);
        job.setVertexClass(PageRankVertex.class);
        job.setVertexInputFormatClass(TextPageRankInputFormat.class);
        job.setVertexOutputFormatClass(SimplePageRankVertexOutputFormat.class);
        job.setMessageCombinerClass(PageRankVertex.SimpleSumCombiner.class);
        job.setNoramlizedKeyComputerClass(VLongNormalizedKeyComputer.class);
        job.setDynamicVertexValueSize(true);
        FileInputFormat.setInputPaths(job, HDFS_INPUTPATH);
        FileOutputFormat.setOutputPath(job, new Path(HDFS_OUTPUTPAH));
        job.getConfiguration().setLong(PregelixJob.NUM_VERTICE, 20);
        job.getConfiguration().writeXml(new FileOutputStream(new File(outputPath)));
    }

    private static void generateTriangleCountingJob(String jobName, String outputPath) throws IOException {
        PregelixJob job = new PregelixJob(jobName);
        job.setVertexClass(TriangleCountingVertex.class);
        job.setGlobalAggregatorClass(TriangleCountingAggregator.class);
        job.setVertexInputFormatClass(TextTriangleCountingInputFormat.class);
        job.setVertexOutputFormatClass(TriangleCountingVertexOutputFormat.class);
        job.setNoramlizedKeyComputerClass(VLongNormalizedKeyComputer.class);
        FileInputFormat.setInputPaths(job, HDFS_INPUTPATH3);
        FileOutputFormat.setOutputPath(job, new Path(HDFS_OUTPUTPAH3));
        job.getConfiguration().writeXml(new FileOutputStream(new File(outputPath)));
    }

    private static void generateMaximalCliqueJob(String jobName, String outputPath) throws IOException {
        PregelixJob job = new PregelixJob(jobName);
        job.setVertexClass(MaximalCliqueVertex.class);
        job.setGlobalAggregatorClass(MaximalCliqueAggregator.class);
        job.setDynamicVertexValueSize(true);
        job.setVertexInputFormatClass(TextMaximalCliqueInputFormat.class);
        job.setVertexOutputFormatClass(MaximalCliqueVertexOutputFormat.class);
        job.setNoramlizedKeyComputerClass(VLongNormalizedKeyComputer.class);
        job.setLSMStorage(true);
        FileInputFormat.setInputPaths(job, HDFS_INPUTPATH3);
        FileOutputFormat.setOutputPath(job, new Path(HDFS_OUTPUTPAH3));
        job.getConfiguration().writeXml(new FileOutputStream(new File(outputPath)));
    }

    private static void generateMaximalCliqueJob2(String jobName, String outputPath) throws IOException {
        PregelixJob job = new PregelixJob(jobName);
        job.setVertexClass(MaximalCliqueVertex.class);
        job.setGlobalAggregatorClass(MaximalCliqueAggregator.class);
        job.setDynamicVertexValueSize(true);
        job.setVertexInputFormatClass(TextMaximalCliqueInputFormat.class);
        job.setVertexOutputFormatClass(MaximalCliqueVertexOutputFormat.class);
        job.setNoramlizedKeyComputerClass(VLongNormalizedKeyComputer.class);
        job.setLSMStorage(true);
        FileInputFormat.setInputPaths(job, HDFS_INPUTPATH4);
        FileOutputFormat.setOutputPath(job, new Path(HDFS_OUTPUTPAH3));
        job.getConfiguration().writeXml(new FileOutputStream(new File(outputPath)));
    }

    private static void generateMaximalCliqueJob3(String jobName, String outputPath) throws IOException {
        PregelixJob job = new PregelixJob(jobName);
        job.setVertexClass(MaximalCliqueVertex.class);
        job.setGlobalAggregatorClass(MaximalCliqueAggregator.class);
        job.setDynamicVertexValueSize(true);
        job.setVertexInputFormatClass(TextMaximalCliqueInputFormat.class);
        job.setVertexOutputFormatClass(MaximalCliqueVertexOutputFormat.class);
        job.setNoramlizedKeyComputerClass(VLongNormalizedKeyComputer.class);
        job.setVertexPartitionerClass(DefaultVertexPartitioner.class);
        job.setLSMStorage(true);
        FileInputFormat.setInputPaths(job, HDFS_INPUTPATH5);
        FileOutputFormat.setOutputPath(job, new Path(HDFS_OUTPUTPAH3));
        job.getConfiguration().writeXml(new FileOutputStream(new File(outputPath)));
    }

    private static void generateGraphMutationJob(String jobName, String outputPath) throws IOException {
        PregelixJob job = new PregelixJob(jobName);
        job.setVertexClass(GraphMutationVertex.class);
        job.setVertexInputFormatClass(TextPageRankInputFormat.class);
        job.setVertexOutputFormatClass(SimpleGraphMutationVertexOutputFormat.class);
        job.setNoramlizedKeyComputerClass(VLongNormalizedKeyComputer.class);
        FileInputFormat.setInputPaths(job, HDFS_INPUTPATH);
        FileOutputFormat.setOutputPath(job, new Path(HDFS_OUTPUTPAH));
        job.getConfiguration().setLong(PregelixJob.NUM_VERTICE, 20);
        job.getConfiguration().writeXml(new FileOutputStream(new File(outputPath)));
    }

    private static void generateMessageOverflowFixedsizeJob(String jobName, String outputPath) throws IOException {
        PregelixJob job = new PregelixJob(jobName);
        job.setVertexClass(MessageOverflowFixedsizeVertex.class);
        job.setVertexInputFormatClass(TextPageRankInputFormat.class);
        job.setVertexOutputFormatClass(MessageOverflowFixedsizeVertex.SimpleMessageOverflowVertexOutputFormat.class);
        job.setFrameSize(2048);
        FileInputFormat.setInputPaths(job, HDFS_INPUTPATH);
        FileOutputFormat.setOutputPath(job, new Path(HDFS_OUTPUTPAH));
        job.getConfiguration().setLong(PregelixJob.NUM_VERTICE, 20);
        job.getConfiguration().writeXml(new FileOutputStream(new File(outputPath)));
    }

    private static void generateMessageOverflowJob(String jobName, String outputPath) throws IOException {
        PregelixJob job = new PregelixJob(jobName);
        job.setVertexClass(MessageOverflowVertex.class);
        job.setVertexInputFormatClass(TextPageRankInputFormat.class);
        job.setVertexOutputFormatClass(SimpleMessageOverflowVertexOutputFormat.class);
        job.setNoramlizedKeyComputerClass(VLongNormalizedKeyComputer.class);
        job.setDynamicVertexValueSize(true);
        job.setFrameSize(2048);
        FileInputFormat.setInputPaths(job, HDFS_INPUTPATH);
        FileOutputFormat.setOutputPath(job, new Path(HDFS_OUTPUTPAH));
        job.getConfiguration().setLong(PregelixJob.NUM_VERTICE, 20);
        job.getConfiguration().writeXml(new FileOutputStream(new File(outputPath)));
    }

    private static void generateMessageOverflowJobLSM(String jobName, String outputPath) throws IOException {
        PregelixJob job = new PregelixJob(jobName);
        job.setVertexClass(MessageOverflowVertex.class);
        job.setVertexInputFormatClass(TextPageRankInputFormat.class);
        job.setVertexOutputFormatClass(SimpleMessageOverflowVertexOutputFormat.class);
        job.setNoramlizedKeyComputerClass(VLongNormalizedKeyComputer.class);
        job.setDynamicVertexValueSize(true);
        job.setFrameSize(2048);
        job.setLSMStorage(true);
        FileInputFormat.setInputPaths(job, HDFS_INPUTPATH);
        FileOutputFormat.setOutputPath(job, new Path(HDFS_OUTPUTPAH));
        job.getConfiguration().setLong(PregelixJob.NUM_VERTICE, 20);
        job.getConfiguration().writeXml(new FileOutputStream(new File(outputPath)));
    }

    private static void generateEarlyTerminationJob(String jobName, String outputPath) throws IOException {
        PregelixJob job = new PregelixJob(jobName);
        job.setVertexClass(EarlyTerminationVertex.class);
        job.setVertexInputFormatClass(TextPageRankInputFormat.class);
        job.setVertexOutputFormatClass(SimpleEarlyTerminattionVertexOutputFormat.class);
        job.setNoramlizedKeyComputerClass(VLongNormalizedKeyComputer.class);
        FileInputFormat.setInputPaths(job, HDFS_INPUTPATH);
        FileOutputFormat.setOutputPath(job, new Path(HDFS_OUTPUTPAH));
        job.getConfiguration().setLong(PregelixJob.NUM_VERTICE, 20);
        job.getConfiguration().writeXml(new FileOutputStream(new File(outputPath)));
    }

    private static void genPageRank() throws IOException {
        generatePageRankJob("PageRank", outputBase + "PageRank.xml");
        generatePageRankJobReal("PageRank", outputBase + "PageRankReal.xml");
        generatePageRankJobRealDynamic("PageRank", outputBase + "PageRankRealDynamic.xml");
        generatePageRankJobRealComplex("PageRank", outputBase + "PageRankRealComplex.xml");
        generatePageRankJobRealNoCombiner("PageRank", outputBase + "PageRankRealNoCombiner.xml");
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

    private static void genTriangleCounting() throws IOException {
        generateTriangleCountingJob("Triangle Counting", outputBase + "TriangleCounting.xml");
    }

    private static void genMaximalClique() throws IOException {
        generateMaximalCliqueJob("Maximal Clique", outputBase + "MaximalClique.xml");
        generateMaximalCliqueJob2("Maximal Clique 2", outputBase + "MaximalClique2.xml");
        generateMaximalCliqueJob3("Maximal Clique 3", outputBase + "MaximalClique3.xml");
    }

    private static void genGraphMutation() throws IOException {
        generateGraphMutationJob("Graph Mutation", outputBase + "GraphMutation.xml");
    }

    private static void genMessageOverflow() throws IOException {
        generateMessageOverflowJob("Message Overflow", outputBase + "MessageOverflow.xml");
        generateMessageOverflowJobLSM("Message Overflow LSM", outputBase + "MessageOverflowLSM.xml");
        generateMessageOverflowFixedsizeJob("Message Overflow Fixedsize", outputBase + "MessageOverflowFixedsize.xml");
    }

    private static void genEarlyTermination() throws IOException {
        generateEarlyTerminationJob("Early Termination", outputBase + "EarlyTermination.xml");
    }

    public static void main(String[] args) throws IOException {
        genPageRank();
        genShortestPath();
        genConnectedComponents();
        genReachibility();
        genTriangleCounting();
        genMaximalClique();
        genGraphMutation();
        genMessageOverflow();
        genEarlyTermination();
    }
}
