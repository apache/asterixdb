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
package edu.uci.ics.pregelix.example.trianglecounting;

import java.io.IOException;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import edu.uci.ics.pregelix.api.graph.Edge;
import edu.uci.ics.pregelix.api.graph.Vertex;
import edu.uci.ics.pregelix.api.io.VertexWriter;
import edu.uci.ics.pregelix.api.io.text.TextVertexOutputFormat;
import edu.uci.ics.pregelix.api.io.text.TextVertexOutputFormat.TextVertexWriter;
import edu.uci.ics.pregelix.api.job.PregelixJob;
import edu.uci.ics.pregelix.api.util.BspUtils;
import edu.uci.ics.pregelix.dataflow.util.IterationUtils;
import edu.uci.ics.pregelix.example.client.Client;
import edu.uci.ics.pregelix.example.data.VLongNormalizedKeyComputer;
import edu.uci.ics.pregelix.example.io.VLongWritable;

/**
 * The triangle counting example -- counting the triangles in an undirected graph.
 */
public class TriangleCountingVertex extends Vertex<VLongWritable, VLongWritable, VLongWritable, VLongWritable> {

    private VLongWritable tmpValue = new VLongWritable(0);
    private long triangleCount = 0;
    private Edge<VLongWritable, VLongWritable> candidateEdge = new Edge<VLongWritable, VLongWritable>(
            new VLongWritable(0), new VLongWritable(0));
    private EdgeComparator edgeComparator = new EdgeComparator();

    @Override
    public void compute(Iterator<VLongWritable> msgIterator) {
        // transforms the edge list into a set to facilitate lookup
        if (getSuperstep() == 1) {
            // sorting edges could be avoid if the dataset already has that property
            sortEdges();
            List<Edge<VLongWritable, VLongWritable>> edges = this.getEdges();
            int numEdges = edges.size();

            //decoding longs
            long src = getVertexId().get();
            long[] dests = new long[numEdges];
            for (int i = 0; i < numEdges; i++) {
                dests[i] = edges.get(i).getDestVertexId().get();
            }

            //send messages -- take advantage of that each discovered 
            //triangle should have vertexes ordered by vertex id
            for (int i = 0; i < numEdges; i++) {
                if (dests[i] < src) {
                    for (int j = i + 1; j < numEdges; j++) {
                        //send messages -- v_j.id > v_i.id -- guaranteed by sortEdge()
                        if (dests[j] > src) {
                            sendMsg(edges.get(i).getDestVertexId(), edges.get(j).getDestVertexId());
                        }
                    }
                }
            }
        }
        if (getSuperstep() >= 2) {
            triangleCount = 0;
            List<Edge<VLongWritable, VLongWritable>> edges = this.getEdges();
            while (msgIterator.hasNext()) {
                VLongWritable msg = msgIterator.next();
                candidateEdge.setDestVertexId(msg);
                if (Collections.binarySearch(edges, candidateEdge, edgeComparator) >= 0) {
                    // if the msg value is a dest from this vertex
                    triangleCount++;
                }
            }

            // set vertex value
            tmpValue.set(triangleCount);
            setVertexValue(tmpValue);
            voteToHalt();
        }
    }

    /**
     * Triangle Counting VertexWriter
     */
    public static class TriangleCountingVertexWriter extends
            TextVertexWriter<VLongWritable, VLongWritable, VLongWritable> {
        public TriangleCountingVertexWriter(RecordWriter<Text, Text> lineRecordWriter) {
            super(lineRecordWriter);
        }

        @Override
        public void writeVertex(Vertex<VLongWritable, VLongWritable, VLongWritable, ?> vertex) throws IOException,
                InterruptedException {
            getRecordWriter().write(new Text(vertex.getVertexId().toString()),
                    new Text(vertex.getVertexValue().toString()));
        }
    }

    @Override
    public String toString() {
        return getVertexId() + " " + getVertexValue();
    }

    /**
     * output format for triangle counting
     */
    public static class TriangleCountingVertexOutputFormat extends
            TextVertexOutputFormat<VLongWritable, VLongWritable, VLongWritable> {

        @Override
        public VertexWriter<VLongWritable, VLongWritable, VLongWritable> createVertexWriter(TaskAttemptContext context)
                throws IOException, InterruptedException {
            RecordWriter<Text, Text> recordWriter = textOutputFormat.getRecordWriter(context);
            return new TriangleCountingVertexWriter(recordWriter);
        }

    }

    private static long readTriangleCountingResult(Configuration conf) {
        try {
            VLongWritable count = (VLongWritable) IterationUtils
                    .readGlobalAggregateValue(conf, BspUtils.getJobId(conf));
            return count.get();
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }

    public static void main(String[] args) throws Exception {
        PregelixJob job = new PregelixJob(TriangleCountingVertex.class.getSimpleName());
        job.setVertexClass(TriangleCountingVertex.class);
        job.setGlobalAggregatorClass(TriangleCountingAggregator.class);
        job.setVertexInputFormatClass(TextTriangleCountingInputFormat.class);
        job.setVertexOutputFormatClass(TriangleCountingVertexOutputFormat.class);
        job.setNoramlizedKeyComputerClass(VLongNormalizedKeyComputer.class);
        Client.run(args, job);
        System.out.println("triangle count: " + readTriangleCountingResult(job.getConfiguration()));
    }
}

/**
 * The comparator for Edge<VLongWritable, VLongWritable>.
 */
class EdgeComparator implements Comparator<Edge<VLongWritable, VLongWritable>> {

    @Override
    public int compare(Edge<VLongWritable, VLongWritable> left, Edge<VLongWritable, VLongWritable> right) {
        long leftValue = left.getDestVertexId().get();
        long rightValue = right.getDestVertexId().get();
        return leftValue > rightValue ? 1 : (leftValue < rightValue ? -1 : 0);
    }
}
