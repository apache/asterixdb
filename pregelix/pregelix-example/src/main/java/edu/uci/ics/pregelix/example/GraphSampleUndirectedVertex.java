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

import java.io.IOException;
import java.util.Iterator;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.pregelix.api.graph.Edge;
import edu.uci.ics.pregelix.api.graph.GlobalAggregator;
import edu.uci.ics.pregelix.api.graph.Vertex;
import edu.uci.ics.pregelix.api.io.VertexWriter;
import edu.uci.ics.pregelix.api.io.text.TextVertexOutputFormat;
import edu.uci.ics.pregelix.api.io.text.TextVertexOutputFormat.TextVertexWriter;
import edu.uci.ics.pregelix.api.job.PregelixJob;
import edu.uci.ics.pregelix.api.util.BspUtils;
import edu.uci.ics.pregelix.api.util.GlobalVertexCountAggregator;
import edu.uci.ics.pregelix.dataflow.util.IterationUtils;
import edu.uci.ics.pregelix.example.client.Client;
import edu.uci.ics.pregelix.example.data.VLongNormalizedKeyComputer;
import edu.uci.ics.pregelix.example.inputformat.TextGraphSampleVertexInputFormat;
import edu.uci.ics.pregelix.example.io.BooleanWritable;
import edu.uci.ics.pregelix.example.io.NullWritable;
import edu.uci.ics.pregelix.example.io.VLongWritable;

public class GraphSampleUndirectedVertex extends Vertex<VLongWritable, BooleanWritable, BooleanWritable, VLongWritable> {

    public static class GlobalSamplingAggregator
            extends
            GlobalAggregator<VLongWritable, BooleanWritable, BooleanWritable, BooleanWritable, LongWritable, LongWritable> {

        private LongWritable state = new LongWritable(0);

        @Override
        public void init() {
            state.set(0);
        }

        @Override
        public void step(Vertex<VLongWritable, BooleanWritable, BooleanWritable, BooleanWritable> v)
                throws HyracksDataException {
            if (v.getVertexValue().get() == true) {
                state.set(state.get() + 1);
            }
        }

        @Override
        public void step(LongWritable partialResult) {
            state.set(state.get() + partialResult.get());
        }

        @Override
        public LongWritable finishPartial() {
            return state;
        }

        @Override
        public LongWritable finishFinal() {
            return state;
        }

    }

    public static final String GLOBAL_RATE = "pregelix.globalrate";
    private int seedInterval = 0;
    private int samplingInterval = 2;
    private float globalRate = 0f;

    private Random random = new Random(System.currentTimeMillis());
    private BooleanWritable selectedFlag = new BooleanWritable(true);
    private float fillingRate = 0f;

    @Override
    public void configure(Configuration conf) {
        try {
            globalRate = conf.getFloat(GLOBAL_RATE, 0);
            seedInterval = (int) (1.0 / (globalRate / 100));
            if (getSuperstep() > 1) {
                LongWritable totalSelectedVertex = (LongWritable) IterationUtils.readGlobalAggregateValue(conf,
                        BspUtils.getJobId(conf), GlobalSamplingAggregator.class.getName());
                LongWritable totalVertex = (LongWritable) IterationUtils.readGlobalAggregateValue(conf,
                        BspUtils.getJobId(conf), GlobalVertexCountAggregator.class.getName());
                fillingRate = (float) totalSelectedVertex.get() / (float) totalVertex.get();
            }
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    @Override
    public void compute(Iterator<VLongWritable> msgIterator) throws Exception {
        if (getSuperstep() == 1) {
            initSeeds();
        } else {
            if (fillingRate >= globalRate) {
                if (msgIterator.hasNext()) {
                    setVertexValue(selectedFlag);
                    
                    //keep the giraph undirected
                    while (msgIterator.hasNext()) {
                        //mark the reverse edge
                        VLongWritable dest = msgIterator.next();
                        markEdge(dest);
                    }
                }
                voteToHalt();
            } else {
                initSeeds();
                if (msgIterator.hasNext()) {
                    markAsSelected();
                }
                
                //keep the graph undirected
                while (msgIterator.hasNext()) {
                    //mark the reverse edge
                    VLongWritable dest = msgIterator.next();
                    markEdge(dest);
                }
            }
        }
    }

    private void initSeeds() {
        int randVal = random.nextInt(seedInterval);
        if (randVal == 0) {
            markAsSelected();
        }
    }

    private void markAsSelected() {
        setVertexValue(selectedFlag);
        for (Edge<VLongWritable, BooleanWritable> edge : getEdges()) {
            int randVal = random.nextInt(samplingInterval);
            if (randVal == 0) {
                if (edge.getEdgeValue().get() == false) {
                    edge.getEdgeValue().set(true);
                    sendMsg(edge.getDestVertexId(), getVertexId());
                }
            }
        }
    }

    private void markEdge(VLongWritable destId) {
        for (Edge<VLongWritable, BooleanWritable> edge : getEdges()) {
            if (edge.getDestVertexId().equals(destId)) {
                if (edge.getEdgeValue().get() == false) {
                    edge.getEdgeValue().set(true);
                }
            }
        }
    }

    @Override
    public String toString() {
        StringBuffer strBuffer = new StringBuffer();
        strBuffer.append(getVertexId().toString());
        strBuffer.append(" ");
        for (Edge<VLongWritable, BooleanWritable> edge : getEdges()) {
            if (edge.getEdgeValue().get() == true) {
                strBuffer.append(edge.getDestVertexId());
                strBuffer.append(" ");
            }
        }
        return strBuffer.toString().trim();
    }

    public static void main(String[] args) throws Exception {
        PregelixJob job = new PregelixJob(GraphSampleUndirectedVertex.class.getSimpleName());
        job.setVertexClass(GraphSampleUndirectedVertex.class);
        job.setVertexInputFormatClass(TextGraphSampleVertexInputFormat.class);
        job.setVertexOutputFormatClass(GraphSampleVertexOutputFormat.class);
        job.addGlobalAggregatorClass(GraphSampleUndirectedVertex.GlobalSamplingAggregator.class);
        job.setNoramlizedKeyComputerClass(VLongNormalizedKeyComputer.class);
        job.setFixedVertexValueSize(true);
        job.setSkipCombinerKey(true);
        Client.run(args, job);
    }

    /**
     * write sampled vertices
     */
    public static class GraphSampleVertexWriter extends TextVertexWriter<VLongWritable, BooleanWritable, NullWritable> {
        public GraphSampleVertexWriter(RecordWriter<Text, Text> lineRecordWriter) {
            super(lineRecordWriter);
        }

        @Override
        public void writeVertex(Vertex<VLongWritable, BooleanWritable, NullWritable, ?> vertex) throws IOException,
                InterruptedException {
            if (vertex.getVertexValue().get() == true) {
                getRecordWriter().write(new Text(vertex.toString()), new Text());
            }
        }
    }

    /**
     * output format for sampled vertices
     */
    public static class GraphSampleVertexOutputFormat extends
            TextVertexOutputFormat<VLongWritable, BooleanWritable, NullWritable> {

        @Override
        public VertexWriter<VLongWritable, BooleanWritable, NullWritable> createVertexWriter(TaskAttemptContext context)
                throws IOException, InterruptedException {
            RecordWriter<Text, Text> recordWriter = textOutputFormat.getRecordWriter(context);
            return new GraphSampleVertexWriter(recordWriter);
        }

    }
}
