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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.pregelix.api.graph.Edge;
import edu.uci.ics.pregelix.api.graph.MessageCombiner;
import edu.uci.ics.pregelix.api.graph.MsgList;
import edu.uci.ics.pregelix.api.graph.Vertex;
import edu.uci.ics.pregelix.api.io.VertexWriter;
import edu.uci.ics.pregelix.api.io.text.TextVertexOutputFormat;
import edu.uci.ics.pregelix.api.io.text.TextVertexOutputFormat.TextVertexWriter;
import edu.uci.ics.pregelix.api.job.PregelixJob;
import edu.uci.ics.pregelix.api.util.BspUtils;
import edu.uci.ics.pregelix.dataflow.util.IterationUtils;
import edu.uci.ics.pregelix.example.client.Client;
import edu.uci.ics.pregelix.example.data.VLongNormalizedKeyComputer;
import edu.uci.ics.pregelix.example.inputformat.TextReachibilityVertexInputFormat;
import edu.uci.ics.pregelix.example.io.ByteWritable;
import edu.uci.ics.pregelix.example.io.VLongWritable;

/**
 * Demonstrates the basic Pregel reachibility query implementation, for undirected graph (e.g., Facebook, LinkedIn graph).
 */
public class ReachabilityVertex extends Vertex<VLongWritable, ByteWritable, FloatWritable, ByteWritable> {

    public static class SimpleReachibilityCombiner extends MessageCombiner<VLongWritable, ByteWritable, ByteWritable> {
        private ByteWritable agg = new ByteWritable();
        private MsgList<ByteWritable> msgList;

        @SuppressWarnings({ "rawtypes", "unchecked" })
        @Override
        public void init(MsgList msgList) {
            this.msgList = msgList;
            agg.set((byte) 0);
        }

        @Override
        public void stepPartial(VLongWritable vertexIndex, ByteWritable msg) throws HyracksDataException {
            int newState = agg.get() | msg.get();
            agg.set((byte) newState);
        }

        @Override
        public void stepFinal(VLongWritable vertexIndex, ByteWritable partialAggregate) throws HyracksDataException {
            int newState = agg.get() | partialAggregate.get();
            agg.set((byte) newState);
        }

        @Override
        public ByteWritable finishPartial() {
            return agg;
        }

        @Override
        public MsgList<ByteWritable> finishFinal() {
            msgList.clear();
            msgList.add(agg);
            return msgList;
        }
    }

    private ByteWritable tmpVertexValue = new ByteWritable();
    private long sourceId = -1;
    private long destId = -1;

    /** The source vertex id */
    public static final String SOURCE_ID = "ReachibilityVertex.sourceId";
    /** The destination vertex id */
    public static final String DEST_ID = "ReachibilityVertex.destId";
    /** Default source vertex id */
    public static final long SOURCE_ID_DEFAULT = 1;
    /** Default destination vertex id */
    public static final long DEST_ID_DEFAULT = 1;

    /**
     * Is this vertex the source id?
     * 
     * @return True if the source id
     */
    private boolean isSource(VLongWritable v) {
        return (v.get() == sourceId);
    }

    /**
     * Is this vertex the dest id?
     * 
     * @return True if the source id
     */
    private boolean isDest(VLongWritable v) {
        return (v.get() == destId);
    }

    @Override
    public void compute(Iterator<ByteWritable> msgIterator) throws Exception {
        if (sourceId < 0) {
            sourceId = getContext().getConfiguration().getLong(SOURCE_ID, SOURCE_ID_DEFAULT);
        }
        if (destId < 0) {
            destId = getContext().getConfiguration().getLong(DEST_ID, DEST_ID_DEFAULT);
        }
        if (getSuperstep() == 1) {
            boolean isSource = isSource(getVertexId());
            if (isSource) {
                tmpVertexValue.set((byte) 1);
                setVertexValue(tmpVertexValue);
            }
            boolean isDest = isDest(getVertexId());
            if (isDest) {
                tmpVertexValue.set((byte) 2);
                setVertexValue(tmpVertexValue);
            }
            if (isSource && isDest) {
                signalTerminate();
                return;
            }
            if (isSource || isDest) {
                sendOutMsgs();
            } else {
                tmpVertexValue.set((byte) 0);
                setVertexValue(tmpVertexValue);
            }
        } else {
            while (msgIterator.hasNext()) {
                ByteWritable msg = msgIterator.next();
                int msgValue = msg.get();
                if (msgValue < 3) {
                    int state = getVertexValue().get();
                    int newState = state | msgValue;
                    boolean changed = state == newState ? false : true;
                    if (changed) {
                        tmpVertexValue.set((byte) newState);
                        setVertexValue(tmpVertexValue);
                        if (newState < 3) {
                            sendOutMsgs();
                        } else {
                            signalTerminate();
                        }
                    }
                } else {
                    signalTerminate();
                }
            }
        }
        voteToHalt();
    }

    @Override
    public String toString() {
        return getVertexId() + " " + getVertexValue();
    }

    private void signalTerminate() throws Exception {
        writeReachibilityResult(getContext().getConfiguration(), true);
        terminateJob();
    }

    private void writeReachibilityResult(Configuration conf, boolean terminate) throws IOException {
        FileSystem dfs = FileSystem.get(conf);
        String pathStr = IterationUtils.TMP_DIR + BspUtils.getJobId(conf) + "reachibility";
        Path path = new Path(pathStr);
        if (!dfs.exists(path)) {
            FSDataOutputStream output = dfs.create(path, true);
            output.writeBoolean(terminate);
            output.flush();
            output.close();
        }
    }

    private void sendOutMsgs() {
        for (Edge<VLongWritable, FloatWritable> edge : getEdges()) {
            sendMsg(edge.getDestVertexId(), tmpVertexValue);
        }
    }

    private static boolean readReachibilityResult(Configuration conf) {
        try {
            FileSystem dfs = FileSystem.get(conf);
            String pathStr = IterationUtils.TMP_DIR + BspUtils.getJobId(conf) + "reachibility";
            Path path = new Path(pathStr);
            if (!dfs.exists(path)) {
                return false;
            } else {
                return true;
            }
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }

    public static void main(String[] args) throws Exception {
        PregelixJob job = new PregelixJob(ReachabilityVertex.class.getSimpleName());
        job.setVertexClass(ReachabilityVertex.class);
        job.setVertexInputFormatClass(TextReachibilityVertexInputFormat.class);
        job.setVertexOutputFormatClass(SimpleReachibilityVertexOutputFormat.class);
        job.setMessageCombinerClass(ReachabilityVertex.SimpleReachibilityCombiner.class);
        job.setNoramlizedKeyComputerClass(VLongNormalizedKeyComputer.class);
        Client.run(args, job);
        System.out.println("reachable? " + readReachibilityResult(job.getConfiguration()));
    }

    /**
     * Simple VertexWriter
     */
    public static class SimpleReachibilityVertexWriter extends
            TextVertexWriter<VLongWritable, ByteWritable, FloatWritable> {
        public SimpleReachibilityVertexWriter(RecordWriter<Text, Text> lineRecordWriter) {
            super(lineRecordWriter);
        }

        @Override
        public void writeVertex(Vertex<VLongWritable, ByteWritable, FloatWritable, ?> vertex) throws IOException,
                InterruptedException {
            getRecordWriter().write(new Text(vertex.getVertexId().toString()),
                    new Text(vertex.getVertexValue().toString()));
        }
    }

    /**
     * output format for reachibility
     */
    public static class SimpleReachibilityVertexOutputFormat extends
            TextVertexOutputFormat<VLongWritable, ByteWritable, FloatWritable> {

        @Override
        public VertexWriter<VLongWritable, ByteWritable, FloatWritable> createVertexWriter(TaskAttemptContext context)
                throws IOException, InterruptedException {
            RecordWriter<Text, Text> recordWriter = textOutputFormat.getRecordWriter(context);
            return new SimpleReachibilityVertexWriter(recordWriter);
        }

    }

}
