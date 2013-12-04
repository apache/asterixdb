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
import java.util.Collections;
import java.util.List;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import edu.uci.ics.pregelix.api.graph.Vertex;
import edu.uci.ics.pregelix.api.io.VertexInputFormat;
import edu.uci.ics.pregelix.api.io.VertexReader;
import edu.uci.ics.pregelix.api.util.BspUtils;
import edu.uci.ics.pregelix.example.io.DoubleWritable;
import edu.uci.ics.pregelix.example.io.VLongWritable;

public class UpdateVertexInputFormat extends VertexInputFormat<VLongWritable, Text, FloatWritable, DoubleWritable> {

    @Override
    public VertexReader<VLongWritable, Text, FloatWritable, DoubleWritable> createVertexReader(InputSplit split,
            TaskAttemptContext context) throws IOException {
        return new UpdateVertexReader(context);
    }

    @Override
    public List<InputSplit> getSplits(JobContext context, int numWorkers) throws IOException, InterruptedException {
        InputSplit split = new FileSplit(new Path("testdata"), 0, 0, new String[0]);
        return Collections.singletonList(split);
    }
}

@SuppressWarnings("rawtypes")
class UpdateVertexReader implements VertexReader<VLongWritable, Text, FloatWritable, DoubleWritable> {

    private final static int MAX_ID = 65536;
    private final static int MIN_ID = -65536;
    private Vertex vertex;
    private VLongWritable vertexId = new VLongWritable();
    private int currentId = MIN_ID;
    private TaskAttemptContext context;

    public UpdateVertexReader(TaskAttemptContext context) {
        this.context = context;
    }

    private TaskAttemptContext getContext() {
        return context;
    }

    @Override
    public boolean nextVertex() throws IOException, InterruptedException {
        return currentId < MAX_ID;
    }

    @SuppressWarnings("unchecked")
    @Override
    public Vertex<VLongWritable, Text, FloatWritable, DoubleWritable> getCurrentVertex() throws IOException,
            InterruptedException {
        if (vertex == null)
            vertex = (Vertex) BspUtils.createVertex(getContext().getConfiguration());
        vertex.getMsgList().clear();
        vertex.getEdges().clear();

        vertex.reset();

        /**
         * set the src vertex id
         */
        vertexId.set(currentId++);
        vertex.setVertexId(vertexId);

        /**
         * set the vertex value
         */
        vertex.setVertexValue(new Text("aaa"));
        return vertex;
    }

    @Override
    public void initialize(InputSplit inputSplit, TaskAttemptContext context) throws IOException, InterruptedException {

    }

    @Override
    public void close() throws IOException {

    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
        return 0;
    }
}