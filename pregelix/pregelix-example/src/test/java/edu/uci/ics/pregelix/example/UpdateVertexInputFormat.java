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

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import edu.uci.ics.pregelix.api.graph.Vertex;
import edu.uci.ics.pregelix.api.io.VertexReader;
import edu.uci.ics.pregelix.api.io.text.TextVertexInputFormat;
import edu.uci.ics.pregelix.api.io.text.TextVertexInputFormat.TextVertexReader;
import edu.uci.ics.pregelix.api.util.BspUtils;
import edu.uci.ics.pregelix.example.io.DoubleWritable;
import edu.uci.ics.pregelix.example.io.VLongWritable;

public class UpdateVertexInputFormat extends TextVertexInputFormat<VLongWritable, Text, FloatWritable, DoubleWritable> {

    @Override
    public VertexReader<VLongWritable, Text, FloatWritable, DoubleWritable> createVertexReader(InputSplit split,
            TaskAttemptContext context) throws IOException {
        return new UpdateVertexReader(textInputFormat.createRecordReader(split, context), context);
    }
}

@SuppressWarnings("rawtypes")
class UpdateVertexReader extends TextVertexReader<VLongWritable, Text, FloatWritable, DoubleWritable> {

    private final static String separator = " ";
    private Vertex vertex;
    private VLongWritable vertexId = new VLongWritable();

    public UpdateVertexReader(RecordReader<LongWritable, Text> lineRecordReader, TaskAttemptContext context) {
        super(lineRecordReader);
    }

    @Override
    public boolean nextVertex() throws IOException, InterruptedException {
        return getRecordReader().nextKeyValue();
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
        Text line = getRecordReader().getCurrentValue();
        String[] fields = line.toString().split(separator);

        if (fields.length > 0) {
            /**
             * set the src vertex id
             */
            long src = Long.parseLong(fields[0]);
            vertexId.set(src);
            vertex.setVertexId(vertexId);

            /**
             * set the vertex value
             */
            vertex.setVertexValue(new Text("aaa"));
        }
        return vertex;
    }
}