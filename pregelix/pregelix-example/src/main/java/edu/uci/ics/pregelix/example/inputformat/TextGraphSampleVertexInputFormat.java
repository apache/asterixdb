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
package edu.uci.ics.pregelix.example.inputformat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;

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
import edu.uci.ics.pregelix.example.io.BooleanWritable;
import edu.uci.ics.pregelix.example.io.NullWritable;
import edu.uci.ics.pregelix.example.io.VLongWritable;

public class TextGraphSampleVertexInputFormat extends
        TextVertexInputFormat<VLongWritable, BooleanWritable, NullWritable, BooleanWritable> {

    @Override
    public VertexReader<VLongWritable, BooleanWritable, NullWritable, BooleanWritable> createVertexReader(
            InputSplit split, TaskAttemptContext context) throws IOException {
        return new TextSampleGraphReader(textInputFormat.createRecordReader(split, context));
    }
}

@SuppressWarnings("rawtypes")
class TextSampleGraphReader extends TextVertexReader<VLongWritable, BooleanWritable, NullWritable, BooleanWritable> {

    private Vertex vertex;
    private VLongWritable vertexId = new VLongWritable();
    private List<VLongWritable> pool = new ArrayList<VLongWritable>();
    private int used = 0;
    private BooleanWritable value = new BooleanWritable(false);

    public TextSampleGraphReader(RecordReader<LongWritable, Text> lineRecordReader) {
        super(lineRecordReader);
    }

    @Override
    public boolean nextVertex() throws IOException, InterruptedException {
        return getRecordReader().nextKeyValue();
    }

    @SuppressWarnings("unchecked")
    @Override
    public Vertex<VLongWritable, BooleanWritable, NullWritable, BooleanWritable> getCurrentVertex() throws IOException,
            InterruptedException {
        used = 0;
        if (vertex == null)
            vertex = (Vertex) BspUtils.createVertex(getContext().getConfiguration());
        vertex.getMsgList().clear();
        vertex.getEdges().clear();

        vertex.reset();
        Text line = getRecordReader().getCurrentValue();
        String lineStr = line.toString();
        StringTokenizer tokenizer = new StringTokenizer(lineStr);

        if (tokenizer.hasMoreTokens()) {
            /**
             * set the src vertex id
             */
            long src = Long.parseLong(tokenizer.nextToken());
            vertexId.set(src);
            vertex.setVertexId(vertexId);
            long dest = -1L;

            /**
             * set up edges
             */
            while (tokenizer.hasMoreTokens()) {
                dest = Long.parseLong(tokenizer.nextToken());
                VLongWritable destId = allocate();
                destId.set(dest);
                vertex.addEdge(destId, value);
            }
        }
        vertex.setVertexValue(value);
        return vertex;
    }

    private VLongWritable allocate() {
        if (used >= pool.size()) {
            VLongWritable value = new VLongWritable();
            pool.add(value);
            used++;
            return value;
        } else {
            VLongWritable value = pool.get(used);
            used++;
            return value;
        }
    }
}
