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
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import edu.uci.ics.pregelix.api.graph.Vertex;
import edu.uci.ics.pregelix.api.io.VertexWriter;
import edu.uci.ics.pregelix.api.io.text.TextVertexOutputFormat;
import edu.uci.ics.pregelix.example.io.VLongWritable;

public class UpdateVertexOutputFormat extends TextVertexOutputFormat<VLongWritable, Text, FloatWritable> {

    @Override
    public VertexWriter<VLongWritable, Text, FloatWritable> createVertexWriter(TaskAttemptContext context)
            throws IOException, InterruptedException {
        final RecordWriter<Text, Text> recordWriter = textOutputFormat.getRecordWriter(context);
        return new UpdateVertexWriter(recordWriter);
    }

    /**
     * Simple VertexWriter that support
     */
    public static class UpdateVertexWriter extends TextVertexWriter<VLongWritable, Text, FloatWritable> {
        public UpdateVertexWriter(RecordWriter<Text, Text> lineRecordWriter) {
            super(lineRecordWriter);
        }

        @Override
        public void writeVertex(Vertex<VLongWritable, Text, FloatWritable, ?> vertex) throws IOException,
                InterruptedException {
            int len = vertex.getVertexValue().toString().toCharArray().length;
            if (len != 1) {
                throw new IllegalStateException("invalid value length: " + len);
            }
            getRecordWriter().write(new Text(vertex.getVertexId().toString()), new Text(Integer.toString(len)));
        }
    }

}
