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

package edu.uci.ics.pregelix.benchmark.io;

import java.io.IOException;

import org.apache.giraph.graph.Vertex;
import org.apache.giraph.io.formats.TextVertexOutputFormat;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

public class TextCCOutputFormat extends TextVertexOutputFormat<LongWritable, LongWritable, NullWritable> {

    @Override
    public TextVertexWriter createVertexWriter(TaskAttemptContext context) throws IOException, InterruptedException {
        return new TextVertexWriterToEachLine() {

            @Override
            protected Text convertVertexToLine(Vertex<LongWritable, LongWritable, NullWritable, ?> vertex)
                    throws IOException {
                return new Text(vertex.getId() + " " + vertex.getValue());
            }

        };
    }

}
