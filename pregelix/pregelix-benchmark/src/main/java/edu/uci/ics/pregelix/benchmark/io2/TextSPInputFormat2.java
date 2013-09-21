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

package edu.uci.ics.pregelix.benchmark.io2;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.giraph.edge.Edge;
import org.apache.giraph.edge.MapMutableEdge;
import org.apache.giraph.io.formats.TextVertexInputFormat;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

public class TextSPInputFormat2 extends TextVertexInputFormat<LongWritable, DoubleWritable, DoubleWritable> {

    @Override
    public TextVertexReader createVertexReader(InputSplit split, TaskAttemptContext context) throws IOException {
        return new TextVertexReaderFromEachLine() {
            String[] items;

            @Override
            protected LongWritable getId(Text line) throws IOException {
                String[] kv = line.toString().split("\t");
                items = kv[1].split(" ");
                return new LongWritable(Long.parseLong(kv[0]));
            }

            @Override
            protected DoubleWritable getValue(Text line) throws IOException {
                return null;
            }

            @Override
            protected Iterable<Edge<LongWritable, DoubleWritable>> getEdges(Text line) throws IOException {
                List<Edge<LongWritable, DoubleWritable>> edges = new ArrayList<Edge<LongWritable, DoubleWritable>>();
                Map<LongWritable, DoubleWritable> edgeMap = new HashMap<LongWritable, DoubleWritable>();
                for (int i = 1; i < items.length; i++) {
                    edgeMap.put(new LongWritable(Long.parseLong(items[i])), null);
                }
                for (Entry<LongWritable, DoubleWritable> entry : edgeMap.entrySet()) {
                    MapMutableEdge<LongWritable, DoubleWritable> edge = new MapMutableEdge<LongWritable, DoubleWritable>();
                    edge.setEntry(entry);
                    edge.setValue(new DoubleWritable(1.0));
                    edges.add(edge);
                }
                return edges;
            }

        };
    }

}
