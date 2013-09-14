package edu.uci.ics.pregelix.benchmark.io;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.giraph.edge.Edge;
import org.apache.giraph.edge.MapMutableEdge;
import org.apache.giraph.io.formats.TextVertexInputFormat;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.VLongWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

public class TextCCInputFormat2 extends TextVertexInputFormat<VLongWritable, VLongWritable, FloatWritable> {

    @Override
    public TextVertexReader createVertexReader(InputSplit split, TaskAttemptContext context) throws IOException {
        return new TextVertexReaderFromEachLine() {
            String[] items;

            @Override
            protected VLongWritable getId(Text line) throws IOException {
                String[] kv = line.toString().split("\t");
                items = kv[1].split(" ");
                return new VLongWritable(Long.parseLong(kv[0]));
            }

            @Override
            protected VLongWritable getValue(Text line) throws IOException {
                return null;
            }

            @Override
            protected Iterable<Edge<VLongWritable, FloatWritable>> getEdges(Text line) throws IOException {
                List<Edge<VLongWritable, FloatWritable>> edges = new ArrayList<Edge<VLongWritable, FloatWritable>>();
                Map<VLongWritable, FloatWritable> edgeMap = new HashMap<VLongWritable, FloatWritable>();
                for (int i = 1; i < items.length; i++) {
                    edgeMap.put(new VLongWritable(Long.parseLong(items[i])), null);
                }
                for (Entry<VLongWritable, FloatWritable> entry : edgeMap.entrySet()) {
                    MapMutableEdge<VLongWritable, FloatWritable> edge = new MapMutableEdge<VLongWritable, FloatWritable>();
                    edge.setEntry(entry);
                    edge.setValue(null);
                    edges.add(edge);
                }
                return edges;
            }

        };
    }

}
