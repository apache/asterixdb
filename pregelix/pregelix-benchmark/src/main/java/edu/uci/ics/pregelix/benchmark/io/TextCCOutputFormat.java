package edu.uci.ics.pregelix.benchmark.io;

import java.io.IOException;

import org.apache.giraph.graph.Vertex;
import org.apache.giraph.io.formats.TextVertexOutputFormat;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.VLongWritable;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

public class TextCCOutputFormat extends TextVertexOutputFormat<VLongWritable, VLongWritable, FloatWritable> {

    @Override
    public TextVertexWriter createVertexWriter(TaskAttemptContext context) throws IOException, InterruptedException {
        return new TextVertexWriterToEachLine() {

            @Override
            protected Text convertVertexToLine(Vertex<VLongWritable, VLongWritable, FloatWritable, ?> vertex)
                    throws IOException {
                return new Text(vertex.getId() + " " + vertex.getValue());
            }

        };
    }

}
