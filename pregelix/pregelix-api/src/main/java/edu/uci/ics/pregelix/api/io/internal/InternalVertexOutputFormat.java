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
package edu.uci.ics.pregelix.api.io.internal;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

import edu.uci.ics.pregelix.api.graph.Vertex;
import edu.uci.ics.pregelix.api.io.VertexOutputFormat;
import edu.uci.ics.pregelix.api.io.VertexWriter;

/**
 * @author yingyib
 */
@SuppressWarnings("rawtypes")
public class InternalVertexOutputFormat<I extends WritableComparable, V extends Writable, E extends Writable> extends
        VertexOutputFormat<I, V, E> {
    private SequenceFileOutputFormat sequenceOutputFormat = new SequenceFileOutputFormat();

    @Override
    public VertexWriter<I, V, E> createVertexWriter(final TaskAttemptContext context) throws IOException,
            InterruptedException {
        return new VertexWriter<I, V, E>() {
            private RecordWriter recordWriter = sequenceOutputFormat.getRecordWriter(context);
            private NullWritable key = NullWritable.get();

            @Override
            public void initialize(TaskAttemptContext context) throws IOException, InterruptedException {

            }

            @SuppressWarnings("unchecked")
            @Override
            public void writeVertex(Vertex<I, V, E, ?> vertex) throws IOException, InterruptedException {
                recordWriter.write(key, vertex);
            }

            @Override
            public void close(TaskAttemptContext context) throws IOException, InterruptedException {
                recordWriter.close(context);
            }

        };
    }

    @Override
    public void checkOutputSpecs(JobContext context) throws IOException, InterruptedException {

    }

    @Override
    public OutputCommitter getOutputCommitter(TaskAttemptContext context) throws IOException, InterruptedException {
        return new OutputCommitter() {

            @Override
            public void abortTask(TaskAttemptContext arg0) throws IOException {
                // TODO Auto-generated method stub

            }

            @Override
            public void cleanupJob(JobContext arg0) throws IOException {
                // TODO Auto-generated method stub

            }

            @Override
            public void commitTask(TaskAttemptContext arg0) throws IOException {
                // TODO Auto-generated method stub

            }

            @Override
            public boolean needsTaskCommit(TaskAttemptContext arg0) throws IOException {
                return false;
            }

            @Override
            public void setupJob(JobContext arg0) throws IOException {

            }

            @Override
            public void setupTask(TaskAttemptContext arg0) throws IOException {

            }

        };
    }

}
