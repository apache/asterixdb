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
import java.util.List;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;

import edu.uci.ics.pregelix.api.graph.Vertex;
import edu.uci.ics.pregelix.api.io.VertexInputFormat;
import edu.uci.ics.pregelix.api.io.VertexReader;
import edu.uci.ics.pregelix.api.io.WritableSizable;

/**
 * @author yingyib
 */
@SuppressWarnings("rawtypes")
public class InternalVertexInputFormat<I extends WritableComparable, V extends Writable, E extends Writable, M extends WritableSizable>
        extends VertexInputFormat<I, V, E, M> {
    /** Uses the SequenceFileInputFormat to do everything */
    private SequenceFileInputFormat sequenceInputFormat = new SequenceFileInputFormat();

    @SuppressWarnings("unchecked")
    @Override
    public List<InputSplit> getSplits(JobContext context, int numWorkers) throws IOException, InterruptedException {
        return sequenceInputFormat.getSplits(context);
    }

    @Override
    public VertexReader<I, V, E, M> createVertexReader(final InputSplit split, final TaskAttemptContext context)
            throws IOException {
        return new VertexReader<I, V, E, M>() {
            RecordReader recordReader = sequenceInputFormat.createRecordReader(split, context);

            @Override
            public void initialize(InputSplit inputSplit, TaskAttemptContext context) throws IOException,
                    InterruptedException {
                recordReader.initialize(inputSplit, context);
            }

            @Override
            public boolean nextVertex() throws IOException, InterruptedException {
                return recordReader.nextKeyValue();
            }

            @SuppressWarnings("unchecked")
            @Override
            public Vertex<I, V, E, M> getCurrentVertex() throws IOException, InterruptedException {
                return (Vertex<I, V, E, M>) recordReader.getCurrentValue();
            }

            @Override
            public void close() throws IOException {
                recordReader.close();
            }

            @Override
            public float getProgress() throws IOException, InterruptedException {
                return 0;
            }

        };
    }

}
