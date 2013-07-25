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

package edu.uci.ics.pregelix.api.io;

import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import edu.uci.ics.pregelix.api.graph.Vertex;

/**
 * Analogous to {@link RecordReader} for vertices. Will read the vertices from
 * an input split.
 * 
 * @param <I>
 *            Vertex id
 * @param <V>
 *            Vertex data
 * @param <E>
 *            Edge data
 * @param <M>
 *            Message data
 */
@SuppressWarnings("rawtypes")
public interface VertexReader<I extends WritableComparable, V extends Writable, E extends Writable, M extends WritableSizable> {
    /**
     * Use the input split and context t o setup reading the vertices.
     * Guaranteed to be called prior to any other function.
     * 
     * @param inputSplit
     *            Input split to be used for reading vertices.
     * @param context
     *            Context from the task.
     * @throws IOException
     * @throws InterruptedException
     */
    void initialize(InputSplit inputSplit, TaskAttemptContext context) throws IOException, InterruptedException;

    /**
     * @return false iff there are no more vertices
     * @throws IOException
     * @throws InterruptedException
     */
    boolean nextVertex() throws IOException, InterruptedException;

    /**
     * Get the current vertex.
     * 
     * @return the current vertex which has been read. nextVertex() should be
     *         called first.
     * @throws IOException
     * @throws InterruptedException
     */
    Vertex<I, V, E, M> getCurrentVertex() throws IOException, InterruptedException;

    /**
     * Close this {@link VertexReader} to future operations.
     * 
     * @throws IOException
     */
    void close() throws IOException;

    /**
     * How much of the input has the {@link VertexReader} consumed i.e. has been
     * processed by?
     * 
     * @return Progress from <code>0.0</code> to <code>1.0</code>.
     * @throws IOException
     * @throws InterruptedException
     */
    float getProgress() throws IOException, InterruptedException;
}
