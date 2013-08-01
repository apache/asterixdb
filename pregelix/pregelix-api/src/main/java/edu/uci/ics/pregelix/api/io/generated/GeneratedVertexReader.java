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

package edu.uci.ics.pregelix.api.io.generated;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import edu.uci.ics.pregelix.api.io.BasicGenInputSplit;
import edu.uci.ics.pregelix.api.io.VertexReader;
import edu.uci.ics.pregelix.api.io.WritableSizable;

/**
 * Used by GeneratedVertexInputFormat to read some generated data
 * 
 * @param <I>
 *            Vertex index value
 * @param <V>
 *            Vertex value
 * @param <E>
 *            Edge value
 */
@SuppressWarnings("rawtypes")
public abstract class GeneratedVertexReader<I extends WritableComparable, V extends Writable, E extends Writable, M extends WritableSizable>
        implements VertexReader<I, V, E, M> {
    /** Records read so far */
    protected long recordsRead = 0;
    /** Total records to read (on this split alone) */
    protected long totalRecords = 0;
    /** The input split from initialize(). */
    protected BasicGenInputSplit inputSplit = null;
    /** Reverse the id order? */
    protected boolean reverseIdOrder;

    protected Configuration configuration = null;

    public static final String READER_VERTICES = "GeneratedVertexReader.reader_vertices";
    public static final long DEFAULT_READER_VERTICES = 10;
    public static final String REVERSE_ID_ORDER = "GeneratedVertexReader.reverseIdOrder";
    public static final boolean DEAFULT_REVERSE_ID_ORDER = false;

    public GeneratedVertexReader() {
    }

    @Override
    final public void initialize(InputSplit inputSplit, TaskAttemptContext context) throws IOException {
        configuration = context.getConfiguration();
        totalRecords = configuration.getLong(GeneratedVertexReader.READER_VERTICES,
                GeneratedVertexReader.DEFAULT_READER_VERTICES);
        reverseIdOrder = configuration.getBoolean(GeneratedVertexReader.REVERSE_ID_ORDER,
                GeneratedVertexReader.DEAFULT_REVERSE_ID_ORDER);
        this.inputSplit = (BasicGenInputSplit) inputSplit;
    }

    @Override
    public void close() throws IOException {
    }

    @Override
    final public float getProgress() throws IOException {
        return recordsRead * 100.0f / totalRecords;
    }
}
