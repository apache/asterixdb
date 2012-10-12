/*
 * Copyright 2009-2010 by The Regents of the University of California
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

package edu.uci.ics.pregelix.api.graph;

import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

/**
 * interface to implement for combining of messages sent to the same vertex.
 * 
 * @param <I extends Writable> index
 * @param <M extends Writable> message data
 */
@SuppressWarnings("rawtypes")
public interface VertexCombiner<I extends WritableComparable, M extends Writable> {

    /**
     * initialize combiner
     */
    public void init();

    /**
     * step call
     * 
     * @param vertexIndex
     * @param msg
     * @throws IOException
     */
    public void step(I vertexIndex, M msg) throws IOException;

    /**
     * finish aggregate
     * 
     * @return Message
     */
    public M finish();
}
