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
package edu.uci.ics.pregelix.api.util;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.pregelix.api.graph.GlobalAggregator;
import edu.uci.ics.pregelix.api.graph.Vertex;
import edu.uci.ics.pregelix.api.io.WritableSizable;

@SuppressWarnings("rawtypes")
public class GlobalCountAggregator<I extends WritableComparable, V extends Writable, E extends Writable, M extends WritableSizable>
        extends GlobalAggregator<I, V, E, M, LongWritable, LongWritable> {

    private LongWritable state = new LongWritable(0);

    @Override
    public void init() {
        state.set(0);
    }

    @Override
    public void step(Vertex<I, V, E, M> v) throws HyracksDataException {
        state.set(state.get() + 1);
    }

    @Override
    public void step(LongWritable partialResult) {
        state.set(state.get() + partialResult.get());
    }

    @Override
    public LongWritable finishPartial() {
        return state;
    }

    @Override
    public LongWritable finishFinal() {
        return state;
    }

}
