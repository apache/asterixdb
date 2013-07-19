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
package edu.uci.ics.pregelix.example.trianglecounting;

import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.pregelix.api.graph.GlobalAggregator;
import edu.uci.ics.pregelix.api.graph.Vertex;
import edu.uci.ics.pregelix.example.io.VLongWritable;

/**
 * The global aggregator aggregates the count of triangles
 */
public class TriangleCountingAggregator extends
        GlobalAggregator<VLongWritable, VLongWritable, VLongWritable, VLongWritable, VLongWritable, VLongWritable> {

    private VLongWritable state = new VLongWritable(0);

    @Override
    public void init() {
        state.set(0);
    }

    @Override
    public void step(Vertex<VLongWritable, VLongWritable, VLongWritable, VLongWritable> v) throws HyracksDataException {
        state.set(state.get() + v.getVertexValue().get());
    }

    @Override
    public void step(VLongWritable partialResult) {
        state.set(state.get() + partialResult.get());
    }

    @Override
    public VLongWritable finishPartial() {
        return state;
    }

    @Override
    public VLongWritable finishFinal() {
        return state;
    }

}
