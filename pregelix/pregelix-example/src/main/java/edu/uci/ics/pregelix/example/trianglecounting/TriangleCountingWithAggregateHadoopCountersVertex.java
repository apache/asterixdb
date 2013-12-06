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

import org.apache.hadoop.mapreduce.Counters;

import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.pregelix.api.graph.Vertex;
import edu.uci.ics.pregelix.api.job.PregelixJob;
import edu.uci.ics.pregelix.api.util.BspUtils;
import edu.uci.ics.pregelix.api.util.HadoopCountersAggregator;
import edu.uci.ics.pregelix.example.client.Client;
import edu.uci.ics.pregelix.example.data.VLongNormalizedKeyComputer;
import edu.uci.ics.pregelix.example.io.VLongWritable;

/**
 * The triangle counting example -- counting the triangles in an undirected graph.
 */
public class TriangleCountingWithAggregateHadoopCountersVertex extends TriangleCountingVertex {

    public static void main(String[] args) throws Exception {
        PregelixJob job = new PregelixJob(TriangleCountingWithAggregateHadoopCountersVertex.class.getSimpleName());
        job.setVertexClass(TriangleCountingWithAggregateHadoopCountersVertex.class);
        job.addGlobalAggregatorClass(TriangleCountingAggregator.class);
        job.setCounterAggregatorClass(TriangleHadoopCountersAggregator.class);
        job.setVertexInputFormatClass(TextTriangleCountingInputFormat.class);
        job.setVertexOutputFormatClass(TriangleCountingVertexOutputFormat.class);
        job.setNoramlizedKeyComputerClass(VLongNormalizedKeyComputer.class);
        Client.run(args, job);
        System.out.println("triangle count in last iteration: " + readTriangleCountingResult(job.getConfiguration()));
        System.out.println("aggregate counter (including all iterations):\n" + BspUtils.getCounters(job));
    }

    public static class TriangleHadoopCountersAggregator extends
            HadoopCountersAggregator<VLongWritable, VLongWritable, VLongWritable, VLongWritable, Counters> {
        private Counters counters;

        @Override
        public void init() {
            counters = new Counters();
        }

        @Override
        public void step(Vertex<VLongWritable, VLongWritable, VLongWritable, VLongWritable> v)
                throws HyracksDataException {
            counters.findCounter("TriangleCounting", "total-ids").increment(1);
            counters.findCounter("TriangleCounting", "total-values").increment(v.getVertexValue().get());
        }

        @Override
        public void step(Counters partialResult) {
            counters.incrAllCounters(partialResult);
        }

        @Override
        public Counters finishPartial() {
            return counters;
        }

        @Override
        public Counters finishFinal() {
            return counters;
        }
    }
}