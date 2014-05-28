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

import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Counters;

import edu.uci.ics.pregelix.api.graph.GlobalAggregator;
import edu.uci.ics.pregelix.api.io.WritableSizable;

/**
 * A global aggregator that produces a Hadoop mapreduce.Counters object
 */
@SuppressWarnings("rawtypes")
public abstract class HadoopCountersAggregator<I extends WritableComparable, V extends Writable, E extends Writable, M extends WritableSizable, P extends Writable>
        extends GlobalAggregator<I, V, E, M, Counters, Counters> {
    private ResettableCounters counters = new ResettableCounters();

    public Counters getCounters() {
        return counters;
    }

    @Override
    public void init() {
        counters.reset();
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

    /**
     * mapreduce.Counters object that is resettable via .reset()
     */
    public static class ResettableCounters extends Counters {
        private static final DataInputStream zeroStream = new DataInputStream(new InputStream() {
            @Override
            public int read() throws IOException {
                return 0;
            }
        });

        /**
         * Reset this Counters object
         * 
         * The reset is done by simulating a readFields() from a stream of 0's,
         * indicating a serialized length of 0 groups. The Counters' cache is not changed. 
         */
        public void reset() {
            try {
                this.readFields(zeroStream);
            } catch (IOException e) {
                throw new RuntimeException("Unexpected failure when trying to reset Counters object!", e);
            }
        }
    }
}
