/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.asterix.runtime.operators.kmeans;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import org.apache.asterix.runtime.utils.VectorDistanceCalculation;
import org.apache.hyracks.api.comm.VSizeFrame;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.job.JobId;
import org.apache.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import org.apache.hyracks.dataflow.common.comm.io.FrameTupleAppender;
import org.apache.hyracks.dataflow.std.misc.MaterializerTaskState;
import org.apache.hyracks.test.support.TestUtils;
import org.junit.Assert;
import org.junit.Test;

/**
 * Pins {@link KMeansLoopIO#streamScoredAgainstPool}, the bounded scan that replaced holding the candidate pool
 * in the heap. The scan exists to make memory independent of {@code k}; these tests exist to make sure it did
 * not buy that with a change in results.
 * <p>
 * The property that matters is not "close enough" but bit-identical, at every block size. Downstream, the
 * per-vector minima are summed into the round's local sigma and the argmin indexes the weigh accumulators, and
 * floating-point addition is not associative -- if the blocking boundary reordered anything, sigma would drift
 * with the frame budget and the same query would answer differently on differently-configured clusters.
 */
public class KMeansBoundedScanTest {

    private static final int FRAME_SIZE = 32768;

    /** One scored vector, in the order the scan produced it. */
    private static final class Scored {
        private final double[] vec;
        private final double nearest;
        private final int index;

        private Scored(double[] vec, double nearest, int index) {
            this.vec = vec;
            this.nearest = nearest;
            this.index = index;
        }
    }

    @Test
    public void scanIsIdenticalAcrossBlockSizes() throws Exception {
        double[][] vectors = vectors(500, 16, 7L);
        double[][] pool = vectors(64, 16, 99L);
        assertSpansManyBlocks(1, 16, vectors.length);

        // 1 frame forces many short blocks and therefore many passes over the pool; 4096 holds everything in
        // one block, which is the shape the resident form had.
        List<Scored> tiny = scan(vectors, pool, 1);
        List<Scored> huge = scan(vectors, pool, 4096);

        Assert.assertEquals(vectors.length, tiny.size());
        assertSameSequence(tiny, huge);
    }

    @Test
    public void scanMatchesTheResidentForm() throws Exception {
        double[][] vectors = vectors(1200, 8, 3L);
        double[][] pool = vectors(40, 8, 11L);
        assertSpansManyBlocks(1, 8, vectors.length);

        List<Scored> blocked = scan(vectors, pool, 1);

        // Reference form: pool resident, vectors streamed, first-wins on ties.
        for (int v = 0; v < vectors.length; v++) {
            double best = Double.POSITIVE_INFINITY;
            int bestIdx = -1;
            for (int c = 0; c < pool.length; c++) {
                double d = VectorDistanceCalculation.euclideanSquared(vectors[v], pool[c]);
                if (d < best) {
                    best = d;
                    bestIdx = c;
                }
            }
            Assert.assertEquals("nearest distance at " + v, best, blocked.get(v).nearest, 0.0d);
            Assert.assertEquals("nearest index at " + v, bestIdx, blocked.get(v).index);
            Assert.assertArrayEquals("vector order at " + v, vectors[v], blocked.get(v).vec, 0.0d);
        }
    }

    /**
     * The sum the scan feeds into sigma must not shift with the budget. Asserted as an exact {@code long} bit
     * comparison rather than a delta, because a tolerance would hide exactly the reordering this guards.
     */
    @Test
    public void sigmaSumIsBitIdenticalAcrossBlockSizes() throws Exception {
        double[][] vectors = vectors(777, 12, 21L);
        double[][] pool = vectors(50, 12, 5L);
        assertSpansManyBlocks(1, 12, vectors.length);

        double tiny = 0.0d;
        for (Scored s : scan(vectors, pool, 1)) {
            tiny += s.nearest;
        }
        double huge = 0.0d;
        for (Scored s : scan(vectors, pool, 4096)) {
            huge += s.nearest;
        }
        Assert.assertEquals(Double.doubleToRawLongBits(huge), Double.doubleToRawLongBits(tiny));
    }

    /** Ties must resolve to the first pool member however the block boundary falls. */
    @Test
    public void tiesResolveToTheFirstPoolMember() throws Exception {
        double[][] vectors = new double[2000][];
        for (int i = 0; i < vectors.length; i++) {
            vectors[i] = new double[] { 0.0d, 0.0d };
        }
        // Four candidates all exactly the same distance from every vector.
        double[][] pool = { { 1.0d, 0.0d }, { -1.0d, 0.0d }, { 0.0d, 1.0d }, { 0.0d, -1.0d } };
        assertSpansManyBlocks(1, 2, vectors.length);

        for (int framesLimit : new int[] { 1, 2, 4096 }) {
            for (Scored s : scan(vectors, pool, framesLimit)) {
                Assert.assertEquals("framesLimit=" + framesLimit, 0, s.index);
            }
        }
    }

    /** An empty pool leaves every vector unassigned rather than failing, as the resident form did. */
    @Test
    public void emptyPoolLeavesEveryVectorUnassigned() throws Exception {
        List<Scored> scored = scan(vectors(20, 4, 1L), new double[0][], 1);
        Assert.assertEquals(20, scored.size());
        for (Scored s : scored) {
            Assert.assertEquals(-1, s.index);
            Assert.assertEquals(Double.POSITIVE_INFINITY, s.nearest, 0.0d);
        }
    }

    /** The capacity is at least one vector, so even a width that overruns the budget still makes progress. */
    @Test
    public void blockCapacityIsAtLeastOne() {
        IHyracksTaskContext ctx = TestUtils.create(FRAME_SIZE);
        Assert.assertTrue(KMeansLoopIO.blockCapacity(ctx, 1, 1_000_000) >= 1);
        Assert.assertTrue(KMeansLoopIO.blockCapacity(ctx, 1, 16) > 1);
    }

    /**
     * Guards the tests above against becoming vacuous. Each compares results across block sizes, which proves
     * nothing if the small budget happens to hold everything in one block -- so assert the fixture really is
     * large enough to be split, and fail loudly if a change to the capacity formula ever makes it not.
     */
    private static void assertSpansManyBlocks(int framesLimit, int dim, int count) {
        int capacity = KMeansLoopIO.blockCapacity(TestUtils.create(FRAME_SIZE), framesLimit, dim);
        Assert.assertTrue("fixture no longer spans multiple blocks: capacity=" + capacity + " count=" + count,
                capacity < count);
    }

    private static void assertSameSequence(List<Scored> a, List<Scored> b) {
        Assert.assertEquals(a.size(), b.size());
        for (int i = 0; i < a.size(); i++) {
            Assert.assertArrayEquals("vector " + i, a.get(i).vec, b.get(i).vec, 0.0d);
            Assert.assertEquals("nearest " + i, Double.doubleToRawLongBits(a.get(i).nearest),
                    Double.doubleToRawLongBits(b.get(i).nearest));
            Assert.assertEquals("index " + i, a.get(i).index, b.get(i).index);
        }
    }

    /** Runs the bounded scan at {@code framesLimit} and flattens the blocks back into per-vector order. */
    private static List<Scored> scan(double[][] vectors, double[][] pool, int framesLimit) throws Exception {
        IHyracksTaskContext ctx = TestUtils.create(FRAME_SIZE);
        MaterializerTaskState vectorState = materialize(ctx, vectors, 1);
        MaterializerTaskState poolState = materialize(ctx, pool, 2);
        List<Scored> out = new ArrayList<>();
        try {
            KMeansLoopIO.streamScoredAgainstPool(vectorState, poolState, ctx, framesLimit,
                    (vecs, n, nearest, nearestIdx) -> {
                        for (int i = 0; i < n; i++) {
                            out.add(new Scored(vecs[i].clone(), nearest[i], nearestIdx[i]));
                        }
                    });
        } finally {
            vectorState.close();
            poolState.close();
        }
        return out;
    }

    /** Writes vectors into a run file in {@link KMeansLoopIO#POOL_RD} layout, batched so no frame overruns. */
    private static MaterializerTaskState materialize(IHyracksTaskContext ctx, double[][] vectors, int taskId)
            throws HyracksDataException {
        MaterializerTaskState state = new MaterializerTaskState(new JobId(1),
                new org.apache.hyracks.api.dataflow.TaskId(new org.apache.hyracks.api.dataflow.ActivityId(
                        new org.apache.hyracks.api.dataflow.OperatorDescriptorId(taskId), 0), 0));
        state.open(ctx);
        VSizeFrame frame = new VSizeFrame(ctx);
        FrameTupleAppender appender = new FrameTupleAppender(frame);
        ArrayTupleBuilder tb = new ArrayTupleBuilder(1);
        for (double[] vec : vectors) {
            tb.reset();
            KMeansLoopIO.writeRawVector(tb, vec);
            if (!appender.append(tb.getFieldEndOffsets(), tb.getByteArray(), 0, tb.getSize())) {
                flush(state, appender, frame);
                Assert.assertTrue(appender.append(tb.getFieldEndOffsets(), tb.getByteArray(), 0, tb.getSize()));
            }
        }
        if (appender.getTupleCount() > 0) {
            flush(state, appender, frame);
        }
        return state;
    }

    private static void flush(MaterializerTaskState state, FrameTupleAppender appender, VSizeFrame frame)
            throws HyracksDataException {
        ByteBuffer buffer = frame.getBuffer();
        buffer.position(0);
        buffer.limit(buffer.capacity());
        state.appendFrame(buffer);
        appender.reset(frame, true);
    }

    /** Deterministic pseudo-random vectors — fixed seed, so a failure is reproducible. */
    private static double[][] vectors(int count, int dim, long seed) {
        java.util.Random rng = new java.util.Random(seed);
        double[][] out = new double[count][dim];
        for (int i = 0; i < count; i++) {
            for (int d = 0; d < dim; d++) {
                out[i][d] = rng.nextDouble() * 10.0d;
            }
        }
        return out;
    }
}
