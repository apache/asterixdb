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

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.apache.hyracks.api.comm.VSizeFrame;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.ActivityId;
import org.apache.hyracks.api.dataflow.OperatorDescriptorId;
import org.apache.hyracks.api.dataflow.TaskId;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.job.JobId;
import org.apache.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import org.apache.hyracks.dataflow.common.comm.io.FrameTupleAppender;
import org.apache.hyracks.dataflow.std.misc.MaterializerTaskState;
import org.apache.hyracks.test.support.TestUtils;
import org.junit.Assert;
import org.junit.Test;

/**
 * Pins {@link KMeansLoopIO#accumulateInWindows}, which reduces vectors onto the weigh accumulators while
 * holding only a window of slots instead of one per candidate.
 * <p>
 * The window is a memory knob and not a semantic one, so every test here compares a narrow window against one
 * wide enough to hold every slot at once. The sums are compared as raw bits: they are means-in-waiting, float
 * addition is not associative, and a tolerance would hide precisely the reordering windowing could introduce.
 */
public class KMeansWindowedAccumulateTest {

    private static final int FRAME_SIZE = 32768;

    /** One emitted slot. */
    private static final class Slot {
        private final int index;
        private final long count;
        private final double[] sum;

        private Slot(int index, long count, double[] sum) {
            this.index = index;
            this.count = count;
            this.sum = sum;
        }
    }

    @Test
    public void narrowWindowsMatchTheSinglePass() throws Exception {
        int slots = 500;
        double[][] vectors = vectors(4000, 6, 12L);
        int[] assignment = assignment(vectors.length, slots, 77L);

        List<Slot> single = accumulate(vectors, assignment, slots, slots);
        Assert.assertFalse(single.isEmpty());
        for (int window : new int[] { 1, 2, 7, 64, 499, 501, 100000 }) {
            assertSame("window=" + window, single, accumulate(vectors, assignment, slots, window));
        }
    }

    /** Unassigned vectors (index -1, e.g. an all-NaN distance row) belong to no window and must be dropped. */
    @Test
    public void unassignedVectorsAreDroppedInEveryWindow() throws Exception {
        int slots = 40;
        double[][] vectors = vectors(600, 4, 8L);
        int[] assignment = assignment(vectors.length, slots, 5L);
        for (int i = 0; i < assignment.length; i += 3) {
            assignment[i] = -1;
        }

        List<Slot> single = accumulate(vectors, assignment, slots, slots);
        long counted = 0;
        for (Slot s : single) {
            counted += s.count;
        }
        Assert.assertEquals(vectors.length - (vectors.length + 2) / 3, counted);
        assertSame("window=3", single, accumulate(vectors, assignment, slots, 3));
    }

    /** A NaN distance is not accumulated even when its index points at a real slot. */
    @Test
    public void nanDistancesAreSkipped() throws Exception {
        int slots = 10;
        double[][] vectors = vectors(200, 3, 2L);
        int[] assignment = assignment(vectors.length, slots, 9L);

        List<Slot> withNaN = accumulate(vectors, assignment, slots, slots, 0.5d);
        long counted = 0;
        for (Slot s : withNaN) {
            counted += s.count;
        }
        Assert.assertTrue("some rows should have been skipped", counted < vectors.length);
        assertSame("window=2", withNaN, accumulate(vectors, assignment, slots, 2, 0.5d));
    }

    /** Slots nothing landed on are not emitted, at any window. */
    @Test
    public void emptySlotsAreNotEmitted() throws Exception {
        int slots = 100;
        double[][] vectors = vectors(50, 5, 31L);
        int[] assignment = new int[vectors.length];
        for (int i = 0; i < assignment.length; i++) {
            assignment[i] = i % 4; // only slots 0..3 are ever hit
        }
        List<Slot> single = accumulate(vectors, assignment, slots, slots);
        Assert.assertEquals(4, single.size());
        assertSame("window=1", single, accumulate(vectors, assignment, slots, 1));
    }

    /** No slots means no work and no output, rather than an attempted read of an empty column. */
    @Test
    public void zeroSlotsEmitsNothing() throws Exception {
        Assert.assertTrue(accumulate(vectors(10, 2, 1L), new int[10], 0, 8).isEmpty());
    }

    private static void assertSame(String label, List<Slot> expected, List<Slot> actual) {
        Assert.assertEquals(label + ": slot count", expected.size(), actual.size());
        for (int i = 0; i < expected.size(); i++) {
            Slot e = expected.get(i);
            Slot a = actual.get(i);
            Assert.assertEquals(label + ": index at " + i, e.index, a.index);
            Assert.assertEquals(label + ": count at " + i, e.count, a.count);
            Assert.assertEquals(label + ": sum width at " + i, e.sum.length, a.sum.length);
            for (int d = 0; d < e.sum.length; d++) {
                Assert.assertEquals(label + ": sum[" + d + "] at slot " + e.index, Double.doubleToRawLongBits(e.sum[d]),
                        Double.doubleToRawLongBits(a.sum[d]));
            }
        }
    }

    private static List<Slot> accumulate(double[][] vectors, int[] assignment, int slots, int window) throws Exception {
        return accumulate(vectors, assignment, slots, window, 0.0d);
    }

    /** Materialises the vectors and a matching score column, then reduces them at the given window. */
    private static List<Slot> accumulate(double[][] vectors, int[] assignment, int slots, int window,
            double nanFraction) throws Exception {
        IHyracksTaskContext ctx = TestUtils.create(FRAME_SIZE);
        MaterializerTaskState vectorState = newState(ctx, 1);
        MaterializerTaskState scoreState = newState(ctx, 2);
        List<Slot> out = new ArrayList<>();
        try {
            writeVectors(ctx, vectorState, vectors);
            writeColumn(ctx, scoreState, assignment, nanFraction);
            KMeansLoopIO.accumulateInWindows(vectorState, scoreState, ctx, slots, window,
                    (index, count, sum) -> out.add(new Slot(index, count, sum.clone())));
        } finally {
            vectorState.close();
            vectorState.deleteFile();
            scoreState.close();
            scoreState.deleteFile();
        }
        return out;
    }

    private static void writeColumn(IHyracksTaskContext ctx, MaterializerTaskState state, int[] assignment,
            double nanFraction) throws HyracksDataException {
        KMeansLoopIO.ScoreColumnWriter writer = new KMeansLoopIO.ScoreColumnWriter(state, ctx);
        Random rng = new Random(1234L);
        double[] nearest = new double[assignment.length];
        for (int i = 0; i < assignment.length; i++) {
            nearest[i] = nanFraction > 0.0d && rng.nextDouble() < nanFraction ? Double.NaN : rng.nextDouble();
        }
        writer.append(nearest, assignment, assignment.length);
        writer.finish();
    }

    private static void writeVectors(IHyracksTaskContext ctx, MaterializerTaskState state, double[][] vectors)
            throws HyracksDataException {
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
    }

    private static void flush(MaterializerTaskState state, FrameTupleAppender appender, VSizeFrame frame)
            throws HyracksDataException {
        frame.getBuffer().position(0);
        frame.getBuffer().limit(frame.getBuffer().capacity());
        state.appendFrame(frame.getBuffer());
        appender.reset(frame, true);
    }

    private static MaterializerTaskState newState(IHyracksTaskContext ctx, int id) throws HyracksDataException {
        MaterializerTaskState state =
                new MaterializerTaskState(new JobId(1), new TaskId(new ActivityId(new OperatorDescriptorId(id), 0), 0));
        state.open(ctx);
        return state;
    }

    private static int[] assignment(int count, int slots, long seed) {
        Random rng = new Random(seed);
        int[] out = new int[count];
        for (int i = 0; i < count; i++) {
            out[i] = rng.nextInt(slots);
        }
        return out;
    }

    private static double[][] vectors(int count, int dim, long seed) {
        Random rng = new Random(seed);
        double[][] out = new double[count][dim];
        for (int i = 0; i < count; i++) {
            for (int d = 0; d < dim; d++) {
                out[i][d] = rng.nextDouble() * 10.0d;
            }
        }
        return out;
    }
}
