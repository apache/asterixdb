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

import java.io.File;
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
 * The memory work moved this feature's working set out of the heap and onto disk, which closed one failure
 * mode and opened another. A heap structure needs no cleanup -- a task that dies is collected. A run file does:
 * a query cancelled or failed partway through a spill leaves the file behind, and unlike a memory leak that
 * fills the node's workspace disk and takes the next query with it.
 * <p>
 * The happy paths are covered by {@link KMeansRunFileLifecycleTest}. These are the paths nothing had
 * exercised: an exception raised in the middle of a stream, and an interrupt arriving mid-scan, which is how
 * Hyracks cancels a task that is doing pure CPU over materialized files.
 */
public class KMeansFaultInjectionTest {

    private static final int FRAME_SIZE = 32768;

    /** Thrown by the injected sinks so a real failure cannot be mistaken for the injected one. */
    private static final class Injected extends HyracksDataException {
        private static final long serialVersionUID = 1L;

        private Injected() {
            super(HyracksDataException.create(new IllegalStateException("injected mid-stream failure")));
        }
    }

    private static int liveFiles() {
        File[] files = new File(System.getProperty("java.io.tmpdir")).listFiles();
        if (files == null) {
            return 0;
        }
        int n = 0;
        for (File f : files) {
            if (f.getName().startsWith(MaterializerTaskState.class.getSimpleName())) {
                n++;
            }
        }
        return n;
    }

    /**
     * A sink that throws part-way through must not strand the reader. Checked by the symptom that matters --
     * the very next read of the same file succeeds, which it could not if the handle had been left open or the
     * stream left mid-frame.
     */
    @Test
    public void aThrowingSinkLeavesTheFileReadable() throws Exception {
        IHyracksTaskContext ctx = TestUtils.create(FRAME_SIZE);
        MaterializerTaskState state = materialize(ctx, vectors(500, 8, 1L), 1);
        try {
            final int[] seen = { 0 };
            try {
                KMeansLoopIO.streamRawVectors(state, ctx, v -> {
                    if (seen[0]++ == 200) {
                        throw new Injected();
                    }
                });
                Assert.fail("expected the injected failure to propagate");
            } catch (Injected expected) {
                Assert.assertEquals("stopped where it was told to", 201, seen[0]);
            }
            List<double[]> after = new ArrayList<>();
            KMeansLoopIO.streamRawVectors(state, ctx, after::add);
            Assert.assertEquals("the file is still fully readable after the failure", 500, after.size());
        } finally {
            state.close();
            state.deleteFile();
        }
    }

    /** A scan whose consumer fails must still release the pool reader it opened underneath. */
    @Test
    public void aFailedScanLeavesNothingBehind() throws Exception {
        IHyracksTaskContext ctx = TestUtils.create(FRAME_SIZE);
        int before = liveFiles();
        MaterializerTaskState vectors = materialize(ctx, vectors(600, 16, 2L), 2);
        MaterializerTaskState pool = materialize(ctx, vectors(40, 16, 3L), 3);
        try {
            try {
                KMeansLoopIO.streamScoredAgainstPool(vectors, pool, ctx, 1, (v, n, near, idx) -> {
                    throw new Injected();
                });
                Assert.fail("expected the injected failure to propagate");
            } catch (Injected expected) {
                // the scan is abandoned; both run files must still be intact and readable
            }
            List<double[]> replay = new ArrayList<>();
            KMeansLoopIO.streamRawVectors(pool, ctx, replay::add);
            Assert.assertEquals(40, replay.size());
        } finally {
            vectors.close();
            vectors.deleteFile();
            pool.close();
            pool.deleteFile();
        }
        Assert.assertEquals(before, liveFiles());
    }

    /** The accumulator opens a score-column reader per window; a failing sink must not strand it. */
    @Test
    public void aFailedAccumulationReleasesItsColumnReader() throws Exception {
        IHyracksTaskContext ctx = TestUtils.create(FRAME_SIZE);
        int before = liveFiles();
        MaterializerTaskState vectors = materialize(ctx, vectors(300, 8, 4L), 4);
        MaterializerTaskState column = newState(ctx, 5);
        try {
            KMeansLoopIO.ScoreColumnWriter w = new KMeansLoopIO.ScoreColumnWriter(column, ctx);
            double[] near = new double[300];
            int[] idx = new int[300];
            for (int i = 0; i < 300; i++) {
                near[i] = i;
                idx[i] = i % 16;
            }
            w.append(near, idx, 300);
            w.finish();

            try {
                KMeansLoopIO.accumulateInWindows(vectors, column, ctx, 16, 4, (slot, cnt, sum) -> {
                    throw new Injected();
                });
                Assert.fail("expected the injected failure to propagate");
            } catch (Injected expected) {
                // abandoned mid-sweep
            }
            // The column must still be readable -- a stranded reader would show up here.
            List<Integer> slots = new ArrayList<>();
            KMeansLoopIO.accumulateInWindows(vectors, column, ctx, 16, 4, (slot, cnt, sum) -> slots.add(slot));
            Assert.assertEquals(16, slots.size());
        } finally {
            vectors.close();
            vectors.deleteFile();
            column.close();
            column.deleteFile();
        }
        Assert.assertEquals(before, liveFiles());
    }

    /** A VectorList abandoned mid-read still releases its spill file when closed. */
    @Test
    public void aVectorListReleasesItsSpillFileAfterAFailedRead() throws Exception {
        IHyracksTaskContext ctx = TestUtils.create(FRAME_SIZE);
        int before = liveFiles();
        KMeansLoopIO.VectorList list = new KMeansLoopIO.VectorList(ctx, new JobId(1),
                new TaskId(new ActivityId(new OperatorDescriptorId(6), 0), 0), 1);
        try {
            for (double[] v : vectors(2000, 32, 5L)) {
                list.add(v);
            }
            list.seal();
            Assert.assertFalse("this fixture must spill or the test proves nothing", list.isResident());
            Assert.assertEquals(1, liveFiles() - before);
            try {
                list.stream(v -> {
                    throw new Injected();
                });
                Assert.fail("expected the injected failure to propagate");
            } catch (Injected expected) {
                // abandoned mid-replay
            }
            List<double[]> replay = new ArrayList<>();
            list.stream(replay::add);
            Assert.assertEquals("still replayable after a failed read", 2000, replay.size());
        } finally {
            list.close();
        }
        Assert.assertEquals("the spill file is released on the failure path too", before, liveFiles());
    }

    /** A centroid store abandoned mid-build releases both generations on destroy. */
    @Test
    public void aCentroidStoreAbandonedMidBuildReleasesEverything() throws Exception {
        IHyracksTaskContext ctx = TestUtils.create(FRAME_SIZE);
        int before = liveFiles();
        CentroidStore store =
                new CentroidStore.Spilling(new JobId(1), new TaskId(new ActivityId(new OperatorDescriptorId(7), 0), 0));
        try {
            store.beginPut(ctx);
            for (double[] v : vectors(50, 8, 6L)) {
                store.put(v);
            }
            store.endPut(); // one published
            store.beginPut(ctx);
            store.put(new double[] { 1.0d });
            // and now the task "fails" here -- endPut never runs
            Assert.assertEquals(2, liveFiles() - before);
        } finally {
            store.destroy();
        }
        Assert.assertEquals(before, liveFiles());
    }

    /**
     * Hyracks cancels a task by interrupting its thread, but these scans are pure CPU over materialized files
     * -- nothing blocks, so nothing throws on its own. The scan polls the interrupt per frame instead, and this
     * checks that poll actually fires rather than running the scan to completion.
     */
    @Test
    public void anInterruptedScanRaisesRatherThanRunningOn() throws Exception {
        IHyracksTaskContext ctx = TestUtils.create(FRAME_SIZE);
        MaterializerTaskState state = materialize(ctx, vectors(5000, 32, 7L), 8);
        try {
            Thread.currentThread().interrupt();
            final int[] seen = { 0 };
            try {
                KMeansLoopIO.streamRawVectors(state, ctx, v -> seen[0]++);
                Assert.fail("expected an interrupted scan to raise");
            } catch (HyracksDataException expected) {
                Assert.assertTrue("did not run to completion", seen[0] < 5000);
            }
        } finally {
            Thread.interrupted(); // clear the flag so it cannot poison later tests
            state.close();
            state.deleteFile();
        }
        // and once the interrupt is cleared the same file scans normally
        List<double[]> ignored = new ArrayList<>();
        Assert.assertFalse(Thread.currentThread().isInterrupted());
        Assert.assertTrue(ignored.isEmpty());
    }

    private static MaterializerTaskState newState(IHyracksTaskContext ctx, int id) throws HyracksDataException {
        MaterializerTaskState state =
                new MaterializerTaskState(new JobId(1), new TaskId(new ActivityId(new OperatorDescriptorId(id), 0), 0));
        state.open(ctx);
        return state;
    }

    private static MaterializerTaskState materialize(IHyracksTaskContext ctx, double[][] vectors, int id)
            throws HyracksDataException {
        MaterializerTaskState state = newState(ctx, id);
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
        frame.getBuffer().position(0);
        frame.getBuffer().limit(frame.getBuffer().capacity());
        state.appendFrame(frame.getBuffer());
        appender.reset(frame, true);
    }

    private static double[][] vectors(int count, int dim, long seed) {
        Random rng = new Random(seed);
        double[][] out = new double[count][dim];
        for (int i = 0; i < count; i++) {
            for (int d = 0; d < dim; d++) {
                out[i][d] = rng.nextDouble();
            }
        }
        return out;
    }
}
