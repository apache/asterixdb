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

import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.ActivityId;
import org.apache.hyracks.api.dataflow.OperatorDescriptorId;
import org.apache.hyracks.api.dataflow.TaskId;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.job.JobId;
import org.apache.hyracks.dataflow.std.misc.MaterializerTaskState;
import org.apache.hyracks.test.support.TestUtils;
import org.junit.Assert;
import org.junit.Test;

/**
 * The memory work traded heap for run files, which only helps if the files themselves stay bounded. These
 * structures are all replaced per round or per iteration, so a generation that outlived its successor would
 * turn a bounded heap into an unbounded disk footprint over a long loop -- and the loops run for as many
 * rounds and iterations as the query asks for.
 * <p>
 * Counted on disk rather than asserted in prose. The test context puts managed workspace files in the JVM's
 * temp directory, so the live file count is directly observable.
 */
public class KMeansRunFileLifecycleTest {

    private static final int FRAME_SIZE = 32768;
    private static final String WORKSPACE_PREFIX = MaterializerTaskState.class.getSimpleName();

    /** Managed workspace files currently on disk. */
    private static int liveFiles() {
        File[] files = new File(System.getProperty("java.io.tmpdir")).listFiles();
        if (files == null) {
            return 0;
        }
        int n = 0;
        for (File f : files) {
            if (f.getName().startsWith(WORKSPACE_PREFIX)) {
                n++;
            }
        }
        return n;
    }

    /**
     * The centroid store swaps a whole generation at a time, so exactly two files are live at the swap -- the
     * set being built and the one still readable -- and never more, however many iterations the loop runs.
     */
    @Test
    public void centroidStoreKeepsAtMostTwoGenerationsOnDisk() throws Exception {
        IHyracksTaskContext ctx = TestUtils.create(FRAME_SIZE);
        int before = liveFiles();
        CentroidStore store =
                new CentroidStore.Spilling(new JobId(1), new TaskId(new ActivityId(new OperatorDescriptorId(1), 0), 0));
        try {
            int peak = 0;
            for (int generation = 0; generation < 20; generation++) {
                store.beginPut(ctx);
                peak = Math.max(peak, liveFiles() - before); // both generations live here
                for (double[] centroid : vectors(25, 8, generation)) {
                    store.put(centroid);
                }
                store.endPut();
                Assert.assertEquals("one generation live after publish, generation " + generation, 1,
                        liveFiles() - before);
            }
            Assert.assertEquals("at most two live across the swap", 2, peak);
        } finally {
            store.destroy();
        }
        Assert.assertEquals("destroy releases the last generation", before, liveFiles());
    }

    /** An abandoned generation is released by the next beginPut rather than left behind. */
    @Test
    public void anAbandonedGenerationIsNotLeaked() throws Exception {
        IHyracksTaskContext ctx = TestUtils.create(FRAME_SIZE);
        int before = liveFiles();
        CentroidStore store =
                new CentroidStore.Spilling(new JobId(1), new TaskId(new ActivityId(new OperatorDescriptorId(2), 0), 0));
        try {
            store.beginPut(ctx);
            store.put(new double[] { 1.0d, 2.0d });
            for (int i = 0; i < 10; i++) {
                store.beginPut(ctx); // abandons the previous build
                store.put(new double[] { i, i });
            }
            Assert.assertEquals("only the current build is live", 1, liveFiles() - before);
        } finally {
            store.destroy();
        }
        Assert.assertEquals(before, liveFiles());
    }

    /** destroy() releases both a published generation and one still being built. */
    @Test
    public void destroyReleasesBothGenerations() throws Exception {
        IHyracksTaskContext ctx = TestUtils.create(FRAME_SIZE);
        int before = liveFiles();
        CentroidStore store =
                new CentroidStore.Spilling(new JobId(1), new TaskId(new ActivityId(new OperatorDescriptorId(3), 0), 0));
        store.beginPut(ctx);
        store.put(new double[] { 1.0d });
        store.endPut(); // published
        store.beginPut(ctx); // and a build in flight
        store.put(new double[] { 2.0d });
        Assert.assertEquals(2, liveFiles() - before);
        store.destroy();
        Assert.assertEquals("both released", before, liveFiles());
    }

    /** A score column is one file, released when its state is. */
    @Test
    public void aScoreColumnIsReleasedWithItsState() throws Exception {
        IHyracksTaskContext ctx = TestUtils.create(FRAME_SIZE);
        int before = liveFiles();
        MaterializerTaskState state =
                new MaterializerTaskState(new JobId(1), new TaskId(new ActivityId(new OperatorDescriptorId(4), 0), 0));
        state.open(ctx);
        try {
            KMeansLoopIO.ScoreColumnWriter w = new KMeansLoopIO.ScoreColumnWriter(state, ctx);
            w.append(new double[] { 1.0d, 2.0d }, new int[] { 0, 1 }, 2);
            w.finish();
            Assert.assertEquals(1, liveFiles() - before);
        } finally {
            state.close();
            state.deleteFile();
        }
        Assert.assertEquals(before, liveFiles());
    }

    /** A partition with no rows must not fault the scan or the accumulator -- the width filter can empty one. */
    @Test
    public void anEmptyPartitionProducesNothingAndFaultsNothing() throws Exception {
        IHyracksTaskContext ctx = TestUtils.create(FRAME_SIZE);
        int before = liveFiles();
        MaterializerTaskState vectors = newState(ctx, 5);
        MaterializerTaskState pool = newState(ctx, 6);
        MaterializerTaskState column = newState(ctx, 7);
        try {
            List<Integer> blocks = new ArrayList<>();
            KMeansLoopIO.streamScoredAgainstPool(vectors, pool, ctx, 4, (v, n, near, idx) -> blocks.add(n));
            Assert.assertTrue("no vectors, so no block should be delivered", blocks.isEmpty());

            KMeansLoopIO.ScoreColumnWriter w = new KMeansLoopIO.ScoreColumnWriter(column, ctx);
            w.finish();
            List<Integer> slots = new ArrayList<>();
            KMeansLoopIO.accumulateInWindows(vectors, column, ctx, 64, 8, (i, c, s) -> slots.add(i));
            Assert.assertTrue("no vectors, so no slot is non-empty", slots.isEmpty());
        } finally {
            for (MaterializerTaskState s : new MaterializerTaskState[] { vectors, pool, column }) {
                s.close();
                s.deleteFile();
            }
        }
        Assert.assertEquals(before, liveFiles());
    }

    private static MaterializerTaskState newState(IHyracksTaskContext ctx, int id) throws HyracksDataException {
        MaterializerTaskState state =
                new MaterializerTaskState(new JobId(1), new TaskId(new ActivityId(new OperatorDescriptorId(id), 0), 0));
        state.open(ctx);
        return state;
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
