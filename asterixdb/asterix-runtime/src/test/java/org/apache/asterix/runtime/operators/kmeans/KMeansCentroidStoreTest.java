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

import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.ActivityId;
import org.apache.hyracks.api.dataflow.OperatorDescriptorId;
import org.apache.hyracks.api.dataflow.TaskId;
import org.apache.hyracks.api.job.JobId;
import org.apache.hyracks.test.support.TestUtils;
import org.junit.Assert;
import org.junit.Test;

/**
 * Pins {@link CentroidStore.Spilling}, the run-file-backed centroid handoff between the Lloyd loop's tail and
 * its head. Each iteration replaces the whole set, so what matters is that a generation is published atomically
 * and in order, and that the one it replaces goes away rather than accumulating for the length of the loop.
 */
public class KMeansCentroidStoreTest {

    private static final int FRAME_SIZE = 32768;

    @Test
    public void aPublishedSetReplaysInOrder() throws Exception {
        IHyracksTaskContext ctx = TestUtils.create(FRAME_SIZE);
        CentroidStore store = newStore();
        try {
            double[][] set = vectors(37, 5, 1L);
            publish(store, ctx, set);
            Assert.assertEquals(set.length, store.size());
            assertReplays(store, ctx, set);
        } finally {
            store.destroy();
        }
    }

    /** A set far larger than one frame still replays in centroid-index order. */
    @Test
    public void aSetSpanningManyFramesReplaysInOrder() throws Exception {
        IHyracksTaskContext ctx = TestUtils.create(FRAME_SIZE);
        CentroidStore store = newStore();
        try {
            double[][] set = vectors(9000, 32, 2L); // ~2.3 MB, dozens of frames
            publish(store, ctx, set);
            Assert.assertEquals(set.length, store.size());
            assertReplays(store, ctx, set);
        } finally {
            store.destroy();
        }
    }

    /** Each generation fully replaces the last -- size and contents both. */
    @Test
    public void eachGenerationReplacesTheLast() throws Exception {
        IHyracksTaskContext ctx = TestUtils.create(FRAME_SIZE);
        CentroidStore store = newStore();
        try {
            for (int generation = 1; generation <= 5; generation++) {
                double[][] set = vectors(10 * generation, 4, generation);
                publish(store, ctx, set);
                Assert.assertEquals("generation " + generation, set.length, store.size());
                assertReplays(store, ctx, set);
            }
        } finally {
            store.destroy();
        }
    }

    /** An iteration that produced no centroids publishes an empty set rather than leaving the old one in place. */
    @Test
    public void anEmptySetIsPublished() throws Exception {
        IHyracksTaskContext ctx = TestUtils.create(FRAME_SIZE);
        CentroidStore store = newStore();
        try {
            publish(store, ctx, vectors(6, 3, 7L));
            Assert.assertEquals(6, store.size());
            publish(store, ctx, new double[0][]);
            Assert.assertEquals(0, store.size());
            assertReplays(store, ctx, new double[0][]);
        } finally {
            store.destroy();
        }
    }

    /** Nothing is readable before the first publish. */
    @Test
    public void anUnpublishedStoreIsEmpty() throws Exception {
        IHyracksTaskContext ctx = TestUtils.create(FRAME_SIZE);
        CentroidStore store = newStore();
        try {
            Assert.assertEquals(0, store.size());
            assertReplays(store, ctx, new double[0][]);
        } finally {
            store.destroy();
        }
    }

    /** A set that was started but never published leaves the last published one untouched. */
    @Test
    public void anAbandonedSetDoesNotDisturbThePublishedOne() throws Exception {
        IHyracksTaskContext ctx = TestUtils.create(FRAME_SIZE);
        CentroidStore store = newStore();
        try {
            double[][] good = vectors(12, 4, 3L);
            publish(store, ctx, good);

            store.beginPut(ctx); // started, then abandoned by the next beginPut
            store.put(new double[] { 99.0d, 99.0d, 99.0d, 99.0d });
            store.beginPut(ctx);
            store.put(new double[] { 1.0d, 1.0d, 1.0d, 1.0d });

            Assert.assertEquals("the published set must not move until endPut", good.length, store.size());
            assertReplays(store, ctx, good);
        } finally {
            store.destroy();
        }
    }

    /** destroy() is idempotent, since the loop releases the store on every exit path. */
    @Test
    public void destroyIsIdempotent() throws Exception {
        IHyracksTaskContext ctx = TestUtils.create(FRAME_SIZE);
        CentroidStore store = newStore();
        publish(store, ctx, vectors(5, 2, 4L));
        store.destroy();
        store.destroy();
        Assert.assertEquals(0, store.size());
    }

    private static void publish(CentroidStore store, IHyracksTaskContext ctx, double[][] set) throws Exception {
        store.beginPut(ctx);
        for (double[] centroid : set) {
            store.put(centroid);
        }
        store.endPut();
    }

    private static void assertReplays(CentroidStore store, IHyracksTaskContext ctx, double[][] expected)
            throws Exception {
        List<double[]> seen = new ArrayList<>();
        store.stream(ctx, seen::add);
        Assert.assertEquals("centroid count", expected.length, seen.size());
        for (int i = 0; i < expected.length; i++) {
            Assert.assertArrayEquals("centroid " + i, expected[i], seen.get(i), 0.0d);
        }
    }

    private static CentroidStore newStore() {
        return new CentroidStore.Spilling(new JobId(1), new TaskId(new ActivityId(new OperatorDescriptorId(1), 0), 0));
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
