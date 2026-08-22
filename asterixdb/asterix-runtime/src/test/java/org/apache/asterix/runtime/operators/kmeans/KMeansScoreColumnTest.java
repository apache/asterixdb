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
 * Pins the score column that Op1 writes and Op3 reads. The column carries no key -- entry {@code i} is vector
 * {@code i}, and the two sides stay aligned only because both are strictly sequential. That makes a length or
 * ordering slip silent by construction at the data level, so these tests hold the round trip exact and check
 * that the one detectable symptom, running off the end, is raised rather than swallowed.
 */
public class KMeansScoreColumnTest {

    private static final int FRAME_SIZE = 32768;

    /** Entries survive exactly, across chunk and frame boundaries, whatever block sizes they were written in. */
    @Test
    public void columnRoundTripsExactly() throws Exception {
        // Well past both the 1024-entry chunk and a 32 KB frame, so neither boundary is left untested.
        int count = 40_000;
        double[] nearest = new double[count];
        int[] index = new int[count];
        Random rng = new Random(4L);
        for (int i = 0; i < count; i++) {
            nearest[i] = rng.nextDouble() * 1000.0d;
            index[i] = rng.nextInt(5000);
        }
        // Ragged block sizes: the writer is fed whatever the scan's blocks happened to be.
        assertRoundTrip(nearest, index, new int[] { 1, 7, 1023, 1024, 1025, 4096, 9999 });
    }

    /** The values Op3 branches on -- NaN and +infinity -- must not be normalised away in transit. */
    @Test
    public void nonFiniteScoresSurvive() throws Exception {
        double[] nearest = { Double.NaN, Double.POSITIVE_INFINITY, 0.0d, -0.0d, Double.MIN_VALUE, 3.5d };
        int[] index = { -1, -1, 0, 7, 12, 3 };
        assertRoundTrip(nearest, index, new int[] { 2 });
    }

    /** A column shorter than the vectors it scores must fail loudly, not mis-pair the tail. */
    @Test
    public void readingPastTheEndIsRaised() throws Exception {
        IHyracksTaskContext ctx = TestUtils.create(FRAME_SIZE);
        MaterializerTaskState state = newState(ctx);
        try {
            KMeansLoopIO.ScoreColumnWriter writer = new KMeansLoopIO.ScoreColumnWriter(state, ctx);
            writer.append(new double[] { 1.0d, 2.0d }, new int[] { 0, 1 }, 2);
            writer.finish();
            try (KMeansLoopIO.ScoreColumnReader reader = new KMeansLoopIO.ScoreColumnReader(state, ctx)) {
                reader.advance();
                reader.advance();
                try {
                    reader.advance();
                    Assert.fail("expected the third read to be rejected");
                } catch (HyracksDataException e) {
                    Assert.assertTrue(String.valueOf(e.getMessage()).contains("shorter than the vectors"));
                }
            }
        } finally {
            state.close();
            state.deleteFile();
        }
    }

    /** An empty column is readable and empty, which is the zero-vector partition. */
    @Test
    public void emptyColumnReadsAsEmpty() throws Exception {
        IHyracksTaskContext ctx = TestUtils.create(FRAME_SIZE);
        MaterializerTaskState state = newState(ctx);
        try {
            new KMeansLoopIO.ScoreColumnWriter(state, ctx).finish();
            try (KMeansLoopIO.ScoreColumnReader reader = new KMeansLoopIO.ScoreColumnReader(state, ctx)) {
                try {
                    reader.advance();
                    Assert.fail("expected a read from an empty column to be rejected");
                } catch (HyracksDataException e) {
                    Assert.assertTrue(String.valueOf(e.getMessage()).contains("shorter than the vectors"));
                }
            }
        } finally {
            state.close();
            state.deleteFile();
        }
    }

    /**
     * Writes the entries in the given repeating block sizes and asserts every one reads back with identical
     * bits. Raw-bits equality, not a delta: the distances feed a Bernoulli threshold and a sum, so a value that
     * is merely close is a different result.
     */
    private static void assertRoundTrip(double[] nearest, int[] index, int[] blockSizes) throws Exception {
        IHyracksTaskContext ctx = TestUtils.create(FRAME_SIZE);
        MaterializerTaskState state = newState(ctx);
        try {
            KMeansLoopIO.ScoreColumnWriter writer = new KMeansLoopIO.ScoreColumnWriter(state, ctx);
            int at = 0;
            int which = 0;
            while (at < nearest.length) {
                int n = Math.min(blockSizes[which++ % blockSizes.length], nearest.length - at);
                double[] blockNearest = new double[n];
                int[] blockIndex = new int[n];
                System.arraycopy(nearest, at, blockNearest, 0, n);
                System.arraycopy(index, at, blockIndex, 0, n);
                writer.append(blockNearest, blockIndex, n);
                at += n;
            }
            writer.finish();

            try (KMeansLoopIO.ScoreColumnReader reader = new KMeansLoopIO.ScoreColumnReader(state, ctx)) {
                for (int i = 0; i < nearest.length; i++) {
                    reader.advance();
                    Assert.assertEquals("nearest at " + i, Double.doubleToRawLongBits(nearest[i]),
                            Double.doubleToRawLongBits(reader.nearest()));
                    Assert.assertEquals("index at " + i, index[i], reader.index());
                }
            }
        } finally {
            state.close();
            state.deleteFile();
        }
    }

    private static MaterializerTaskState newState(IHyracksTaskContext ctx) throws HyracksDataException {
        MaterializerTaskState state =
                new MaterializerTaskState(new JobId(1), new TaskId(new ActivityId(new OperatorDescriptorId(1), 0), 0));
        state.open(ctx);
        return state;
    }
}
