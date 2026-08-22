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
 * Pins {@link KMeansLoopIO.VectorList}, which holds RECLUSTER's candidate set in the heap while it fits the
 * frame budget and in a run file once it does not.
 * <p>
 * The whole point is that residency is a memory decision and not a semantic one, so every test here reads the
 * same content back from both modes and compares it exactly. RECLUSTER reads this set once per centroid it
 * picks, and a difference between the two modes would make the same query answer differently on differently
 * configured clusters -- which is the failure this class exists to prevent.
 */
public class KMeansVectorListTest {

    private static final int FRAME_SIZE = 32768;

    @Test
    public void aSmallListStaysResidentAndALargeOneSpills() throws Exception {
        IHyracksTaskContext ctx = TestUtils.create(FRAME_SIZE);
        try (KMeansLoopIO.VectorList small = newList(ctx, 1, 4)) {
            for (double[] v : vectors(10, 8, 1L)) {
                small.add(v);
            }
            small.seal();
            Assert.assertTrue("10 small vectors fit 4 frames", small.isResident());
        }
        try (KMeansLoopIO.VectorList big = newList(ctx, 2, 1)) {
            for (double[] v : vectors(4000, 64, 2L)) { // ~2.2 MB against one 32 KB frame
                big.add(v);
            }
            big.seal();
            Assert.assertFalse("4000 wide vectors cannot fit one frame", big.isResident());
        }
    }

    /** Whatever the mode, the list replays exactly what was added, in order. */
    @Test
    public void bothModesReplayTheSameContent() throws Exception {
        double[][] input = vectors(3000, 32, 7L);
        List<double[]> resident = replay(input, 4096); // budget far above the content
        List<double[]> spilled = replay(input, 1); // budget far below it
        Assert.assertEquals(input.length, resident.size());
        Assert.assertEquals(input.length, spilled.size());
        for (int i = 0; i < input.length; i++) {
            Assert.assertArrayEquals("resident at " + i, input[i], resident.get(i), 0.0d);
            Assert.assertArrayEquals("spilled at " + i, input[i], spilled.get(i), 0.0d);
        }
    }

    /** get() is the one non-sequential read -- the member a round just picked -- and must agree across modes. */
    @Test
    public void getAgreesAcrossModes() throws Exception {
        IHyracksTaskContext ctx = TestUtils.create(FRAME_SIZE);
        double[][] input = vectors(2000, 32, 11L);
        try (KMeansLoopIO.VectorList resident = newList(ctx, 5, 4096);
                KMeansLoopIO.VectorList spilled = newList(ctx, 6, 1)) {
            for (double[] v : input) {
                resident.add(v);
                spilled.add(v);
            }
            resident.seal();
            spilled.seal();
            Assert.assertTrue(resident.isResident());
            Assert.assertFalse(spilled.isResident());
            for (int i : new int[] { 0, 1, 999, 1500, input.length - 1 }) {
                Assert.assertArrayEquals("resident get " + i, input[i], resident.get(i), 0.0d);
                Assert.assertArrayEquals("spilled get " + i, input[i], spilled.get(i), 0.0d);
            }
        }
    }

    /** Reading past the end is a broken invariant, not a silent null. */
    @Test
    public void getPastTheEndIsRaised() throws Exception {
        IHyracksTaskContext ctx = TestUtils.create(FRAME_SIZE);
        try (KMeansLoopIO.VectorList list = newList(ctx, 7, 1)) {
            for (double[] v : vectors(2000, 32, 3L)) {
                list.add(v);
            }
            list.seal();
            Assert.assertFalse(list.isResident());
            try {
                list.get(2000);
                Assert.fail("expected a read past the end to be rejected");
            } catch (HyracksDataException e) {
                Assert.assertTrue(String.valueOf(e.getMessage()).contains("past the end"));
            }
        }
    }

    /** An empty list is streamable and empty -- the pool can legitimately attract nothing. */
    @Test
    public void anEmptyListIsEmpty() throws Exception {
        IHyracksTaskContext ctx = TestUtils.create(FRAME_SIZE);
        try (KMeansLoopIO.VectorList list = newList(ctx, 8, 4)) {
            list.seal();
            Assert.assertEquals(0, list.size());
            List<double[]> seen = new ArrayList<>();
            list.stream(seen::add);
            Assert.assertTrue(seen.isEmpty());
        }
    }

    /** A spilled list leaves no run file behind once closed. */
    @Test
    public void closingASpilledListReleasesItsFile() throws Exception {
        IHyracksTaskContext ctx = TestUtils.create(FRAME_SIZE);
        int before = liveFiles();
        KMeansLoopIO.VectorList list = newList(ctx, 9, 1);
        for (double[] v : vectors(2000, 32, 5L)) {
            list.add(v);
        }
        list.seal();
        Assert.assertFalse(list.isResident());
        Assert.assertEquals(1, liveFiles() - before);
        list.close();
        Assert.assertEquals(before, liveFiles());
    }

    private static List<double[]> replay(double[][] input, int framesLimit) throws Exception {
        IHyracksTaskContext ctx = TestUtils.create(FRAME_SIZE);
        List<double[]> out = new ArrayList<>();
        try (KMeansLoopIO.VectorList list = newList(ctx, 3, framesLimit)) {
            for (double[] v : input) {
                list.add(v);
            }
            list.seal();
            list.stream(out::add);
        }
        return out;
    }

    private static KMeansLoopIO.VectorList newList(IHyracksTaskContext ctx, int id, int framesLimit) {
        return new KMeansLoopIO.VectorList(ctx, new JobId(1),
                new TaskId(new ActivityId(new OperatorDescriptorId(id), 0), 0), framesLimit);
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
