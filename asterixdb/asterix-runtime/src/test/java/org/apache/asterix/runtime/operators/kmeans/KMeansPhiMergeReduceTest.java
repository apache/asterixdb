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

import static org.apache.asterix.runtime.operators.kmeans.KMeansPhiMergeOperatorDescriptor.allReported;
import static org.apache.asterix.runtime.operators.kmeans.KMeansPhiMergeOperatorDescriptor.newSigmaSlots;
import static org.apache.asterix.runtime.operators.kmeans.KMeansPhiMergeOperatorDescriptor.reduceInPartitionOrder;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.junit.Test;

/**
 * The phi reduce must not depend on the order partitions report in.
 * <p>
 * Frames reach the merge over a concurrent M-to-1 connector, so arrival order varies run to run. Floating-point
 * addition is not associative, so an arrival-order sum yields a different phi for the same data — and phi scales
 * every draw probability, so the clustering would differ too. These tests pin the slot-then-ordered-sum design,
 * and {@link #arrivalOrderSumIsNotStable_theBugThisPrevents()} shows the hazard is real rather than theoretical.
 */
public class KMeansPhiMergeReduceTest {

    /** Values whose sum genuinely depends on association order in IEEE-754 double. */
    private static final double[] SIGMAS = { 0.1d, 0.2d, 0.3d, 1.0e16d, -1.0e16d, 7.7d };

    private static double reduceViaSlots(List<Integer> arrivalOrder) {
        double[] slots = newSigmaSlots(SIGMAS.length);
        for (int part : arrivalOrder) {
            assertFalse("a partition reported twice", allReported(slots));
            slots[part] = SIGMAS[part];
        }
        assertTrue("every partition should have reported", allReported(slots));
        return reduceInPartitionOrder(slots);
    }

    private static List<Integer> order(int... parts) {
        List<Integer> l = new ArrayList<>();
        for (int p : parts) {
            l.add(p);
        }
        return l;
    }

    /** The property: any arrival order yields bit-identical phi. */
    @Test
    public void phiIsIdenticalWhateverTheArrivalOrder() {
        double inOrder = reduceViaSlots(order(0, 1, 2, 3, 4, 5));
        double reversed = reduceViaSlots(order(5, 4, 3, 2, 1, 0));
        double scattered = reduceViaSlots(order(3, 0, 5, 1, 4, 2));

        assertEquals("reversed arrival changed phi", Double.doubleToLongBits(inOrder),
                Double.doubleToLongBits(reversed));
        assertEquals("scattered arrival changed phi", Double.doubleToLongBits(inOrder),
                Double.doubleToLongBits(scattered));
    }

    /** Exhaustive over every permutation, so the property is not an accident of three samples. */
    @Test
    public void phiIsIdenticalOverEveryPermutation() {
        long expected = Double.doubleToLongBits(reduceViaSlots(order(0, 1, 2, 3, 4, 5)));
        List<Integer> perm = order(0, 1, 2, 3, 4, 5);
        int permutations = 0;
        do {
            assertEquals("permutation " + perm + " changed phi", expected,
                    Double.doubleToLongBits(reduceViaSlots(perm)));
            permutations++;
        } while (nextPermutation(perm));
        assertEquals("expected 6! permutations", 720, permutations);
    }

    /**
     * The bug this design prevents: summing on arrival, as the previous implementation did, is order-dependent.
     * If this ever starts passing, IEEE-754 has changed and the slot machinery is no longer needed.
     */
    @Test
    public void arrivalOrderSumIsNotStable_theBugThisPrevents() {
        double forward = 0.0d;
        for (int i = 0; i < SIGMAS.length; i++) {
            forward += SIGMAS[i];
        }
        double backward = 0.0d;
        for (int i = SIGMAS.length - 1; i >= 0; i--) {
            backward += SIGMAS[i];
        }
        assertNotEquals("these values were chosen because association order matters", Double.doubleToLongBits(forward),
                Double.doubleToLongBits(backward));
    }

    /** A round is only complete once every partition has filled its slot. */
    @Test
    public void roundIsIncompleteUntilEveryPartitionReports() {
        double[] slots = newSigmaSlots(3);
        assertFalse(allReported(slots));
        slots[0] = 1.0d;
        slots[2] = 3.0d;
        assertFalse("gap at partition 1 should not count as complete", allReported(slots));
        slots[1] = 2.0d;
        assertTrue(allReported(slots));
    }

    private static boolean nextPermutation(List<Integer> a) {
        int i = a.size() - 2;
        while (i >= 0 && a.get(i) >= a.get(i + 1)) {
            i--;
        }
        if (i < 0) {
            return false;
        }
        int j = a.size() - 1;
        while (a.get(j) <= a.get(i)) {
            j--;
        }
        Collections.swap(a, i, j);
        Collections.reverse(a.subList(i + 1, a.size()));
        return true;
    }
}
