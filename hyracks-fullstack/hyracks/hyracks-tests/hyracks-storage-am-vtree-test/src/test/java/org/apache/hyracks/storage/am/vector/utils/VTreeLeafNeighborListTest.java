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

package org.apache.hyracks.storage.am.vector.utils;

import org.junit.Assert;
import org.junit.Test;

/**
 * The leaf neighbor-list byte encoding. A provisional and a resolved entry have the same width, so the two-pass
 * static-structure build resolves each entry in place without shifting the tuple around it.
 */
public class VTreeLeafNeighborListTest {

    /** Every centroid id becomes one entry, readable back, and each is marked unresolved. */
    @Test
    public void provisionalEntriesRoundTrip() {
        int[] neighbours = { 7, 11, 4 };
        byte[] encoded = VTreeLeafNeighborList.encodeProvisional(neighbours);

        Assert.assertEquals(neighbours.length * VTreeLeafNeighborList.ENTRY_SIZE, encoded.length);
        Assert.assertEquals(neighbours.length, VTreeLeafNeighborList.entryCount(encoded, 0, encoded.length));
        for (int i = 0; i < neighbours.length; i++) {
            Assert.assertEquals(neighbours[i], VTreeLeafNeighborList.readCentroidId(encoded, 0, i));
            Assert.assertFalse("a freshly encoded entry is not resolved",
                    VTreeLeafNeighborList.isResolved(encoded, 0, i));
        }
    }

    /** No neighbours encodes to the shared empty array. */
    @Test
    public void noNeighboursEncodesEmpty() {
        Assert.assertSame(VTreeLeafNeighborList.EMPTY, VTreeLeafNeighborList.encodeProvisional(null));
        Assert.assertSame(VTreeLeafNeighborList.EMPTY, VTreeLeafNeighborList.encodeProvisional(new int[0]));
        Assert.assertEquals(0, VTreeLeafNeighborList.entryCount(VTreeLeafNeighborList.EMPTY, 0, 0));
    }

    /**
     * The width invariant: resolving an entry must not change the buffer's length, and must leave its
     * neighbours untouched. This is what lets the resolution pass rewrite entries on a live page.
     */
    @Test
    public void resolvingAnEntryIsInPlaceAndLocal() {
        byte[] encoded = VTreeLeafNeighborList.encodeProvisional(new int[] { 7, 11, 4 });
        int lengthBefore = encoded.length;

        VTreeLeafNeighborList.writeResolved(encoded, 0, 1, 42, 3);

        Assert.assertEquals("resolution must not change the encoded width", lengthBefore, encoded.length);
        Assert.assertTrue(VTreeLeafNeighborList.isResolved(encoded, 0, 1));
        Assert.assertEquals(42, VTreeLeafNeighborList.readPageId(encoded, 0, 1));
        Assert.assertEquals(3, VTreeLeafNeighborList.readSlot(encoded, 0, 1));
        Assert.assertEquals(7, VTreeLeafNeighborList.readCentroidId(encoded, 0, 0));
        Assert.assertFalse(VTreeLeafNeighborList.isResolved(encoded, 0, 0));
        Assert.assertEquals(4, VTreeLeafNeighborList.readCentroidId(encoded, 0, 2));
        Assert.assertFalse(VTreeLeafNeighborList.isResolved(encoded, 0, 2));
    }

    /** Slot 0 reads as resolved, since the sentinel is negative and every real slot is {@code >= 0}. */
    @Test
    public void slotZeroIsResolvedNotMistakenForTheSentinel() {
        byte[] encoded = VTreeLeafNeighborList.encodeProvisional(new int[] { 7 });

        VTreeLeafNeighborList.writeResolved(encoded, 0, 0, 5, 0);

        Assert.assertTrue("slot 0 is a real slot", VTreeLeafNeighborList.isResolved(encoded, 0, 0));
        Assert.assertEquals(0, VTreeLeafNeighborList.readSlot(encoded, 0, 0));
        Assert.assertTrue("the sentinel must not collide with any real slot", VTreeLeafNeighborList.SENTINEL < 0);
    }

    /** Entries are addressed from an offset, so a list embedded in a larger buffer reads correctly. */
    @Test
    public void entriesAreReadRelativeToTheGivenStart() {
        byte[] embedded = new byte[3 * VTreeLeafNeighborList.ENTRY_SIZE + 5];
        int start = 5;
        byte[] list = VTreeLeafNeighborList.encodeProvisional(new int[] { 100, 200 });
        System.arraycopy(list, 0, embedded, start, list.length);

        Assert.assertEquals(100, VTreeLeafNeighborList.readCentroidId(embedded, start, 0));
        Assert.assertEquals(200, VTreeLeafNeighborList.readCentroidId(embedded, start, 1));
        Assert.assertFalse(VTreeLeafNeighborList.isResolved(embedded, start, 0));
    }

    /** Negative ids survive the round trip: centroid ids are not assumed non-negative by the encoding. */
    @Test
    public void negativeCentroidIdsRoundTrip() {
        byte[] encoded = VTreeLeafNeighborList.encodeProvisional(new int[] { -3, Integer.MAX_VALUE });

        Assert.assertEquals(-3, VTreeLeafNeighborList.readCentroidId(encoded, 0, 0));
        Assert.assertEquals(Integer.MAX_VALUE, VTreeLeafNeighborList.readCentroidId(encoded, 0, 1));
    }
}
