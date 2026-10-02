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

package org.apache.hyracks.storage.am.vector.impls;

import org.apache.hyracks.storage.common.buffercache.IBufferCache;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

/** The centroid-to-directory-page lookup that {@code VTree}, its search cursor and its flush loader share. */
public class StaticStructureRefTest {

    /** Centroid ids are absolute; the mapping is indexed relative to the first leaf centroid. */
    @Test
    public void directoryPageIsLookedUpRelativeToTheFirstCentroid() {
        StaticStructureRef ref = ref(new int[] { 40, 41, 42 }, 10);

        Assert.assertEquals(40, ref.directoryPageFor(10));
        Assert.assertEquals(41, ref.directoryPageFor(11));
        Assert.assertEquals(42, ref.directoryPageFor(12));
    }

    /**
     * Navigating the shared static structure can land on a centroid this component has no directory for, so an
     * unmapped centroid returns a sentinel that tells the caller to fall back to its own resolution.
     */
    @Test
    public void centroidsOutsideTheMappingReportNoDirectoryPage() {
        StaticStructureRef ref = ref(new int[] { 40, 41, 42 }, 10);

        Assert.assertEquals(StaticStructureRef.NO_DIRECTORY_PAGE, ref.directoryPageFor(9));
        Assert.assertEquals(StaticStructureRef.NO_DIRECTORY_PAGE, ref.directoryPageFor(13));
        Assert.assertEquals(StaticStructureRef.NO_DIRECTORY_PAGE, ref.directoryPageFor(Integer.MIN_VALUE));
        Assert.assertEquals(StaticStructureRef.NO_DIRECTORY_PAGE, ref.directoryPageFor(Integer.MAX_VALUE));
    }

    /** An empty mapping covers no centroid and does not throw. */
    @Test
    public void anEmptyMappingCoversNoCentroid() {
        Assert.assertEquals(StaticStructureRef.NO_DIRECTORY_PAGE, ref(new int[0], 0).directoryPageFor(0));
    }

    /** A half-built attachment is refused at construction, so it cannot fail later during navigation. */
    @Test
    public void theMandatoryPartsAreRejectedWhenMissing() {
        Assert.assertThrows(NullPointerException.class, () -> new StaticStructureRef(null, 1, 2, new int[1], 0, 1));
        Assert.assertThrows(NullPointerException.class,
                () -> new StaticStructureRef(Mockito.mock(IBufferCache.class), 1, 2, null, 0, 1));
    }

    private static StaticStructureRef ref(int[] dirPageMap, int firstLeafCentroidId) {
        return new StaticStructureRef(Mockito.mock(IBufferCache.class), 3, 4, dirPageMap, firstLeafCentroidId,
                dirPageMap.length);
    }
}
