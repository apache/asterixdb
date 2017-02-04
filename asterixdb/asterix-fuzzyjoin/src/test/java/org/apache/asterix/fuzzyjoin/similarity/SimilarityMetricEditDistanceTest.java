/**
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

package org.apache.asterix.fuzzyjoin.similarity;

import static org.apache.hyracks.data.std.primitive.UTF8StringPointable.generateUTF8Pointable;
import static org.junit.Assert.assertEquals;

import org.apache.hyracks.data.std.primitive.UTF8StringPointable;
import org.junit.Test;

public class SimilarityMetricEditDistanceTest {

    private static final SimilarityMetricEditDistance ed = new SimilarityMetricEditDistance();

    @Test
    public void test() throws Exception {
        // For this case, the edit-distance of two strings is 3.
        UTF8StringPointable leftStrPointable1 = generateUTF8Pointable("coupon not available in store");
        UTF8StringPointable rightStrPointable1 = generateUTF8Pointable("coupon is available in store");

        // The edit-distance between leftStrPointable1 and the following is 14.
        UTF8StringPointable rightStrPointable2 = generateUTF8Pointable("coupon in store");

        byte[] leftBytes1 = leftStrPointable1.getByteArray();
        int leftStartOffset1 = leftStrPointable1.getStartOffset();
        byte[] rightBytes1 = rightStrPointable1.getByteArray();
        int rightStartOffset1 = rightStrPointable1.getStartOffset();
        byte[] rightBytes2 = rightStrPointable2.getByteArray();
        int rightStartOffset2 = rightStrPointable2.getStartOffset();

        // Case 1 - normal - no early termination
        int edThresh = 3;
        int edVal = ed.UTF8StringEditDistance(leftBytes1, leftStartOffset1, rightBytes1, rightStartOffset1, edThresh);
        assertEquals(edThresh, edVal);

        // Case 2 - the length difference between two strings is greater than edThresh.
        // Even without calculating the distance, the method should return -1.
        edVal = ed.UTF8StringEditDistance(leftBytes1, leftStartOffset1, rightBytes2, rightStartOffset2, edThresh);
        assertEquals(SimilarityMetricEditDistance.SIMILARITY_THRESHOLD_NOT_SATISFIED_VALUE, edVal);

        // Case 3 - the edit distance is 14, but the threshold is 1.
        // The early termination should happen and the returned value should be -1.
        edThresh = 1;
        edVal = ed.UTF8StringEditDistance(leftBytes1, leftStartOffset1, rightBytes2, rightStartOffset2, edThresh);
        assertEquals(SimilarityMetricEditDistance.SIMILARITY_THRESHOLD_NOT_SATISFIED_VALUE, edVal);

        // Case 4 - the edit distance is 14, but the threshold is 13.
        // The early termination will not happen. But, the resulting edit distance is greater than the given threshold.
        // So, the final returned value should be -1.
        edThresh = 13;
        edVal = ed.UTF8StringEditDistance(leftBytes1, leftStartOffset1, rightBytes2, rightStartOffset2, edThresh);
        assertEquals(SimilarityMetricEditDistance.SIMILARITY_THRESHOLD_NOT_SATISFIED_VALUE, edVal);
    }

}
