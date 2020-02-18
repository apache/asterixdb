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

package org.apache.hyracks.dataflow.common.data.partition.range;

import org.apache.commons.lang3.ArrayUtils;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.junit.Test;

public class FieldRangeIntersectPartitionComputerFactoryTest
        extends AbstractFieldRangeMultiPartitionComputerFactoryTest {

    @Test
    public void testFRMPCF_Split_ASC_D3_N4_EDGE() throws HyracksDataException {
        int[][] results = new int[15][];
        results[0] = new int[] { 0 }; // -25:-22
        results[1] = new int[] { 0 }; //  50:53
        results[2] = new int[] { 0, 1 }; //  99:102
        results[3] = new int[] { 1 }; // 100:103
        results[4] = new int[] { 1 }; // 101:104
        results[5] = new int[] { 1 }; // 150:153
        results[6] = new int[] { 1, 2 }; // 199:202
        results[7] = new int[] { 2 }; // 200:203
        results[8] = new int[] { 2 }; // 201:204
        results[9] = new int[] { 2 }; // 250:253
        results[10] = new int[] { 2, 3 }; // 299:302
        results[11] = new int[] { 3 }; // 300:303
        results[12] = new int[] { 3 }; // 301:304
        results[13] = new int[] { 3 }; // 350:353
        results[14] = new int[] { 3 }; // 425:428

        RangeMap rangeMap = getIntegerRangeMap(MAP_POINTS);

        executeFieldRangeIntersectPartitionTests(PARTITION_EDGE_CASES, rangeMap, BINARY_ASC_COMPARATOR_FACTORIES, 4,
                results, 3, START_FIELD, END_FIELD);
    }

    @Test
    public void testFRMPCF_Split_DESC_D3_N4_EDGE() throws HyracksDataException {
        int[][] results = new int[15][];
        results[0] = new int[] { 3 }; // -25:-22
        results[1] = new int[] { 3 }; //  50:53
        results[2] = new int[] { 2, 3 }; //  99:102
        results[3] = new int[] { 2 }; // 100:103
        results[4] = new int[] { 2 }; // 101:104
        results[5] = new int[] { 2 }; // 150:153
        results[6] = new int[] { 1, 2 }; // 199:202
        results[7] = new int[] { 1 }; // 200:203
        results[8] = new int[] { 1 }; // 201:204
        results[9] = new int[] { 1 }; // 250:253
        results[10] = new int[] { 0, 1 }; // 299:302
        results[11] = new int[] { 0 }; // 300:303
        results[12] = new int[] { 0 }; // 301:304
        results[13] = new int[] { 0 }; // 350:353
        results[14] = new int[] { 0 }; // 425:428

        Long[] map = MAP_POINTS.clone();
        ArrayUtils.reverse(map);
        RangeMap rangeMap = getIntegerRangeMap(map);

        executeFieldRangeIntersectPartitionTests(PARTITION_EDGE_CASES, rangeMap, BINARY_DESC_COMPARATOR_FACTORIES, 4,
                results, 3, END_FIELD, START_FIELD);
    }

    @Test
    public void testFRMPCF_Split_ASC_D50_N16_EDGE() throws HyracksDataException {
        int[][] results = new int[15][];
        results[0] = new int[] { 0 }; // -25:25
        results[1] = new int[] { 2, 3 }; // 50:100
        results[2] = new int[] { 3, 4, 5 }; // 99:149
        results[3] = new int[] { 4, 5 }; // 100:150
        results[4] = new int[] { 4, 5, 6 }; // 101:151
        results[5] = new int[] { 6, 7 }; // 150:200
        results[6] = new int[] { 7, 8, 9 }; // 199:249
        results[7] = new int[] { 8, 9 }; // 200:250
        results[8] = new int[] { 8, 9, 10 }; // 201:251
        results[9] = new int[] { 10, 11 }; // 250:300
        results[10] = new int[] { 11, 12, 13 }; // 299:349
        results[11] = new int[] { 12, 13 }; // 300:350
        results[12] = new int[] { 12, 13, 14 }; // 301:351
        results[13] = new int[] { 14, 15 }; // 350:400
        results[14] = new int[] { 15 }; // 425:475

        RangeMap rangeMap = getIntegerRangeMap(MAP_POINTS);

        executeFieldRangeIntersectPartitionTests(PARTITION_EDGE_CASES, rangeMap, BINARY_ASC_COMPARATOR_FACTORIES, 16,
                results, 50, START_FIELD, END_FIELD);
    }

    @Test
    public void testFRMPCF_Split_DESC_D50_N16_EDGE() throws HyracksDataException {
        int[][] results = new int[15][];
        results[0] = new int[] { 15 }; // -25:25
        results[1] = new int[] { 12, 13 }; // 50:100
        results[2] = new int[] { 10, 11, 12 }; // 99:149
        results[3] = new int[] { 10, 11 }; // 100:150
        results[4] = new int[] { 9, 10, 11 }; // 101:151
        results[5] = new int[] { 8, 9 }; // 150:200
        results[6] = new int[] { 6, 7, 8 }; // 199:249
        results[7] = new int[] { 6, 7 }; // 200:250
        results[8] = new int[] { 5, 6, 7 }; // 201:251
        results[9] = new int[] { 4, 5 }; // 250:300
        results[10] = new int[] { 2, 3, 4 }; // 299:349
        results[11] = new int[] { 2, 3 }; // 300:350
        results[12] = new int[] { 1, 2, 3 }; // 301:351
        results[13] = new int[] { 0, 1 }; // 350:400
        results[14] = new int[] { 0 }; // 425:475

        Long[] map = MAP_POINTS.clone();
        ArrayUtils.reverse(map);
        RangeMap rangeMap = getIntegerRangeMap(map);

        executeFieldRangeIntersectPartitionTests(PARTITION_EDGE_CASES, rangeMap, BINARY_DESC_COMPARATOR_FACTORIES, 16,
                results, 50, END_FIELD, START_FIELD);
    }

    @Test
    public void testFRMPCF_Split_ASC_D3_N16_EACH() throws HyracksDataException {
        int[][] results = new int[16][];
        results[0] = new int[] { 0 }; // 20:23
        results[1] = new int[] { 1 }; // 45:48
        results[2] = new int[] { 2 }; // 70:73
        results[3] = new int[] { 3 }; // 95:98
        results[4] = new int[] { 4 }; // 120:123
        results[5] = new int[] { 5 }; // 145:148
        results[6] = new int[] { 6 }; // 170:173
        results[7] = new int[] { 7 }; // 195:198
        results[8] = new int[] { 8 }; // 220:223
        results[9] = new int[] { 9 }; // 245:248
        results[10] = new int[] { 10 }; // 270:273
        results[11] = new int[] { 11 }; // 295:298
        results[12] = new int[] { 12 }; // 320:323
        results[13] = new int[] { 13 }; // 345:348
        results[14] = new int[] { 14 }; // 370:373
        results[15] = new int[] { 15 }; // 395:398

        RangeMap rangeMap = getIntegerRangeMap(MAP_POINTS);

        executeFieldRangeIntersectPartitionTests(EACH_PARTITION, rangeMap, BINARY_ASC_COMPARATOR_FACTORIES, 16, results,
                3, START_FIELD, END_FIELD);
    }

    @Test
    public void testFRMPCF_Split_DESC_D3_N16_EACH() throws HyracksDataException {
        int[][] results = new int[16][];
        results[0] = new int[] { 15 }; // 20:23
        results[1] = new int[] { 14 }; // 45:48
        results[2] = new int[] { 13 }; // 70:73
        results[3] = new int[] { 12 }; // 95:98
        results[4] = new int[] { 11 }; // 120:123
        results[5] = new int[] { 10 }; // 145:148
        results[6] = new int[] { 9 }; // 170:173
        results[7] = new int[] { 8 }; // 195:198
        results[8] = new int[] { 7 }; // 220:223
        results[9] = new int[] { 6 }; // 245:248
        results[10] = new int[] { 5 }; // 270:273
        results[11] = new int[] { 4 }; // 295:298
        results[12] = new int[] { 3 }; // 320:323
        results[13] = new int[] { 2 }; // 345:348
        results[14] = new int[] { 1 }; // 370:373
        results[15] = new int[] { 0 }; // 395:398

        Long[] map = MAP_POINTS.clone();
        ArrayUtils.reverse(map);
        RangeMap rangeMap = getIntegerRangeMap(map);

        executeFieldRangeIntersectPartitionTests(EACH_PARTITION, rangeMap, BINARY_DESC_COMPARATOR_FACTORIES, 16,
                results, 3, END_FIELD, START_FIELD);
    }
}