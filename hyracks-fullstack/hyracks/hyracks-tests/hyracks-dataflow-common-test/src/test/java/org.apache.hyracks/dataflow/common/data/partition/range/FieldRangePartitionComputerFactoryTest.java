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

public class FieldRangePartitionComputerFactoryTest extends AbstractFieldRangeMultiPartitionComputerFactoryTest {

    @Test
    public void testFRMPCF_Project_ASC_D3_N4_EDGE() throws HyracksDataException {
        int[][] results = new int[15][];
        results[0] = new int[] { 0 }; // -25:-22
        results[1] = new int[] { 0 }; //  50:53
        results[2] = new int[] { 0 }; //  99:102
        results[3] = new int[] { 1 }; // 100:103
        results[4] = new int[] { 1 }; // 101:104
        results[5] = new int[] { 1 }; // 150:153
        results[6] = new int[] { 1 }; // 199:202
        results[7] = new int[] { 2 }; // 200:203
        results[8] = new int[] { 2 }; // 201:204
        results[9] = new int[] { 2 }; // 250:253
        results[10] = new int[] { 2 }; // 299:302
        results[11] = new int[] { 3 }; // 300:303
        results[12] = new int[] { 3 }; // 301:304
        results[13] = new int[] { 3 }; // 350:353
        results[14] = new int[] { 3 }; // 425:428

        RangeMap rangeMap = getIntegerRangeMap(MAP_POINTS);
        int[] rangeFields = new int[] { 0 };

        executeFieldRangePartitionTests(PARTITION_EDGE_CASES, rangeMap, BINARY_ASC_COMPARATOR_FACTORIES, 4, results, 3,
                START_FIELD);
    }

    @Test
    public void testFRMPCF_Project_DESC_D3_N4_EDGE() throws HyracksDataException {
        int[][] results = new int[15][];
        results[0] = new int[] { 3 }; // -25:-22
        results[1] = new int[] { 3 }; //  50:53
        results[2] = new int[] { 2 }; //  99:102
        results[3] = new int[] { 2 }; // 100:103
        results[4] = new int[] { 2 }; // 101:104
        results[5] = new int[] { 2 }; // 150:153
        results[6] = new int[] { 1 }; // 199:202
        results[7] = new int[] { 1 }; // 200:203
        results[8] = new int[] { 1 }; // 201:204
        results[9] = new int[] { 1 }; // 250:253
        results[10] = new int[] { 0 }; // 299:302
        results[11] = new int[] { 0 }; // 300:303
        results[12] = new int[] { 0 }; // 301:304
        results[13] = new int[] { 0 }; // 350:353
        results[14] = new int[] { 0 }; // 425:428

        Long[] map = MAP_POINTS.clone();
        ArrayUtils.reverse(map);
        RangeMap rangeMap = getIntegerRangeMap(map);

        executeFieldRangePartitionTests(PARTITION_EDGE_CASES, rangeMap, BINARY_DESC_COMPARATOR_FACTORIES, 4, results, 3,
                END_FIELD);
    }
}
